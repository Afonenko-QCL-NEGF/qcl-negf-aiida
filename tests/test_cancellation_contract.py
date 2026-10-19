"""Actual cancellation service with synthetic ORM/daemon boundaries, no profile."""

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiida_qcl_negf import service

RUN = "00000000-0000-0000-0000-000000000023"
PRIVATE = "amqp://fake-user:fake-secret@private-broker.invalid:5672/private-vhost"


def process(*, state="running", terminated=False, exit_status=None, second=0):
    return SimpleNamespace(
        uuid=RUN, pk=23, label="synthetic cancellation", is_terminated=terminated,
        process_state=SimpleNamespace(value=state), exit_status=exit_status,
        is_finished_ok=terminated and state == "finished" and exit_status == 0,
        ctime=datetime(2026, 10, 19, tzinfo=timezone.utc),
        mtime=datetime(2026, 10, 19, 0, 0, second, tzinfo=timezone.utc),
    )


@pytest.fixture
def cancellation(monkeypatch):
    # Fail if a wheel or another checkout silently supplies the owning service.
    assert Path(service.__file__).resolve() == (
        Path(__file__).resolve().parents[1] / "src/aiida_qcl_negf/service.py"
    )
    boundary = SimpleNamespace(
        initial=process(), refreshed=process(second=1), daemon_running=True,
        fault=None, events=[], forbidden=[],
    )

    def node(identifier):
        assert identifier == RUN
        boundary.events.append("node")
        if boundary.events.count("node") == 1:
            return boundary.initial
        if boundary.fault == "refresh":
            raise OSError(PRIVATE)
        return boundary.refreshed

    def daemon_client():
        boundary.events.append("daemon")
        if boundary.fault == "daemon":
            raise OSError(PRIVATE)
        return SimpleNamespace(is_daemon_running=boundary.daemon_running)

    def kill(nodes, *, msg_text, timeout):
        boundary.events.append("kill")
        assert nodes == [boundary.initial]
        assert isinstance(msg_text, str) and msg_text
        assert 0 < timeout <= 10.0
        if boundary.fault == "dispatch":
            raise OSError(PRIVATE)

    def forbidden(name):
        def reject(*args, **kwargs):
            boundary.forbidden.append(name)
            raise AssertionError(f"Cancellation must not start work: {name}")
        return reject

    monkeypatch.setattr(service, "_node", node)
    monkeypatch.setattr(service, "get_daemon_client", daemon_client)
    monkeypatch.setattr(service, "kill_processes", kill)
    for name in ("submit", "submit_plan", "plan_data"):
        monkeypatch.setattr(service, name, forbidden(name))
    yield boundary
    # Checked even on an expected error: cancellation ambiguity never starts work.
    assert boundary.forbidden == []


def test_kill_run_daemon_down_has_no_success(cancellation):
    """Reject false success and prevent dispatch when the daemon is down."""
    cancellation.daemon_running = False
    with pytest.raises(RuntimeError, match="cancellation"):
        service.kill_run(RUN)
    assert cancellation.events == ["node", "daemon"]
    assert cancellation.initial.process_state.value == "running"
    assert not cancellation.initial.is_terminated


def test_kill_run_unconfirmed_state_remains_unknown(cancellation):
    """A dispatch acknowledgement alone cannot manufacture a killed summary."""
    with pytest.raises(RuntimeError, match="not confirmed"):
        service.kill_run(RUN)
    assert cancellation.events == ["node", "daemon", "kill", "node"]
    assert cancellation.refreshed.process_state.value == "running"
    assert not cancellation.refreshed.is_terminated


def test_kill_run_refreshes_before_confirmed_summary(cancellation):
    """Catch summaries from the stale running node or a fabricated killed state."""
    cancellation.refreshed = process(
        state="finished", terminated=True, exit_status=301, second=7,
    )
    assert service.kill_run(RUN) == {
        "uuid": RUN, "pk": 23, "label": "synthetic cancellation",
        "process_state": "finished", "exit_status": 301,
        "is_finished_ok": False, "ctime": "2026-10-19T00:00:00+00:00",
        "mtime": "2026-10-19T00:00:07+00:00",
    }
    assert cancellation.events == ["node", "daemon", "kill", "node"]
    assert cancellation.initial.process_state.value == "running"


@pytest.mark.parametrize("site", ["daemon", "dispatch", "refresh"])
def test_kill_run_exception_has_redacted_public_error(cancellation, site):
    """Backend failure stays unknown and must not expose a private broker URL."""
    cancellation.fault = site
    with pytest.raises(RuntimeError) as caught:
        service.kill_run(RUN)
    public_message = str(caught.value)
    assert "cancel" in public_message.lower()
    for private_fragment in (PRIVATE, "fake-secret", "private-broker", "private-vhost"):
        assert private_fragment not in public_message
    assert cancellation.events.count("kill") == (0 if site == "daemon" else 1)
    assert cancellation.initial.process_state.value == "running"


@pytest.mark.parametrize("state, status", [("killed", 400), ("finished", 301), ("finished", 0)])
def test_kill_run_already_terminal_needs_no_daemon(cancellation, state, status):
    """Terminal runs retain their truthful outcome and never send another kill."""
    cancellation.initial = process(state=state, terminated=True, exit_status=status)
    cancellation.daemon_running = False
    summary = service.kill_run(RUN)
    assert summary["process_state"] == state
    assert summary["exit_status"] == status
    assert summary["is_finished_ok"] is (state == "finished" and status == 0)
    assert summary["uuid"] == RUN
    assert cancellation.events == ["node"]
