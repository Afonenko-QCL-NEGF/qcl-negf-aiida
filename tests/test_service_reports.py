"""Tiny actual-service checks; no profile, daemon or scientific execution."""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace as NS

import aiida
import pytest
from qcl_negf_contracts import messages
from aiida_qcl_negf import service

RUN = "00000000-0000-0000-0000-000000000001"
CHILD = "00000000-0000-0000-0000-000000000002"
ERROR = "Report metadata exceeds the service response limit; use AiiDA directly"


def encoded(reports):
    # Independent transport oracle; byte expectations below are literal arithmetic.
    return json.dumps({"entries": reports}, ensure_ascii=False, allow_nan=False,
                      indent=None, separators=(",", ":")).encode("utf-8")


@pytest.fixture
def report_source(monkeypatch):
    assert sys.version_info[:2] == (3, 14)
    assert aiida.__version__ == "2.9.2"
    expected_service = Path(__file__).resolve().parents[1] / "src/aiida_qcl_negf/service.py"
    assert Path(service.__file__).resolve() == expected_service
    actual_contracts = Path(messages.__file__).resolve(strict=True)
    if "QCL_TEST_CONTRACTS_SOURCE" in os.environ:
        configured_source = os.environ["QCL_TEST_CONTRACTS_SOURCE"]
        assert configured_source
        expected_contracts = Path(configured_source)
        assert expected_contracts.is_absolute()
        canonical_contracts = expected_contracts.resolve(strict=True)
        assert expected_contracts == canonical_contracts
        assert actual_contracts == canonical_contracts
    assert hashlib.sha256(actual_contracts.read_bytes()).hexdigest() == (
        "fd82f76a005519962b1edbfd5b9971f6e8d50c0806ca1f4070359e908fa205fa"
    )
    forbidden_calls = []
    node_calls = []
    query_calls = []
    rows = []

    def forbidden(name):
        def fail(*args, **kwargs):
            forbidden_calls.append(name)
            raise AssertionError(f"Forbidden report side effect: {name}")
        return fail

    class FakeProcess:
        def __init__(self, pk, uuid, descendants):
            self.pk = pk
            self.uuid = uuid
            self.called_descendants = descendants

        def __getattr__(self, name):
            forbidden_calls.append(f"node.{name}")
            raise AssertionError(f"Unexpected node access: {name}")

    child = FakeProcess(12, CHILD, [])
    root = FakeProcess(11, RUN, [child])

    def node(identifier):
        node_calls.append(identifier)
        assert identifier == RUN
        return root

    class Collection:
        def find(self, filters=None, order_by=None, limit=None, offset=None):
            query_calls.append((filters, order_by, limit, offset))
            assert filters == {"dbnode_id": {"in": [11, 12]}}
            assert order_by == [{"time": "desc"}]
            assert limit == 1000
            assert offset is None
            return list(rows)

    monkeypatch.setattr(service, "_node", node)
    monkeypatch.setattr(service.orm, "ProcessNode", FakeProcess)
    monkeypatch.setattr(service.orm, "Log", NS(collection=Collection()))
    monkeypatch.setattr(service, "MAX_RESPONSE_BYTES", 512)
    for name in ("submit", "get_daemon_client", "kill_processes", "plan_data", "read_plan", "read_json"):
        monkeypatch.setattr(service, name, forbidden(name))

    @contextmanager
    def invocation():
        node_count, query_count = len(node_calls), len(query_calls)
        try:
            yield
        finally:
            assert node_calls[node_count:] == [RUN]
            assert query_calls[query_count:] == [
                ({"dbnode_id": {"in": [11, 12]}}, [{"time": "desc"}], 1000, None)
            ]
            assert forbidden_calls == []

    def row(message, pk=11, second=1):
        return NS(levelname="INFO", message=message,
                  time=datetime(2026, 10, 10, 0, 0, second, tzinfo=timezone.utc), dbnode_id=pk)

    return rows, row, invocation


def test_report_preserves_root_child_order_and_query(report_source):
    """Catch changed latest selection, chronological order, UUID or raw text."""
    rows, row, invocation = report_source
    rows[:] = [row("root", second=2), row("child", pk=12)]
    expected = [
        {"level": "INFO", "message": "child", "time": "2026-10-10T00:00:01+00:00",
         "process_uuid": "00000000-0000-0000-0000-000000000002"},
        {"level": "INFO", "message": "root", "time": "2026-10-10T00:00:02+00:00",
         "process_uuid": "00000000-0000-0000-0000-000000000001"},
    ]
    assert len(encoded(expected)) == 260
    with invocation():
        assert service.get_run_report(RUN) == expected


def test_report_rejects_unicode_bytes_over_bound(report_source):
    """Catch absent byte refusal or code-point counting instead of UTF-8."""
    rows, row, invocation = report_source
    rows[:] = [row("я" * 191)]
    expected = [{"level": "INFO", "message": "я" * 191,
                 "time": "2026-10-10T00:00:01+00:00",
                 "process_uuid": "00000000-0000-0000-0000-000000000001"}]
    assert len(encoded(expected)) == 514
    with invocation():
        with pytest.raises(ValueError) as error:
            service.get_run_report(RUN)
        assert str(error.value) == ERROR


def test_report_rejects_json_escape_overhead(report_source):
    """Catch raw-message sizing that omits JSON quote/control escapes."""
    rows, row, invocation = report_source
    rows[:] = [row('"\\\n\x00' * 32)]
    expected = [{"level": "INFO", "message": '"\\\n\x00' * 32,
                 "time": "2026-10-10T00:00:01+00:00",
                 "process_uuid": "00000000-0000-0000-0000-000000000001"}]
    assert len(encoded(expected)) == 516
    with invocation():
        with pytest.raises(ValueError) as error:
            service.get_run_report(RUN)
        assert str(error.value) == ERROR


def test_report_allows_exact_bound_and_rejects_plus_one(report_source):
    """Catch wrong equality comparison or omission of the entries envelope."""
    rows, row, invocation = report_source
    rows[:] = [row("a" * 380)]
    expected = [{"level": "INFO", "message": "a" * 380,
                 "time": "2026-10-10T00:00:01+00:00",
                 "process_uuid": "00000000-0000-0000-0000-000000000001"}]
    assert len(encoded(expected)) == 512
    with invocation():
        assert service.get_run_report(RUN) == expected
    rows[:] = [row("a" * 381)]
    plus_one = [{"level": "INFO", "message": "a" * 381,
                 "time": "2026-10-10T00:00:01+00:00",
                 "process_uuid": "00000000-0000-0000-0000-000000000001"}]
    assert len(encoded(plus_one)) == 513
    with invocation():
        with pytest.raises(ValueError) as error:
            service.get_run_report(RUN)
        assert str(error.value) == ERROR
