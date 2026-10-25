"""Active graph/mapping port only: no NFS access or scientific process."""
from types import SimpleNamespace as NS
from uuid import UUID

import pytest

from aiida_qcl_negf import live_sources, saved_sources, service

RUN, CHILD, REMOTE, COMPUTER = [str(UUID(int=n)) for n in range(1, 5)]


@pytest.fixture
def active(monkeypatch):
    class Ports(dict):
        def __getattr__(self, key): return self[key]
    remote = NS(uuid=REMOTE, creator=NS(uuid=CHILD), computer=NS(uuid=COMPUTER),
                get_remote_path=lambda: "/worker/jobs/aa/bb")
    child = NS(uuid=CHILD, inputs=Ports(code=NS(computer=remote.computer)),
               outputs=Ports(remote_folder=remote), process_state=NS(value="running"))
    monkeypatch.setattr(service, "_export_child", lambda *a, **k: (child, 3, CHILD))
    monkeypatch.setattr(service, "_node", lambda _: NS())
    monkeypatch.setattr(saved_sources, "_plan", lambda *a: (None, b"", {
        "fingerprint": "a" * 64, "points": [{"id": "P", "execution_id": "E"}]}))
    monkeypatch.setattr(saved_sources, "_exact_child", lambda **k: pytest.fail("L1 quiescence must not be called"))
    monkeypatch.setattr(saved_sources, "_bindings", {"jobs": {
        "live_enabled": True, "remote_root": "/worker/jobs", "root": "/private/controller/jobs",
        "computer_uuid": COMPUTER, "storage_identity": "test-storage"}})
    return child


def request(**kwargs):
    return live_sources.get_live_history_source(RUN, "E", attempt=3, calcjob_uuid=CHILD,
        point_id=kwargs.get("point_id", "P"), source_attempt=7, namespace_id="jobs")


def test_active_collision_attempt_keeps_native_identity_without_claiming_descriptor(active):
    source = request()
    assert source["selection"]["attempt"] == 3 and source["selection"]["source_attempt"] == 7
    assert source["descriptor_path"] == "aa/bb/live/current.json"
    assert source["access_mode"] == "selected_closed_copy" and source["descriptor_verified"] is False
    assert "/private" not in str(source) and source["process_state"] == "running"


def test_foreign_point_mapping_or_b0_is_refused(active):
    with pytest.raises(ValueError):
        request(point_id="foreign")
    active.outputs.remote_folder.computer.uuid = str(UUID(int=10))
    with pytest.raises(ValueError):
        request()
    active.inputs["b0_reference"] = "bound"
    with pytest.raises(ValueError):
        request()
