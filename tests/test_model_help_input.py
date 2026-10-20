"""Actual service selection/reader with synthetic retained repository boundaries."""
import hashlib
import json
from contextlib import contextmanager
from io import BytesIO
from types import SimpleNamespace as NS

import pytest
from aiida_qcl_negf import service
from test_model_help_index import PATH, build, frozen, raw_snapshot

RUN = "e7f5313d-22fd-49fa-9be5-85fbe29e1974"
C1 = "10dd2a4c-031b-425c-90b8-2f7ea564d313"
C2 = "20dd2a4c-031b-425c-90b8-2f7ea564d313"
PLAN_PATH = "result/scientific_plan.json"


class Outputs(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


def metadata(document):
    return NS(get_dict=lambda: document)


@pytest.fixture
def actor(monkeypatch):
    plan = frozen()
    original = json.dumps(plan, indent=3).encode() + b"\n"
    events, handles, selections = [], [], []
    class File:
        def __init__(self, raw):
            self.raw = raw
        def open(self, mode):
            assert mode == "rb"
            handle = BytesIO(self.raw)
            handles.append(handle)
            return handle
    class Repository:
        def __init__(self, uuid, payload):
            self.uuid, self.payload = uuid, payload
        def get_object(self, path):
            assert path == PLAN_PATH
            if path not in self.payload:
                raise FileNotFoundError(path)
            return object()
        @contextmanager
        def open(self, path, mode):
            assert mode == "rb" and not path.endswith("h5")
            events.append((self.uuid, path))
            raw = self.payload[path]
            if isinstance(raw, Exception):
                raise raw
            handle = BytesIO(raw)
            handles.append(handle)
            try:
                yield handle
            finally:
                handle.close()
    def child(attempt, uuid):
        path = PATH.replace("attempt-1", f"attempt-{attempt}")
        raw = raw_snapshot(plan, attempt=attempt)
        payload = {path: raw, PLAN_PATH: json.dumps(plan, indent=1).encode() + b"\n\n"}
        repo = Repository(uuid, payload)
        index, _ = build(plan, {PATH: raw_snapshot(plan)})
        row = index["entries"][0]
        row.update(path=path, attempt=attempt, sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
        return NS(uuid=uuid, inputs=NS(execution_id=NS(value="execution-1"), attempt=NS(value=attempt)),
            outputs=Outputs(retrieved=NS(base=NS(repository=repo)),
                inventory=metadata({"complete": True, "files": [{"path": path, "size": len(raw)}]}),
                model_help_index=metadata(index)))
    children = [child(1, C1), child(2, C2)]
    publication = {"execution_id": "execution-1", "attempt": 2, "calcjob_uuid": C2}
    def published():
        selections.append(1)
        pinned = dict(publication)
        publication.update(attempt=1, calcjob_uuid=C1)
        return pinned
    node = NS(uuid=RUN, inputs=NS(plan=File(original)),
              outputs=Outputs(selections={"execution-1": NS(get_dict=published)}))
    monkeypatch.setattr(service, "_node", lambda _: node)
    monkeypatch.setattr(service, "_children", lambda _: children)
    return NS(plan=plan, node=node, children=children, events=events, handles=handles,
              selections=selections, File=File, original=original)


def request(**kwargs):
    return service.get_model_help_input(RUN, "execution-1", "point-1", **kwargs)


def test_select_once_exact_frozen_bytes_and_detached_closed_handles(actor):
    result = request()
    assert actor.selections == [1]
    assert result["context"]["calcjob_uuid"] == C2 and result["context"]["attempt"] == 2
    assert result["input_status"] == {"status": "available", "reason": "ok"}
    assert result["frozen_plan"]["plan"] != actor.original
    assert result["frozen_plan"]["plan"] == actor.children[1].outputs.retrieved.base.repository.payload[PLAN_PATH]
    assert result["execution_input"]["sha256"] == hashlib.sha256(result["execution_input"]["bytes"]).hexdigest()
    assert [uuid for uuid, path in actor.events] == [C2, C2]
    assert all(handle.closed for handle in actor.handles)
    assert "binding" not in result and "value_origin" not in result and result["release_id"] is None


@pytest.mark.parametrize("kind", ["absent", "malformed", "unreadable", "different", "oversize"])
def test_plan_absence_only_fallback(actor, kind, monkeypatch):
    repo = actor.children[1].outputs.retrieved.base.repository
    if kind == "absent":
        del repo.payload[PLAN_PATH]
        assert request()["frozen_plan"]["plan"] == actor.original
    else:
        if kind == "malformed":
            repo.payload[PLAN_PATH] = b"\xff"
        elif kind == "unreadable":
            repo.payload[PLAN_PATH] = OSError("retained read failure")
        elif kind == "different":
            changed = frozen()
            changed["fingerprint"] = "c" * 64
            repo.payload[PLAN_PATH] = json.dumps(changed).encode()
        else:
            monkeypatch.setattr(service, "MAX_PLAN_BYTES", len(actor.original))
            repo.payload[PLAN_PATH] = actor.original + b" "
        with pytest.raises(ValueError):
            request()
        assert all(path == PLAN_PATH for _, path in actor.events)
    assert all(handle.closed for handle in actor.handles)


@pytest.mark.parametrize("kwargs", [{"attempt": True}, {"attempt": 0}, {"calcjob_uuid": "short"},
    {"calcjob_uuid": C1, "attempt": 2}])
def test_invalid_selectors_zero_payload(actor, kwargs):
    with pytest.raises(ValueError):
        request(**kwargs)
    assert not actor.events


@pytest.mark.parametrize("kind,reason", [("missing_index", "inventory_unavailable"),
    ("missing_retrieved", "inventory_unavailable"), ("incomplete", "inventory_incomplete"),
    ("duplicate_generic", "inventory_incomplete"), ("malformed_index", "inventory_incomplete"),
    ("missing", "snapshot_missing"), ("unindexed", "inventory_incomplete")])
def test_inventory_and_missing_keep_plan_without_help_reads(actor, kind, reason):
    outputs = actor.children[1].outputs
    inventory = outputs.inventory.get_dict()
    index = outputs.model_help_index.get_dict()
    if kind == "missing_index":
        del outputs["model_help_index"]
    elif kind == "missing_retrieved":
        del outputs["retrieved"]
    elif kind == "incomplete":
        inventory["complete"] = False
    elif kind == "duplicate_generic":
        inventory["files"] *= 2
    elif kind == "malformed_index":
        index["unknown"] = True
    elif kind == "missing":
        inventory["files"] = []
        index["entries"] = []
    else:
        index["entries"] = []
    result = request()
    assert result["input_status"] == {"status": "unavailable", "reason": reason}
    assert result["context"] and result["frozen_plan"]["plan"]
    assert result["snapshot"] is None and result["execution_input"] is None
    assert all(path == PLAN_PATH for _, path in actor.events)
    assert all(handle.closed for handle in actor.handles)


@pytest.mark.parametrize("kind", ["invalid", "hash", "size", "duplicate", "tamper", "missing_raw", "context"])
def test_invalid_selected_snapshot_and_handle_closure(actor, kind):
    outputs = actor.children[1].outputs
    row = outputs.model_help_index.get_dict()["entries"][0]
    repo = outputs.retrieved.base.repository
    path = row["path"]
    if kind == "invalid":
        row.update(status="invalid", reason="malformed")
    elif kind == "hash":
        row["sha256"] = "a" * 64
    elif kind == "size":
        row["bytes"] += 1
    elif kind == "duplicate":
        outputs.model_help_index.get_dict()["entries"].append(dict(row))
    elif kind == "tamper":
        repo.payload[path] = repo.payload[path].replace(b"solver_input", b"solver_inpuX")
    elif kind == "missing_raw":
        del repo.payload[path]
    else:
        document = json.loads(repo.payload[path])
        document["context"]["model_revision"] = "foreign"
        raw = json.dumps(document).encode()
        repo.payload[path] = raw
        row.update(bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())
        outputs.inventory.get_dict()["files"][0]["size"] = len(raw)
    result = request()
    assert result["input_status"] == ({"status": "unavailable", "reason": "inventory_incomplete"}
        if kind == "duplicate" else {"status": "invalid", "reason": "snapshot_invalid"})
    assert all(handle.closed for handle in actor.handles)


def test_imported_point_not_claimed_executed_by_retry(actor):
    actor.children[1].outputs["result"] = actor.File(json.dumps({"selected_execution_id": "execution-1",
        "points": [{"id": "point-1", "execution_id": "execution-1", "attempt": 1}]}).encode())
    assert request()["input_status"]["reason"] == "point_not_executed_by_selected_child"
    assert all(path == PLAN_PATH for _, path in actor.events)
    assert request(attempt=1, calcjob_uuid=C1)["input_status"]["status"] == "available"
    assert all(handle.closed for handle in actor.handles)


def test_retained_release_conflict_never_infers_current_environment(actor, monkeypatch):
    monkeypatch.setenv("QCL_RELEASE_ID", "current-ignored")
    actor.node.inputs.release_id = NS(value="historical")
    assert request(attempt=2)["release_id"] == "historical"
    actor.children[1].inputs.release_id = NS(value="different")
    with pytest.raises(ValueError, match="Contradictory retained release"):
        request(attempt=2)
