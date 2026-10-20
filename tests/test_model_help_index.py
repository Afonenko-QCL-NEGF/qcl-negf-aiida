"""Synthetic C1 bytes over the actual frozen-plan shape; no origin/native proof."""
import hashlib
import json
from copy import deepcopy
from io import BytesIO
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from types import SimpleNamespace as NS

import pytest
from aiida_qcl_negf import model_help as help, parser

PATH = "result/executions/execution-1/point-1/attempt-1/model-help-inputs.json"


def frozen():
    return json.loads((Path(__file__).parent / "fixtures/plan.json").read_bytes())


def snapshot(plan, point="point-1", attempt=1):
    return {"format": "qcl-negf.model-help-inputs.v1", "phase": "solver_input",
            "context": help._help_context(plan, "execution-1", point, attempt),
            "inputs": [{"path": "solver.fixture_scalar", "declared": {"status": "unknown", "atom": None},
                "used": {"status": "known", "atom": {"kind": "integer", "value": 11 if point == "point-1" else 22, "unit": None}},
                "assigning": {"status": "unknown", "kind": "unknown", "source": None, "rule": None},
                "effective_origin": {"status": "unknown", "steps": []}}], "origin_bindings": []}


def raw_snapshot(plan, point="point-1", attempt=1):
    return json.dumps(snapshot(plan, point, attempt), indent=2).encode() + b"\n"


def build(plan, payload, sizes=None, complete=True):
    reads = []
    def read(path, maximum):
        reads.append((path, maximum + 1))
        raw = payload[path]
        if isinstance(raw, Exception):
            raise raw
        return raw[:maximum + 1]
    inventory = {"complete": complete, "files": [
        {"path": path, "size": (sizes or {}).get(path, len(raw) if isinstance(raw, bytes) else 1)}
        for path, raw in payload.items()]}
    index = help._build_help_index(inventory=inventory, plan=plan,
        execution_id="execution-1", attempt=1, read=read)
    return index, reads


def test_two_points_sorted_hashes_and_missing_malformed():
    plan = frozen()
    second = deepcopy(plan["points"][0])
    second["id"] = "point-2"
    plan["points"].append(second)
    plan["executions"][0]["point_ids"].append("point-2")
    other = PATH.replace("point-1", "point-2")
    payload = {other: raw_snapshot(plan, "point-2"), PATH: raw_snapshot(plan)}
    index, reads = build(plan, payload)
    assert index["complete"] and [row["path"] for row in index["entries"]] == sorted(payload)
    for row in index["entries"]:
        assert row["sha256"] == hashlib.sha256(payload[row["path"]]).hexdigest()
        assert row["status"] == "valid"
    assert len(reads) == 2
    index, _ = build(plan, {other: payload[other], PATH: b"{"})
    assert [(row["point_id"], row["reason"]) for row in index["entries"]] == [
        ("point-1", "malformed"), ("point-2", "ok")]
    assert len(build(plan, {other: payload[other]})[0]["entries"]) == 1


@pytest.mark.parametrize("raw", [b"\xff", b'{"x":1,"x":2}', b'{"x":NaN}',
    b'{"x":Infinity}', b'{"x":' + b'[' * 70 + b'0' + b']' * 70 + b'}'])
def test_json_boundaries_hash_original(raw):
    index, _ = build(frozen(), {PATH: raw})
    assert index["entries"][0]["reason"] == "malformed"
    assert index["entries"][0]["sha256"] == hashlib.sha256(raw).hexdigest()


@pytest.mark.parametrize("field,value,reason", [
    ("format", "future", "unsupported_version"), ("uuid", "injected", "malformed"),
    ("plan_fingerprint", "c" * 64, "identity_mismatch"),
    ("model_revision", "foreign", "identity_mismatch"),
    ("configuration_hash", "c" * 64, "identity_mismatch"),
    ("attempt", 2, "identity_mismatch"), ("point_id", "point-2", "identity_mismatch")])
def test_version_and_identity(field, value, reason):
    plan = frozen()
    document = snapshot(plan)
    if field in ("format", "uuid"):
        document[field] = value
    else:
        document["context"][field] = value
    index, _ = build(plan, {PATH: json.dumps(document).encode()})
    assert index["entries"][0]["reason"] == reason


@pytest.mark.parametrize("kind", ["oversize", "aggregate", "count", "incomplete"])
def test_preflight_caps_read_zero(kind, monkeypatch):
    plan = frozen()
    payload = {PATH: raw_snapshot(plan)}
    sizes = None
    if kind == "oversize":
        sizes = {PATH: 65537}
    elif kind == "aggregate":
        monkeypatch.setattr(help, "HELP_TOTAL_BYTES", len(payload[PATH]) - 1)
    elif kind == "count":
        payload[PATH.replace("point-1", "point-2")] = b"{}"
    index, reads = build(plan, payload, sizes, complete=kind != "incomplete")
    assert index == {"format": "qcl-negf.model-help-index.v1", "complete": False, "entries": []}
    assert reads == []


def test_caps_exact_boundaries_and_index_output(monkeypatch):
    plan = frozen()
    raw = raw_snapshot(plan)
    raw += b" " * (65536 - len(raw))
    monkeypatch.setattr(help, "HELP_TOTAL_BYTES", 65536)
    index, reads = build(plan, {PATH: raw})
    assert index["complete"] and reads == [(PATH, 65537)]
    encoded = help.encode(index)
    monkeypatch.setattr(help, "HELP_INDEX_BYTES", len(encoded))
    assert build(plan, {PATH: raw})[0]["complete"]
    monkeypatch.setattr(help, "HELP_INDEX_BYTES", len(encoded) - 1)
    assert build(plan, {PATH: raw})[0]["entries"] == []


@pytest.mark.parametrize("raw,size", [(b"{}", 3), (b"{} ", 2), (FileNotFoundError(), 1)])
def test_io_race_never_trusts_partial_index(raw, size):
    index, _ = build(frozen(), {PATH: raw}, {PATH: size})
    assert index["complete"] is False and index["entries"] == []


def test_unsafe_inventory_and_foreign_diagnostic():
    plan = frozen()
    assert build(plan, {"../" + PATH: b"{}"})[0]["complete"] is False
    foreign = PATH.replace("execution-1", "execution-2")
    row = build(plan, {foreign: raw_snapshot(plan)})[0]["entries"][0]
    assert row["execution_id"] == "execution-2" and row["reason"] == "identity_mismatch"


@pytest.mark.parametrize("recovery_error", [False, True])
def test_actual_parser_emits_index_before_missing_result_or_recovery_exit(monkeypatch, recovery_error):
    plan = frozen()
    raw = raw_snapshot(plan)
    handles, reads, emitted = [], [], {}
    class Handle(BytesIO):
        def read(self, size=-1):
            reads.append(size)
            return super().read(size)
    class Repository:
        def walk(self):
            return [(PurePosixPath(PATH).parent, [], ["model-help-inputs.json"])]
        @contextmanager
        def open(self, path, mode):
            assert mode == "rb"
            if path != PATH:
                raise FileNotFoundError(path)
            handle = Handle(raw)
            handles.append(handle)
            try:
                yield handle
            finally:
                handle.close()
    def recovery(*args, **kwargs):
        if recovery_error:
            raise ValueError("retained invalid recovery")
        return None, []
    monkeypatch.setattr(parser, "read_plan", lambda _: plan)
    monkeypatch.setattr(parser, "select_recovery", recovery)
    monkeypatch.setattr(parser.orm, "Dict", lambda *, dict: dict)
    actor = NS(retrieved=NS(base=NS(repository=Repository())),
        node=NS(inputs=NS(plan=object(), execution_id=NS(value="execution-1"),
            attempt=NS(value=1), archive_byte_budget=NS(value=100))),
        out=lambda name, value: emitted.__setitem__(name, value),
        logger=NS(error=lambda *args: None),
        exit_codes=NS(ERROR_INVALID_RECOVERY="recovery", ERROR_MISSING_RESULT="missing"))
    assert parser.QCLExecutionParser.parse(actor) == ("recovery" if recovery_error else "missing")
    assert emitted["model_help_index"]["entries"][0]["sha256"] == hashlib.sha256(raw).hexdigest()
    assert reads == [len(raw) + 1] and all(handle.closed for handle in handles)
