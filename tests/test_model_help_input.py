"""Bounded detached context over the ordinary producer plan, without a profile."""
import hashlib
import json
from contextlib import contextmanager
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace as NS

import pytest
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.model_help import _validate_model_help_bindings

from aiida_qcl_negf import service

RUN = "e7f5313d-22fd-49fa-9be5-85fbe29e1974"
C1 = "10dd2a4c-031b-425c-90b8-2f7ea564d313"
C2 = "20dd2a4c-031b-425c-90b8-2f7ea564d313"


class Outputs(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as error:
            raise AttributeError(name) from error


@pytest.fixture
def source(monkeypatch):
    # Exact existing ordinary nested producer shape: no flat point.configuration.
    raw = (Path(__file__).parent / "fixtures/plan.json").read_bytes()
    handles, events = [], []

    class Input:
        def open(self, mode):
            assert mode == "rb"
            stream = BytesIO(raw)
            handles.append(stream)
            return stream

    class Repository:
        payload = raw
        absent = False
        unreadable = False

        def get_object(self, path):
            assert path == "result/scientific_plan.json"
            events.append("probe")
            if self.absent:
                raise FileNotFoundError(path)
            return object()

        @contextmanager
        def open(self, path, mode):
            assert (path, mode) == ("result/scientific_plan.json", "rb")
            events.append("open")
            if self.unreadable:
                raise OSError("repository unavailable")
            with BytesIO(self.payload) as stream:
                handles.append(stream)
                yield stream

    repository = Repository()
    children = [NS(uuid=uuid, inputs=NS(execution_id=NS(value="execution-1"),
                    attempt=NS(value=attempt)), outputs=Outputs())
                for attempt, uuid in ((1, C1), (2, C2))]
    children[1].outputs["retrieved"] = NS(uuid=C2, base=NS(repository=repository))
    selection = {"execution_id": "execution-1", "attempt": 2, "calcjob_uuid": C2}
    node = NS(uuid=RUN, inputs=NS(plan=Input()), outputs=Outputs(
        selections={"one": NS(get_dict=lambda: dict(selection))}))
    monkeypatch.setattr(service, "_node", lambda _: node)
    monkeypatch.setattr(service, "_children", lambda _: children)
    return NS(raw=raw, plan=json.loads(raw), node=node, children=children,
              repository=repository, selection=selection, handles=handles, events=events)


def read(**kwargs):
    return service.get_model_help_input(RUN, "execution-1", "point-1", **kwargs)


def test_actual_ordinary_context_and_c1_c2_unknown(source, monkeypatch):
    monkeypatch.setenv("QCL_NEGF_RELEASE_ID", "current-B")
    source.node.inputs.release_id = NS(value="recorded-A")
    dto = read()
    point = source.plan["points"][0]
    assert "configuration" not in point
    assert dto == {
        "context": {"run_uuid": RUN, "execution_id": "execution-1", "point_id": "point-1",
                    "attempt": 2, "calcjob_uuid": C2,
                    "plan_fingerprint": source.plan["fingerprint"],
                    "scientific_fingerprint": source.plan["scientific_fingerprint"],
                    "model_revision": source.plan["model_revision"],
                    "configuration_hash": point["effective_inputs"]["configuration_hash"]},
        "frozen_plan": {"bytes": source.raw, "sha256": hashlib.sha256(source.raw).hexdigest()},
        "snapshot": None,
        "input_status": {"status": "unavailable", "reason": "inventory_unavailable"},
        "release_id": "recorded-A"}
    view = {"format": "qcl-negf.model-help-view.v1", "context": dto["context"],
            "binding": {"status": "unknown", "reason": "source_binding_missing",
                        "release_id": dto["release_id"], "release_manifest_sha256": None,
                        "source_revision": None, "bundle_sha256": None},
            "fragment": None, "inputs": [], "input_status": dto["input_status"]}
    schema_validator("model-help.schema.json").validate(view)
    retained = {"selection": {"context": dto["context"], "release_id": dto["release_id"]},
                "metadata": {"frozen_plan": dto["frozen_plan"]}}
    _validate_model_help_bindings(view, retained=retained)
    assert all(handle.closed for handle in source.handles)
    assert dto["frozen_plan"]["bytes"] == source.raw


def test_running_child_original_only_does_not_weaken_export_guard(source):
    dto = read(attempt=1, calcjob_uuid=C1)
    assert dto["context"]["attempt"] == 1 and dto["frozen_plan"]["bytes"] == source.raw
    assert dto["release_id"] is None and source.events == []
    with pytest.raises(LookupError, match="Retrieved execution provenance"):
        service.get_export_plan(RUN, "execution-1", attempt=1, calcjob_uuid=C1)


def test_absent_retrieved_entry_falls_back_and_present_whitespace_is_exact(source):
    source.repository.absent = True
    assert read()["frozen_plan"]["bytes"] == source.raw
    source.repository.absent = False
    source.repository.payload = json.dumps(source.plan, indent=1).encode() + b"\n\n"
    assert read()["frozen_plan"]["bytes"] == source.repository.payload
    assert all(handle.closed for handle in source.handles)


@pytest.mark.parametrize("kind", ["corrupt", "different", "unreadable", "oversize"])
def test_present_bad_retrieved_never_falls_back_or_conflicts(source, monkeypatch, kind):
    if kind == "corrupt":
        source.repository.payload = b"\xff"
    elif kind == "different":
        altered = deepcopy(source.plan)
        altered["fingerprint"] = "c" * 64
        source.repository.payload = json.dumps(altered).encode()
    elif kind == "unreadable":
        source.repository.unreadable = True
    else:
        monkeypatch.setattr(service, "MAX_PLAN_BYTES", len(source.raw))
        source.repository.payload = source.raw + b" "
    with pytest.raises((ValueError, OSError)):
        read()
    assert all(handle.closed for handle in source.handles)


def test_selection_is_bound_once_before_frozen_reads(source, monkeypatch):
    original = service._export_child
    calls = []

    def select(*args, **kwargs):
        calls.append(kwargs)
        selected = original(*args, **kwargs)
        source.selection.update(attempt=1, calcjob_uuid=C1)
        return selected

    monkeypatch.setattr(service, "_export_child", select)
    dto = read()
    assert dto["context"]["calcjob_uuid"] == C2 and dto["context"]["attempt"] == 2
    assert len(calls) == 1 and source.events == ["probe", "open"]


def test_retrieved_presence_probe_failure_is_not_absence(source):
    def unavailable(path):
        raise OSError("repository unavailable")
    source.repository.get_object = unavailable
    with pytest.raises(OSError):
        read()
    assert all(handle.closed for handle in source.handles)


@pytest.mark.parametrize("identifier", [None, 3, "bad", RUN[:8]])
def test_invalid_run_does_not_reach_lookup(source, monkeypatch, identifier):
    monkeypatch.setattr(service, "_node", lambda *a: pytest.fail("lookup reached"))
    with pytest.raises(ValueError):
        service.get_model_help_input(identifier, "execution-1", "point-1")


def test_missing_run_remains_lookup_error(source, monkeypatch):
    def missing(identifier):
        raise LookupError("Run not found")
    monkeypatch.setattr(service, "_node", missing)
    with pytest.raises(LookupError, match="Run not found"):
        read()


@pytest.mark.parametrize("kwargs", [{"attempt": True}, {"attempt": 0}, {"calcjob_uuid": "bad"}])
def test_malformed_selectors_fail_before_selection(source, monkeypatch, kwargs):
    monkeypatch.setattr(service, "_export_child", lambda *a, **k: pytest.fail("selection reached"))
    with pytest.raises(ValueError):
        read(**kwargs)


@pytest.mark.parametrize("execution,point", [("", "point-1"), ("../x", "point-1"),
    ("execution-1", ""), ("execution-1", "x/y"), ("execution-1", "x" * 129)])
def test_malformed_context_ids(source, execution, point):
    with pytest.raises(ValueError):
        service.get_model_help_input(RUN, execution, point)
    assert source.events == [] and source.handles == []


def test_ambiguous_and_conflicting_selection_are_typed(source):
    source.node.outputs.clear()
    assert read() == {"error": "selection_conflict"}
    assert read(attempt=1, calcjob_uuid=C2) == {"error": "selection_conflict"}
    assert source.events == [] and source.handles == []


@pytest.mark.parametrize("execution,point,kwargs", [("missing", "point-1", {}),
    ("execution-1", "missing", {}), ("execution-1", "point-1", {"attempt": 3})])
def test_missing_context_is_not_selection_conflict(source, execution, point, kwargs):
    with pytest.raises(LookupError):
        service.get_model_help_input(RUN, execution, point, **kwargs)


@pytest.mark.parametrize("owner", ["node", "child", "both"])
def test_release_reads_only_immutable_inputs(source, owner):
    if owner in ("node", "both"):
        source.node.inputs.release_id = NS(value="retained-A")
    if owner in ("child", "both"):
        source.children[1].inputs.release_id = NS(value="retained-A")
    assert read()["release_id"] == "retained-A"


@pytest.mark.parametrize("value", ["other", "", "bad/release", 3, None])
def test_present_conflicting_or_invalid_release_is_rejected(source, value):
    source.node.inputs.release_id = NS(value="retained-A")
    source.children[1].inputs.release_id = NS(value=value)
    with pytest.raises(ValueError):
        read()


@pytest.mark.parametrize("field,value", [("temperature_K", 71.0),
    ("voltage_per_period_V", 0.1), ("axes_applied", False),
    ("configuration_hash", "x" * 64), ("hash_encoding", "other")])
def test_passport_contradiction_rejected_without_reconstruction(source, field, value):
    altered = deepcopy(source.plan)
    altered["points"][0]["effective_inputs"][field] = value
    raw = json.dumps(altered).encode()
    source.node.inputs.plan.open = lambda mode: BytesIO(raw)
    source.repository.absent = True
    with pytest.raises(ValueError):
        read()


def test_two_points_never_default_to_first_and_get_run_lists_bounded_metadata(source, monkeypatch):
    plan = deepcopy(source.plan)
    second = deepcopy(plan["points"][0])
    second.update(id="point-2", order=2, voltage_per_period_V=0.1)
    second["effective_inputs"].update(voltage_per_period_V=0.1, configuration_hash="d" * 64)
    plan["points"].append(second)
    plan["executions"][0]["point_ids"].append("point-2")
    plan["computation_count"] = 2
    raw = json.dumps(plan).encode()
    source.node.inputs.plan.open = lambda mode: BytesIO(raw)
    source.repository.absent = True
    dto = service.get_model_help_input(RUN, "execution-1", "point-2")
    assert dto["context"]["configuration_hash"] == "d" * 64
    monkeypatch.setattr(service, "_summary", lambda node: {"uuid": node.uuid})
    result = service.get_run(RUN)
    assert result["plan_points"] == [
        {key: point[key] for key in ("id", "execution_id", "temperature_K", "voltage_per_period_V")}
        for point in plan["points"]]
    monkeypatch.setattr(service, "MAX_RESPONSE_BYTES", 50)
    with pytest.raises(ValueError, match="response limit"):
        service.get_run(RUN)
