"""Literal independent structure oracle; no AiiDA profile or scientific execution."""
from copy import deepcopy
from types import SimpleNamespace as NS

import pytest

from aiida_qcl_negf import service
from aiida_qcl_negf.run_structure import project_run_structure


@pytest.fixture
def plan():
    return {
        "schema": "qcl-negf-scientific-plan-v2", "root_kind": "meta", "root_definition_id": "root",
        "fingerprint": "f" * 64,
        "inclusions": [
            {"id": "i0", "parent_id": None, "definition_id": "root", "label": "Root", "path": ["root"], "execution_ids": ["e1", "e2"]},
            {"id": "i1", "parent_id": "i0", "definition_id": "d1", "label": "First", "path": ["root", "d1"], "execution_ids": ["e1"]},
            {"id": "i2", "parent_id": "i0", "definition_id": "d1", "label": "Shared", "path": ["root", "d1"], "execution_ids": ["e1"]}],
        "executions": [
            {"id": "e1", "definition_id": "d1", "variant_id": "v1", "label": "First", "operation": "transport", "point_ids": ["p1", "p2"], "policies": {"voltage": {"mode": "continuation"}}, "resolved_configuration": {"secret": "excluded"}},
            {"id": "e2", "definition_id": "d2", "variant_id": "v2", "label": "Second", "operation": "transport", "point_ids": ["p3"], "policies": {"voltage": {"mode": "independent"}}}],
        "points": [
            {"id": "p1", "execution_id": "e1", "branch": "T1", "order": 1, "initialization": "cold", "predecessor_id": None, "temperature_K": 70.0, "voltage_per_period_V": 0.0},
            {"id": "p2", "execution_id": "e1", "branch": "T1", "order": 2, "initialization": "previous", "predecessor_id": "p1", "temperature_K": 70.0, "voltage_per_period_V": 0.05},
            {"id": "p3", "execution_id": "e2", "branch": "T1", "order": 3, "initialization": "cold", "predecessor_id": None, "temperature_K": 70.0, "voltage_per_period_V": 0.1}]}


def test_literal_allowlist_and_shared_membership(plan):
    original = deepcopy(plan)
    actual = project_run_structure(plan)
    assert actual == {
        "status": "available", "root": {"kind": "meta", "definition_id": "root"},
        "inclusions": [
            {"id": "i0", "parent_id": None, "definition_id": "root", "label": "Root", "path": ["root"], "execution_ids": ["e1", "e2"]},
            {"id": "i1", "parent_id": "i0", "definition_id": "d1", "label": "First", "path": ["root", "d1"], "execution_ids": ["e1"]},
            {"id": "i2", "parent_id": "i0", "definition_id": "d1", "label": "Shared", "path": ["root", "d1"], "execution_ids": ["e1"]}],
        "executions": [
            {"id": "e1", "definition_id": "d1", "variant_id": "v1", "label": "First", "operation": "transport", "point_ids": ["p1", "p2"], "voltage_mode": "continuation"},
            {"id": "e2", "definition_id": "d2", "variant_id": "v2", "label": "Second", "operation": "transport", "point_ids": ["p3"], "voltage_mode": "independent"}],
        "points": [
            {"id": "p1", "execution_id": "e1", "branch": "T1", "order": 1, "initialization": "cold", "predecessor_id": None, "temperature_K": 70.0, "voltage_per_period_V": 0.0},
            {"id": "p2", "execution_id": "e1", "branch": "T1", "order": 2, "initialization": "previous", "predecessor_id": "p1", "temperature_K": 70.0, "voltage_per_period_V": 0.05},
            {"id": "p3", "execution_id": "e2", "branch": "T1", "order": 3, "initialization": "cold", "predecessor_id": None, "temperature_K": 70.0, "voltage_per_period_V": 0.1}]}
    assert plan == original
    actual["inclusions"][0]["execution_ids"].append("detached")
    assert plan == original


@pytest.mark.parametrize("fault", ["missing", "duplicate", "dangling", "cycle", "foreign", "later", "temperature", "branch", "membership", "inclusion_membership", "duplicate_ref", "missing_mode", "bool", "nan", "infinity", "wrong_type"])
def test_invalid_structure_unavailable(plan, fault):
    if fault == "missing": del plan["points"][0]["initialization"]
    elif fault == "duplicate": plan["points"][1]["id"] = "p1"
    elif fault == "dangling": plan["inclusions"][0]["parent_id"] = "absent"
    elif fault == "cycle": plan["inclusions"][0]["parent_id"] = "i1"
    elif fault == "foreign": plan["points"][2]["predecessor_id"] = "p2"
    elif fault == "later": plan["points"][0]["predecessor_id"] = "p2"
    elif fault == "temperature": plan["points"][1]["temperature_K"] = 71
    elif fault == "branch": plan["points"][1]["branch"] = "other"
    elif fault == "membership": plan["executions"][0]["point_ids"] = ["p1"]
    elif fault == "inclusion_membership": plan["inclusions"][0]["execution_ids"] = ["e1"]
    elif fault == "duplicate_ref": plan["executions"][0]["point_ids"] = ["p1", "p1", "p2"]
    elif fault == "missing_mode": del plan["executions"][0]["policies"]["voltage"]["mode"]
    elif fault == "bool": plan["points"][0]["order"] = True
    elif fault == "nan": plan["points"][0]["temperature_K"] = float("nan")
    elif fault == "infinity": plan["points"][0]["voltage_per_period_V"] = float("inf")
    elif fault == "wrong_type": plan["inclusions"][0]["path"] = "root"
    assert project_run_structure(plan) == {"status": "unavailable", "reason": "unsupported_structure"}


def test_unknown_schema_unavailable(plan):
    plan["schema"] = "other"
    assert project_run_structure(plan) == {"status": "unavailable", "reason": "unsupported_plan_schema"}


def test_get_run_one_plan_read_preserves_attempts_and_cap(plan, monkeypatch):
    node = NS(uuid="run", inputs=NS(plan=object()))
    children = [NS(uuid=f"attempt-{n}", inputs=NS(execution_id=NS(value="e1"), attempt=NS(value=n)), outputs={}) for n in (1, 2)]
    reads = []
    monkeypatch.setattr(service, "_node", lambda _: node)
    def read(value):
        assert value is node.inputs.plan
        reads.append(value)
        return plan
    monkeypatch.setattr(service, "read_plan", read)
    monkeypatch.setattr(service, "_children", lambda _: children)
    monkeypatch.setattr(service, "_summary", lambda n: {"uuid": n.uuid, "exit_status": 0 if n.uuid == "attempt-2" else 1})
    def forbidden(*a, **kw): pytest.fail("Unrelated I/O or mutation reached")
    for name in ("submit", "submit_plan", "read_json", "get_export_plan", "open_artifact", "get_model_help_input"):
        monkeypatch.setattr(service, name, forbidden)
    actual = service.get_run("run")
    assert len(reads) == 1
    assert actual["plan_structure"] == project_run_structure(plan)
    assert actual["plan_fingerprint"] == "f" * 64 and actual["results"] == {}
    assert [(r["uuid"], r["attempt"], r["exit_status"]) for r in actual["children"]] == [("attempt-1", 1, 1), ("attempt-2", 2, 0)]
    assert actual["plan_points"] == [{"id": "p1", "execution_id": "e1", "temperature_K": 70.0, "voltage_per_period_V": 0.0}, {"id": "p2", "execution_id": "e1", "temperature_K": 70.0, "voltage_per_period_V": 0.05}, {"id": "p3", "execution_id": "e2", "temperature_K": 70.0, "voltage_per_period_V": 0.1}]
    monkeypatch.setattr(service, "MAX_RESPONSE_BYTES", 50)
    monkeypatch.setattr(service, "_children", forbidden)
    with pytest.raises(ValueError, match="response limit"):
        service.get_run("run")
