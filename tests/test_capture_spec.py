"""Operational capture spec preserves frozen bytes/unknown actual CPU."""
import copy
import hashlib
import json

from aiida_qcl_negf.capture_spec import CAPTURE_RESERVE_BYTES, capture_spec


def test_capture_spec_uses_actual_calcjob_identity_and_reserves_no_new_grant():
    plan = {"fingerprint": "a" * 64, "executions": [{"id": "E", "outputs": {"recovery": {
        "byte_budget": 512 * 1024**2, "reserve_bytes": 64 * 1024**2}}}],
        "points": [{"id": "P", "execution_id": "E"}]}
    before = copy.deepcopy(plan)
    raw = json.dumps(plan, indent=3).encode()
    value = json.loads(capture_spec(raw, plan, calcjob_uuid="job", execution_id="E", attempt=2))
    assert plan == before and hashlib.sha256(raw).hexdigest() == value["plan"]["sha256"]
    assert value["allocated_logical_cpus"] is None
    assert value["allocation_id"] == "calcjob-process:job:2"
    assert len(value["capture_token"]) == 32
    assert value["credits"]["performance_bytes"] + value["credits"]["scalar_bytes"] + 8 * 1024**2 == CAPTURE_RESERVE_BYTES
    plan["executions"][0]["outputs"]["recovery"]["byte_budget"] = 64 * 1024**2 + CAPTURE_RESERVE_BYTES - 1
    assert capture_spec(raw, plan, calcjob_uuid="job", execution_id="E", attempt=2) is None
    assert capture_spec(b"x" * (1024**2 + 1), before, calcjob_uuid="job", execution_id="E", attempt=2) is None
