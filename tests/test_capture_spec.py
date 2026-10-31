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


def test_native3_exact9_actual_run_uuid_and_safe_attempt():
    from aiida_qcl_negf.capture_spec import native_capture_spec
    from test_native_binding import SEED
    plan=copy.deepcopy(SEED['plan']);raw=json.dumps(plan,indent=2).encode()
    job='00000000-0000-4000-8000-000000000001';run='00000000-0000-4000-8000-000000000002'
    value=json.loads(native_capture_spec(raw,plan,calcjob_uuid=job,run_uuid=run,execution_id='execution',attempt=7,capture_token='a'*32))
    assert set(value)=={'schema','selection','plan','capture_token','allocation_id','allocated_logical_cpus','scratch_root','credits','native_performance'}
    assert value['schema']=='qcl-negf.capture-spec.v3' and value['native_performance']=={'run_uuid':run}
    assert value['selection']['calcjob_uuid']==job and value['plan']['sha256']==hashlib.sha256(raw).hexdigest()
    assert value['scratch_root'] is None and value['capture_token']=='a'*32
    for attempt in (True,1.0,0,9007199254740992):
        assert native_capture_spec(raw,plan,calcjob_uuid=job,run_uuid=run,execution_id='execution',attempt=attempt) is None
