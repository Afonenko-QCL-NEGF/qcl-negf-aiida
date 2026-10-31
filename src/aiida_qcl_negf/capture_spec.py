"""Small optional ordinary-job spec; Runner alone admits the storage credit."""
import hashlib
import json
import secrets
import re
from uuid import UUID

MIB = 1024 * 1024
CAPTURE_RESERVE_BYTES = 104 * MIB


def _capture_spec(plan_raw, plan, *, calcjob_uuid, execution_id, attempt, scratch_root=None,
                  run_uuid=None, capture_token=None):
    """No requested CPU is represented as an actual scheduler allocation."""
    selected = [e for e in plan["executions"] if e["id"] == execution_id]
    points = [p for p in plan["points"] if p["execution_id"] == execution_id]
    if len(selected) != 1 or not points or len(points) > 256 or len(plan_raw) > MIB:
        return None
    policy = selected[0].get("output" if run_uuid is not None else "outputs", {}).get("recovery")
    if not isinstance(policy, dict):
        return None # Legacy/missing operational policy is not a new grant.
    budget, reserve = policy.get("byte_budget"), policy.get("reserve_bytes")
    if (type(budget) is not int or type(reserve) is not int or reserve < 0
            or budget - reserve < CAPTURE_RESERVE_BYTES):
        return None
    value = {"schema": "qcl-negf.capture-spec.v1",
             "selection": {"calcjob_uuid": calcjob_uuid, "execution_id": execution_id,
                           "attempt": attempt, "plan_fingerprint": plan["fingerprint"]},
             "plan": {"path": "plan.json", "bytes": len(plan_raw), "sha256": hashlib.sha256(plan_raw).hexdigest()},
             "capture_token": secrets.token_hex(16) if capture_token is None else capture_token,
             "allocation_id": f"calcjob-process:{calcjob_uuid}:{attempt}",
             "allocated_logical_cpus": None, "scratch_root": scratch_root,
             "credits": {"performance_bytes": 32 * MIB, "scalar_bytes": 64 * MIB,
                         "io_bytes": min(128 * MIB, budget - reserve), "reserve_bytes": reserve}}
    if run_uuid is not None:
        value["schema"] = "qcl-negf.capture-spec.v3"
        value["native_performance"] = {"run_uuid": run_uuid}
    raw = (json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()
    return raw if len(raw) <= 16384 else None


def capture_spec(plan_raw, plan, *, calcjob_uuid, execution_id, attempt, scratch_root=None):
    """Ordinary spec1 remains unchanged."""
    return _capture_spec(plan_raw,plan,calcjob_uuid=calcjob_uuid,execution_id=execution_id,
                         attempt=attempt,scratch_root=scratch_root)


def native_capture_eligible(plan_raw,plan,*,execution_id,attempt,scratch_root=None):
    """Operational request only; genuine graph/CREATE must precede publication."""
    if scratch_root is not None or type(attempt) is not int or not 1 <= attempt <= 9007199254740991:
        return False
    if (type(plan_raw) is not bytes or not plan_raw or type(execution_id) is not str
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}",execution_id) is None
            or re.fullmatch(r"[0-9a-f]{64}",plan.get("fingerprint", "")) is None):return False
    selected=[e for e in plan["executions"] if e["id"]==execution_id]
    points=[p for p in plan["points"] if p["execution_id"]==execution_id]
    if len(selected)!=1 or not points or len(points)>256 or len(plan_raw)>MIB:return False
    policy=selected[0].get("output",{}).get("recovery")
    if type(policy) is not dict:return False
    budget,reserve=policy.get("byte_budget"),policy.get("reserve_bytes")
    return (type(budget) is int and type(reserve) is int and reserve>=0 and
            budget-reserve>=CAPTURE_RESERVE_BYTES)


def native_capture_spec(plan_raw,plan,*,calcjob_uuid,run_uuid,execution_id,attempt,capture_token=None):
    """Closed native3 exact9; run UUID is supplied by the actual CALL-chain owner."""
    if not native_capture_eligible(plan_raw,plan,execution_id=execution_id,attempt=attempt):return None
    for value in (calcjob_uuid,run_uuid):
        if type(value) is not str or str(UUID(value))!=value or UUID(value).int==0:
            raise ValueError("native capture UUID")
    capture_token=secrets.token_hex(16) if capture_token is None else capture_token
    if type(capture_token) is not str or re.fullmatch(r"[0-9a-f]{32}",capture_token) is None:
        raise ValueError("native capture token")
    return _capture_spec(plan_raw,plan,calcjob_uuid=calcjob_uuid,execution_id=execution_id,
                         attempt=attempt,run_uuid=run_uuid,capture_token=capture_token)
