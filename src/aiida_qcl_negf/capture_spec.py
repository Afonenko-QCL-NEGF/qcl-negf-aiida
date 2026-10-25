"""Small optional ordinary-job spec; Runner alone admits the storage credit."""
import hashlib
import json
import secrets

MIB = 1024 * 1024
CAPTURE_RESERVE_BYTES = 104 * MIB


def capture_spec(plan_raw, plan, *, calcjob_uuid, execution_id, attempt, scratch_root=None):
    """No requested CPU is represented as an actual scheduler allocation."""
    selected = [e for e in plan["executions"] if e["id"] == execution_id]
    points = [p for p in plan["points"] if p["execution_id"] == execution_id]
    if len(selected) != 1 or not points or len(points) > 256 or len(plan_raw) > MIB:
        return None
    policy = selected[0].get("outputs", {}).get("recovery")
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
             "capture_token": secrets.token_hex(16),
             "allocation_id": f"calcjob-process:{calcjob_uuid}:{attempt}",
             "allocated_logical_cpus": None, "scratch_root": scratch_root,
             "credits": {"performance_bytes": 32 * MIB, "scalar_bytes": 64 * MIB,
                         "io_bytes": min(128 * MIB, budget - reserve), "reserve_bytes": reserve}}
    raw = (json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()
    return raw if len(raw) <= 16384 else None
