"""Small optional ordinary-job spec; Runner alone admits the storage credit."""
import hashlib
import json
import secrets
import re
import os
from copy import deepcopy
from uuid import UUID

MIB = 1024 * 1024
CAPTURE_RESERVE_BYTES = 104 * MIB


def _native_publication_fixture_mode(plan, *, execution_id, reserve=None):
    """Select one full constructor profile; this grants no execution authority.

    The caller's existing read ledger prepays the additional projection and
    canonical workspace. Original input bytes and their hashes remain untouched.
    """
    if plan.get('root_definition_id') != 'native-publication-fixture-v1':
        return None
    if execution_id != 'execution-1':
        raise ValueError('native publication fixture execution differs')
    from qcl_negf_results.model import canonical_bytes

    if reserve is not None:
        reserve(64 * 2048)  # Bounded depth-first accounting traversal, before copying.
    count = 0
    def workspace(value, depth=0):
        nonlocal count
        count += 1
        if depth > 64 or count > 16384:
            raise ValueError('native publication fixture object bound')
        base = 128
        if type(value) is str:
            return base + 8 * len(value)
        if type(value) is dict:
            return base + 96 * len(value) + sum(
                workspace(k, depth + 1) + workspace(v, depth + 1)
                for k, v in value.items())
        if type(value) is list:
            return base + 16 * len(value) + sum(workspace(v, depth + 1) for v in value)
        if type(value) is int:
            return base + 8 * (value.bit_length() // 3 + 2)
        if value is None or type(value) in (bool, float):
            return base
        raise ValueError('native publication fixture value type')
    charge = 4 * workspace(plan)  # Copy, normalized strings, canonical children and joins.
    if reserve is not None:
        reserve(charge)
    value = deepcopy(plan)
    value.pop('fingerprint', None)
    names = {'reference-2019-70k.yaml', 'analytical-diagnostic.yaml',
             'native-publication-fixture.yaml'}
    try:
        for execution in value['executions']:
            histories = [execution['provenance']['sources']] + [
                section['input_assigning_histories']
                for section in execution['resolved_snapshot']['sections'].values()]
            for history in histories:
                if type(history) is not dict:
                    raise ValueError('native publication fixture history shape')
                for key, locators in history.items():
                    if type(locators) is not list or any(type(v) is not str for v in locators):
                        raise ValueError('native publication fixture locator shape')
                    normalized = []
                    for locator in locators:
                        path, separator, fragment = locator.partition('#')
                        if path.startswith('/'):
                            name = os.path.basename(path)
                            if name not in names:
                                raise ValueError('native publication fixture unknown source')
                            locator = 'fixture-source:' + name + separator + fragment
                        normalized.append(locator)
                    history[key] = normalized
            execution['resource_estimate'].pop('worker_capacity', None)
        digest = hashlib.sha256(canonical_bytes(value)).hexdigest()
    except (KeyError, TypeError) as error:
        raise ValueError('native publication fixture profile shape') from error
    # Independently matched against the source-owned Julia planner's full v3 profile.
    if digest != '376aedbbfa31311f40bdffdb988242613629d128c0a97e8cdd2d6b14b8c42e47':
        raise ValueError('native publication fixture full profile differs')
    return 'native-publication-v1'


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
