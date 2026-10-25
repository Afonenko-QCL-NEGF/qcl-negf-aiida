"""Native counters are not CalcJob attempt IDs; only retained science proves owner."""
from __future__ import annotations

import hashlib

from qcl_negf_contracts.artifacts import relative_path
from qcl_negf_contracts.messages import decode


def validate_native_collisions(points, commits, *, input_attempt, execution_id,
                               plan_fingerprint, calcjob_uuid, scientific_fingerprint=None,
                               result_scientific_fingerprint=None):
    """Narrow greater-than-input exception after existing artifact verification."""
    for row in points:
        native = row["attempt"]
        if type(native) is not int or native < 1:
            raise ValueError("invalid native point attempt")
        if native <= input_attempt:
            continue
        if scientific_fingerprint is None or result_scientific_fingerprint != scientific_fingerprint:
            raise ValueError("native collision scientific fingerprint differs")
        data = row.get("data") or {}
        if (data.get("checkpoint_source_attempt") is not None or not data.get("result_commit")
                or row["execution_id"] != execution_id
                or data.get("artifact_root") != f"executions/{execution_id}/{row['id']}/attempt-{native}"):
            raise ValueError("unproven native attempt collision")
        pin = commits[row["id"]]
        identity = pin["identity"]
        expected = {"point_id": row["id"], "execution_id": execution_id,
                    "attempt": native, "plan_fingerprint": plan_fingerprint}
        if (relative_path(data["result_commit"]) != pin["path"]
                or any(type(identity.get(k)) is not type(v) or identity[k] != v for k, v in expected.items())
                or pin.get("receipt_status") != "verified"
                or not isinstance(pin.get("state_id"), str) or not pin["state_id"]
                or type(pin.get("state_sequence")) is not int or pin["state_sequence"] < 1
                or identity.get("state_id") != pin["state_id"] or identity.get("state_sequence") != pin["state_sequence"]
                or ("calcjob_uuid" in identity and identity["calcjob_uuid"] != calcjob_uuid)):
            raise ValueError("native collision lacks exact verified state identity")
        if row.get("status") in ("completed", "completed_with_warnings") and pin["path"] != f"archive/{execution_id}/{row['id']}/final/commit.json":
            raise ValueError("native collision final is not canonical")


def source_point_owner(child, source_attempt, plan_uuid, *, point_id, commit_sha256,
                       identity, proofs=None, budget=None, scientific_fingerprint=None):
    """Current/direct recovery creators only, cached bounded repository metadata.

    Missing/ambiguous relations stay unknown. Observer data never enters proof.
    Callers may seed current proof with their already verified raw result+pin.
    """
    from . import saved_sources as saved
    proofs = {} if proofs is None else proofs
    budget = saved._Budget(8 * saved.MAX_METADATA) if budget is None else budget

    def proof(candidate):
        if candidate.uuid in proofs:
            return proofs[candidate.uuid]
        try:
            if candidate.inputs.plan.uuid != plan_uuid:
                return None
            result, selection = candidate.outputs.result, candidate.outputs.selection
            if result.creator.uuid != candidate.uuid or selection.creator.uuid != candidate.uuid:
                return None
            pin = selection.get_dict()
            budget.charge(len(saved._canonical(pin)))
            raw = saved._node_bytes(result, saved.MAX_METADATA, budget)
            if (pin["calcjob_uuid"] != candidate.uuid or pin["attempt"] != candidate.inputs.attempt.value
                    or pin["execution_id"] != candidate.inputs.execution_id.value
                    or pin["result_sha256"] != hashlib.sha256(raw).hexdigest()):
                return None
            value = decode(raw, maximum=saved.MAX_METADATA)
            proofs[candidate.uuid] = (value, pin)
            return value, pin
        except (AttributeError, KeyError, ValueError, OSError):
            proofs[candidate.uuid] = None
            return None

    def owns(candidate):
        try:
            if candidate.inputs.plan.uuid != plan_uuid or candidate.inputs.execution_id.value != identity["execution_id"]:
                return False
            pair = proof(candidate)
            if pair is None:
                return False
            series, pin = pair
            rows = [r for r in series["points"] if r["id"] == point_id and r["execution_id"] == identity["execution_id"]]
            if len(rows) != 1 or series["plan_fingerprint"] != identity["plan_fingerprint"]:
                return False
            row = rows[0]; native = pin["commits"][point_id]
            if (row["attempt"] != source_attempt or (row.get("data") or {}).get("checkpoint_source_attempt") is not None
                    or (row.get("data") or {}).get("result_commit") != native["path"]
                    or native["commit_sha256"] != commit_sha256 or native["identity"] != identity
                    or source_attempt < candidate.inputs.attempt.value):
                return False
            if "calcjob_uuid" in identity and identity["calcjob_uuid"] != candidate.uuid:
                return False
            validate_native_collisions([row], pin["commits"], input_attempt=candidate.inputs.attempt.value,
                execution_id=identity["execution_id"], plan_fingerprint=identity["plan_fingerprint"], calcjob_uuid=candidate.uuid,
                scientific_fingerprint=scientific_fingerprint,result_scientific_fingerprint=series.get("plan_scientific_fingerprint"))
            return True
        except (AttributeError, KeyError, ValueError, OSError):
            return False

    candidates = {child.uuid: child}
    for name in ("recovery_bundle", "prior_archive"):
        try:
            prior = getattr(child.inputs, name).creator
            candidates[prior.uuid] = prior
        except (AttributeError, KeyError):
            pass
    owners = {candidate.uuid for candidate in candidates.values() if owns(candidate)}
    return next(iter(owners)) if len(owners) == 1 else None
