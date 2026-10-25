"""Bind one retained close sidecar to independently validated scientific pins.

No NFS, catalog/latest lookup, scientific acceptance or retrospective mutation.
"""
from __future__ import annotations

import hashlib
import json

from qcl_negf_contracts.artifacts import relative_path
from qcl_negf_contracts.messages import decode

MAX_METADATA = 1024 * 1024
SELECTION = ("calcjob_uuid", "execution_id", "attempt", "plan_fingerprint")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def point_pins(points, commits, selection, source_owners=None):
    """Use parser-verified commit bytes/identities, never the sidecar's claims."""
    if not isinstance(points, list) or len(points) > 256:
        raise ValueError("bounded scientific point inventory required")
    pins, seen = [], set()
    for point in points:
        point_id = point["id"]
        if point_id in seen or point["execution_id"] != selection["execution_id"]:
            raise ValueError("duplicate/foreign scientific point")
        seen.add(point_id)
        reference = (point.get("data") or {}).get("result_commit")
        if reference is None:
            continue
        reference = relative_path(reference)
        native = commits[point_id]
        identity = native["identity"]
        source_attempt = (point.get("data") or {}).get("checkpoint_source_attempt", point["attempt"])
        expected = {"execution_id": selection["execution_id"], "point_id": point_id,
                    "attempt": source_attempt, "plan_fingerprint": selection["plan_fingerprint"]}
        if (type(source_attempt) is not int or source_attempt < 1 or native["path"] != reference
                or any(type(identity.get(k)) is not type(v) or identity[k] != v for k, v in expected.items())):
            raise ValueError("native source attempt/commit contradicts scientific result")
        owner = (source_owners or {}).get(point_id)
        if "calcjob_uuid" in identity and (owner is None or identity["calcjob_uuid"] != owner):
            raise ValueError("native CalcJob identity lacks its exact provenance relation")
        pins.append({"point_id": point_id, "source_attempt": source_attempt,
                     "point_commit_path": "result/" + reference,
                     "point_commit_sha256": native["commit_sha256"]})
    return pins


def verify_sidecar(raw, *, selection, result_raw, points, commits, source_owners=None):
    """Pure bounded association after the owning parser has pinned science."""
    from qcl_negf_results.performance_views import validate_performance_binding

    if type(raw) is not bytes or len(raw) > MAX_METADATA:
        raise ValueError("performance sidecar exceeds retained metadata cap")
    if set(selection) != set(SELECTION):
        raise ValueError("exact execution selection required")
    value = decode(raw, maximum=MAX_METADATA)
    expected_pins = point_pins(points, commits, selection, source_owners)
    _, summary = validate_performance_binding(
        value, expected_selection=selection, expected_point_pins=expected_pins,
        maximum=MAX_METADATA,
    )
    result_pin = {"path": "result/series_result.json", "bytes": len(result_raw), "sha256": sha(result_raw)}
    if value["runner_result"] != result_pin:
        raise ValueError("performance sidecar does not bind the exact retrieved scientific result")
    return {"selection": dict(selection), "sidecar_sha256": sha(raw), "sidecar_bytes": len(raw),
            "point_pins": expected_pins, "runner_result": result_pin,
            "session_id": summary["session_id"], "commit_sha256": value["commit"]["sha256"],
            "summary_sha256": value["summary"]["sha256"], "scope": "execution_attempt"}


def retained_source(child, *, maximum=MAX_METADATA):
    """Rebind exact retained output node/hash; no remote transport or host paths."""
    try:
        pin_node = child.outputs.selection
    except (AttributeError, KeyError):
        return {"status": "unavailable", "reason": "performance_binding_missing"}
    pin = pin_node.get_dict()
    reference = pin.get("performance_binding")
    if reference is None:
        if pin.get("performance_status") == "invalid":
            return {"status": "unavailable", "reason": "performance_binding_rejected"}
        return {"status": "unavailable", "reason": "performance_binding_missing"}
    try:
        sidecar = child.outputs.performance_binding
    except (AttributeError, KeyError) as error:
        raise ValueError("pinned performance output is absent") from error
    if (pin_node.creator.uuid != child.uuid or sidecar.creator.uuid != child.uuid
            or reference["sidecar_uuid"] != sidecar.uuid):
        raise ValueError("performance retained-node provenance changed")
    selection = {"calcjob_uuid": child.uuid, "execution_id": child.inputs.execution_id.value,
                 "attempt": child.inputs.attempt.value,
                 "plan_fingerprint": reference["selection"]["plan_fingerprint"]}
    if reference["selection"] != selection or pin["result_sha256"] != reference["runner_result"]["sha256"]:
        raise ValueError("performance selection/result pin contradicts its CalcJob")
    with sidecar.open(mode="rb") as stream:
        raw = stream.read(min(MAX_METADATA, maximum) + 1)
    if (len(raw) > min(MAX_METADATA, maximum) or len(raw) != reference["sidecar_bytes"]
            or sha(raw) != reference["sidecar_sha256"]):
        raise ValueError("retained performance bytes changed")
    from qcl_negf_results.performance_views import validate_performance_binding
    value = decode(raw, maximum=MAX_METADATA)
    _, summary = validate_performance_binding(value, expected_selection=selection,
                                             expected_point_pins=reference["point_pins"])
    if value["runner_result"] != reference["runner_result"]:
        raise ValueError("retained performance result differs")
    if (reference["scope"] != "execution_attempt" or reference["session_id"] != summary["session_id"]
            or reference["commit_sha256"] != value["commit"]["sha256"]
            or reference["summary_sha256"] != value["summary"]["sha256"]):
        raise ValueError("retained performance session/commit/summary pin differs")
    binding = {key: reference[key] for key in ("selection", "sidecar_sha256", "point_pins", "runner_result")}
    result = {"status": "available", "scope": "execution_attempt", "binding": binding,
              "sidecar_uuid": sidecar.uuid, "sidecar_utf8": raw.decode("utf-8")}
    if len(json.dumps(result, ensure_ascii=False).encode()) > 2 * MAX_METADATA:
        raise ValueError("performance source response exceeds cap")
    return result
