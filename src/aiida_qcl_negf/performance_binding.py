"""Read typed selected performance, explicitly retiring the old close wire.

No NFS, catalog/latest lookup, scientific acceptance or retrospective mutation.
"""
from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcl_negf_results.state import StateReader
    from qcl_negf_results.performance_views import TelemetrySelection, PerformanceLimits

from qcl_negf_contracts.artifacts import relative_path

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
    """Retained callable signature; the removed v1 wire cannot be reverified."""
    raise ValueError("legacy_performance_binding_v1_retired")


def verify_native_performance(reader: StateReader, *, telemetry: TelemetrySelection,
                              limits: PerformanceLimits | None = None) -> dict:
    """Read a public typed view, keeping caller assertions separate from origin.

    Current A has no technical operand. A manually supplied TelemetrySelection
    does not become an actual ORM creator certificate through this function.
    """
    from qcl_negf_results.performance_views import read_performance_summary
    if telemetry is None:
        return {"status":"unavailable","reason":"native_technical_context_unavailable"}
    return read_performance_summary(reader,telemetry=telemetry,limits=limits)


def retained_source(child, *, maximum=MAX_METADATA):
    """Rebind exact retained output node/hash; no remote transport or host paths."""
    try:
        pin_node = child.outputs.selection
    except (AttributeError, KeyError):
        return {"status": "unavailable", "reason": "performance_binding_missing"}
    pin = pin_node.get_dict()
    if "native_binding" in pin:
        return {"status":"unavailable","reason":"native_technical_context_unavailable"}
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
    # Original UUID/creator/bytes checks above still protect retained data. The
    # legacy payload supplies no current typed contexts, so never claim verified
    # or available metrics from the retired wire.
    return {"status":"unavailable","reason":"legacy_performance_binding_v1_retired",
            "sidecar_uuid":sidecar.uuid,"sidecar_utf8":raw.decode("utf-8"),
            "integrity":{"native_contexts":"not_reverified","current_access":"not_checked"}}
