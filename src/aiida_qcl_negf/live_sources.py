"""Active selection/mapping only; filesystem work stays in detached Results child.

The historical L1 registration and its quiescence predicates are not called.
"""
from __future__ import annotations

from pathlib import PurePosixPath

from . import saved_sources as saved


def get_live_history_source(run_uuid, execution_id, *, attempt, calcjob_uuid,
                            point_id, source_attempt, namespace_id):
    from . import service

    saved._uuid(run_uuid); saved._uuid(calcjob_uuid)
    saved._positive(attempt); saved._positive(source_attempt)
    if (type(point_id) is not str or not service.IDENTIFIER.fullmatch(point_id)
            or type(namespace_id) is not str or not service.IDENTIFIER.fullmatch(namespace_id)):
        saved._fail("invalid_binding")
    child, selected_attempt, selected_uuid = service._export_child(
        run_uuid, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid)
    if (selected_attempt, selected_uuid) != (attempt, calcjob_uuid):
        saved._fail("source_identity_mismatch")
    if "b0_reference" in child.inputs:
        saved._fail("unsupported_contract", "ordinary_jobs_only")
    try:
        remote = child.outputs.remote_folder
        if remote.creator.uuid != child.uuid or remote.computer.uuid != child.inputs.code.computer.uuid:
            saved._fail("source_identity_mismatch")
        saved._uuid(remote.uuid); saved._uuid(remote.computer.uuid)
        binding = saved._bindings[namespace_id]
        if binding.get("live_enabled") is not True:
            saved._fail("source_unavailable", "active_namespace_not_enabled")
        if binding["computer_uuid"] != remote.computer.uuid:
            saved._fail("source_identity_mismatch")
        # The trusted namespace supplies the mapping; never accept caller host paths.
        root = PurePosixPath(saved._absolute(binding["remote_root"]))
        remote_path = PurePosixPath(saved._absolute(remote.get_remote_path()))
        relative = saved._relative(str(remote_path.relative_to(root)))
        storage_identity = binding["storage_identity"]
        if (type(storage_identity) is not str or not 1 <= len(storage_identity) <= 256
                or storage_identity.startswith("/") or any(ord(c)<32 for c in storage_identity)):
            saved._fail("invalid_binding")
    except (AttributeError, KeyError, ValueError) as error:
        if isinstance(error, saved.SourceError):
            raise
        raise saved.SourceError("source_unavailable", "active_mapping_required") from error
    run = service._node(run_uuid)
    budget = saved._Budget(2 * saved.MAX_METADATA)
    _, _, plan = saved._plan(run, child, saved.MAX_METADATA, budget)
    points = [point for point in plan["points"]
              if point["id"] == point_id and point["execution_id"] == execution_id]
    if len(points) != 1:
        saved._fail("source_identity_mismatch", "point_not_in_frozen_execution")
    selection = {"run_uuid": run_uuid, "calcjob_uuid": calcjob_uuid, "execution_id": execution_id,
                 "attempt": attempt, "point_id": point_id, "source_attempt": source_attempt,
                 "plan_fingerprint": plan["fingerprint"]}
    # No source_attempt<=input attempt guess: native collision attempts are independent.
    # The child must verify descriptor/current point and raw native index against this
    # exact selection, then copy selected closed leaves through same-open FDs.
    return {"schema": "qcl-negf.live-source.v1", "status": "available", "selection": selection,
            "access_mode": "selected_closed_copy", "scope": "closed_prefix",
            "storage": {"namespace_id": namespace_id, "storage_identity": storage_identity,
                        "computer_uuid": remote.computer.uuid, "remote_folder_uuid": remote.uuid,
                        "relative_job_root": relative},
            "descriptor_path": relative + "/live/current.json",
            "process_state": child.process_state.value if child.process_state else "created",
            "descriptor_verified": False, "payload_status": "not_measured"}
