"""Small application boundary over a loaded AiiDA profile.

    The caller owns authentication, permitted Code UUIDs, and request limits.
    This module never starts a second scheduler or opens arbitrary host paths.
"""

from __future__ import annotations

import hashlib
import io
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO
from uuid import UUID

from aiida import orm
from aiida.common.exceptions import NotExistent
from aiida.common.links import LinkType
from aiida.engine import submit
from aiida.engine.daemon.client import get_daemon_client
from aiida.engine.processes.control import kill_processes
from qcl_negf_contracts.agent_reports import (
    MAX_AGENT_REPORT_BYTES,
    validate_agent_report,
)
from qcl_negf_contracts.agent_reports import SCHEMA as AGENT_REPORT_SCHEMA
from qcl_negf_contracts.messages import decode

from .data import plan_data, read_json, read_plan
from .recovery import MAX_ARCHIVE_BYTES
from .run_structure import project_run_structure
from .validation import (
    IDENTIFIER,
    MAX_PLAN_BYTES,
    MAX_RESULT_BYTES,
    scheduler_options,
    validate_plan,
    validate_scratch_root,
)
from .workflow import QCLPlanWorkChain

PROCESS_TYPE = "aiida.workflows:qcl_negf.plan"
MAX_ARTIFACTS = 10000
MAX_REPORTS = 1000
MAX_RESPONSE_BYTES = 32 * 1024 * 1024


def _node(identifier: str) -> orm.WorkChainNode:
    try:
        UUID(identifier)
    except (ValueError, TypeError, AttributeError) as exception:
        raise ValueError("A full process UUID is required") from exception
    try:
        node = orm.load_node(identifier)
    except NotExistent as exception:
        raise LookupError("Run not found") from exception
    if not isinstance(node, orm.WorkChainNode) or node.process_type != PROCESS_TYPE:
        raise LookupError("Run not found")
    return node


def _summary(node: orm.ProcessNode) -> dict[str, Any]:
    return {
        "uuid": node.uuid,
        "pk": node.pk,
        "label": node.label,
        "process_state": node.process_state.value if node.process_state else "created",
        "exit_status": node.exit_status,
        "is_finished_ok": node.is_finished_ok,
        "ctime": node.ctime.isoformat(),
        "mtime": node.mtime.isoformat(),
    }


def _children(node: orm.WorkChainNode) -> list[orm.CalcJobNode]:
    return sorted((child for child in node.called_descendants if isinstance(child, orm.CalcJobNode)), key=lambda child: child.ctime)


def _attempt(child):
    try:
        return child.inputs.attempt.value
    except (AttributeError, KeyError):
        return 1


def _select_child(node, execution_id, *, attempt=None, calcjob_uuid=None):
    if attempt is None and calcjob_uuid is None:
        try:
            selections = node.outputs.selections.values()
        except (AttributeError, KeyError):
            selections = []
        chosen = [item.get_dict() for item in selections if item.get_dict().get("execution_id") == execution_id]
        if len(chosen) > 1:
            raise ValueError("Conflicting published attempt selections")
        if chosen:
            attempt, calcjob_uuid = chosen[0]["attempt"], chosen[0]["calcjob_uuid"]
    candidates = [child for child in _children(node) if child.inputs.execution_id.value == execution_id]
    if attempt is not None:
        if type(attempt) is not int or attempt < 1:
            raise ValueError("Require a positive attempt")
        candidates = [child for child in candidates if _attempt(child) == attempt]
    if calcjob_uuid is not None:
        candidates = [child for child in candidates if child.uuid == calcjob_uuid]
    if len(candidates) > 1:
        raise ValueError("Multiple attempts exist; select an exact attempt or CalcJob UUID")
    if not candidates:
        raise LookupError("Selected attempt is unavailable")
    return candidates[0]


def _export_uuid(value, message):
    if not isinstance(value, str):
        raise ValueError(message)
    try:
        return str(UUID(value))
    except (ValueError, TypeError, AttributeError) as exception:
        raise ValueError(message) from exception


def _export_child_identity(child):
    try:
        try:
            attempt_input = child.inputs.attempt
        except (AttributeError, KeyError):
            attempt = 1  # Only an absent input permits the legacy default.
        else:
            attempt = attempt_input.value
        uuid = _export_uuid(child.uuid, "Selected child identity is invalid")
        if type(attempt) is not int or attempt < 1:
            raise ValueError("Selected child identity is invalid")
    except (AttributeError, KeyError) as exception:
        raise ValueError("Selected child identity is invalid") from exception
    return attempt, uuid


def _export_child(identifier, execution_id, *, attempt=None, calcjob_uuid=None,
                  _bound_node=None):
    """Validate an export request and bind existing selection authority once."""
    identifier = _export_uuid(identifier, "A full process UUID is required")
    if not isinstance(execution_id, str) or not 1 <= len(execution_id) <= 128:
        raise ValueError("A nonempty execution ID of at most 128 characters is required")
    if attempt is not None and (type(attempt) is not int or attempt < 1):
        raise ValueError("Require a positive attempt")
    if calcjob_uuid is not None:
        calcjob_uuid = _export_uuid(calcjob_uuid, "A full CalcJob UUID is required")
    node = _node(identifier) if _bound_node is None else _bound_node
    published = False
    if attempt is None and calcjob_uuid is None:
        try:
            selections = node.outputs.selections.values()
        except (AttributeError, KeyError):
            selections = []
        chosen = []
        for item in selections:
            try:
                record = item.get_dict()
                if not isinstance(record, dict):
                    raise ValueError("Published attempt selection is invalid")
                record_execution_id = record.get("execution_id")
                if (not isinstance(record_execution_id, str)
                        or not 1 <= len(record_execution_id) <= 128):
                    raise ValueError("Published attempt selection is invalid")
                if record_execution_id == execution_id:
                    chosen.append(record)
            except Exception as exception:
                raise ValueError("Published attempt selection is invalid") from exception
        if len(chosen) > 1:
            raise ValueError("Conflicting published attempt selections")
        if chosen:
            published = True
            record = chosen[0]
            attempt = record.get("attempt")
            if type(attempt) is not int or attempt < 1:
                raise ValueError("Published attempt selection is invalid")
            calcjob_uuid = _export_uuid(record.get("calcjob_uuid"),
                                       "Published attempt selection is invalid")
    candidates = [child for child in _children(node)
                  if child.inputs.execution_id.value == execution_id]
    if calcjob_uuid is not None:
        candidates = [child for child in candidates if child.uuid == calcjob_uuid]
        if not candidates:
            if published:
                raise ValueError("Published attempt selection is invalid")
            raise LookupError("Selected attempt is unavailable")
        for child in candidates:
            stored_attempt, _ = _export_child_identity(child)
            if attempt is not None and stored_attempt != attempt:
                message = ("Published attempt selection is invalid" if published else
                           "Attempt and CalcJob UUID select different children")
                raise ValueError(message)
    elif attempt is not None:
        candidates = [child for child in candidates if _attempt(child) == attempt]
    if len(candidates) > 1:
        raise ValueError("Multiple attempts exist; select an exact attempt or CalcJob UUID")
    if not candidates:
        raise LookupError("Selected attempt is unavailable")
    effective_attempt, effective_uuid = _export_child_identity(candidates[0])
    # Always explicit: _select_child must never reread a changing publication.
    child = _select_child(node, execution_id, attempt=effective_attempt,
                          calcjob_uuid=effective_uuid)
    if _export_child_identity(child) != (effective_attempt, effective_uuid):
        raise ValueError("Selected child identity is invalid")
    return child, effective_attempt, effective_uuid


def get_export_selection(identifier: str, execution_id: str, *, attempt=None,
                         calcjob_uuid=None) -> dict[str, Any]:
    """Return detached exact child identity and its recorded scientific status."""
    child, attempt, calcjob_uuid = _export_child(
        identifier, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid)
    status = "unknown"
    if "result" in child.outputs:
        try:
            result = read_json(child.outputs.result, limit=MAX_RESULT_BYTES)
            status = result.get("status")
            if (not isinstance(status, str) or status not in {
                    "running", "completed", "completed_with_warnings", "failed", "cancelled", "paused"}
                    or result.get("selected_execution_id") != execution_id):
                raise ValueError("Invalid recorded status or execution identity")
        except Exception as exception:
            raise ValueError("Selected scientific result metadata is invalid") from exception
    return {"execution_id": execution_id, "attempt": attempt,
            "calcjob_uuid": calcjob_uuid, "status": status}


def list_execution_artifacts(identifier: str, execution_id: str, *, attempt=None,
                             calcjob_uuid=None) -> list[dict[str, Any]]:
    """Return the selected child's bounded cached inventory without payload I/O."""
    child, attempt, calcjob_uuid = _export_child(
        identifier, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid)
    if "retrieved" not in child.outputs or "inventory" not in child.outputs:
        raise LookupError("Retrieved execution artifact inventory is unavailable")
    try:
        inventory = child.outputs.inventory.get_dict()
        if (not isinstance(inventory, dict) or inventory.get("complete") is not True
                or not isinstance(inventory.get("files"), list)
                or len(inventory["files"]) > MAX_ARTIFACTS):
            raise ValueError("Invalid inventory")
        files, paths = [], set()
        for row in inventory["files"]:
            if not isinstance(row, dict) or not isinstance(row.get("path"), str):
                raise ValueError("Invalid artifact row")
            path, size = row["path"], row.get("size")
            _artifact_path(path)
            if path in paths or type(size) is not int or size < 0:
                raise ValueError("Invalid artifact path or size")
            paths.add(path)
            files.append({"execution_id": execution_id, "attempt": attempt,
                          "calcjob_uuid": calcjob_uuid, "path": path, "size": size})
    except Exception as exception:
        raise ValueError("Selected artifact inventory is invalid or incomplete") from exception
    return sorted(files, key=lambda row: row["path"])


def list_runs(limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    """List only QCL-NEGF workflows, newest first."""
    if type(limit) is not int or not 1 <= limit <= 200 or type(offset) is not int or offset < 0:
        raise ValueError("Require 1 <= limit <= 200 and offset >= 0")
    query = orm.QueryBuilder().append(orm.WorkChainNode, filters={"process_type": PROCESS_TYPE}, tag="run")
    query.order_by({"run": {"ctime": "desc"}}).limit(limit).offset(offset)
    return [_summary(node) for node in query.all(flat=True)]


RECOVERY_PROCESS_TYPE = "aiida.workflows:qcl_negf.execution_restart"
RECOVERY_CALC_TYPE = "aiida.calculations:qcl_negf.execution"


def _recovery_optional(namespace, name):
    try:
        return getattr(namespace, name)
    except (AttributeError, KeyError):
        return None


def _recovery_attempt(node):
    item = _recovery_optional(node.inputs, "attempt")
    if item is None:
        return None
    value = getattr(item, "value", None)
    if type(value) is not int or value < 1:
        raise ValueError("Invalid recovery attempt input")
    return value


def _recovery_uuid(namespace, name):
    item = _recovery_optional(namespace, name)
    return item.uuid if item is not None else None


def _recovery_summary(node):
    # Only a real CALL_WORK edge establishes a caller. Shared inputs do not.
    query = orm.QueryBuilder().append(
        orm.WorkChainNode, filters={"id": node.pk}, tag="recovery")
    query.append(orm.ProcessNode, with_outgoing="recovery",
                 edge_filters={"type": LinkType.CALL_WORK.value}, project="*")
    callers = query.limit(2).all(flat=True)
    if len(callers) > 1:
        raise ValueError("Ambiguous recovery caller links")
    response = _summary(node)
    response.update(kind="recovery_execution", caller=(
        {"uuid": callers[0].uuid, "process_type": callers[0].process_type}
        if callers else None))
    return response


def _bounded_recovery_response(response):
    try:
        raw = json.dumps(response, ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (ValueError, TypeError) as exception:
        raise ValueError("Invalid recovery response metadata") from exception
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError("Recovery metadata exceeds the service response limit")
    return response


def list_recovery_executions(limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    """List exact restart workflows, including both standalone and called roots."""
    if type(limit) is not int or not 1 <= limit <= 200 or type(offset) is not int or offset < 0:
        raise ValueError("Require 1 <= limit <= 200 and offset >= 0")
    query = orm.QueryBuilder().append(
        orm.WorkChainNode, filters={"process_type": RECOVERY_PROCESS_TYPE}, tag="recovery")
    query.order_by({"recovery": [{"ctime": "desc"}, {"id": "desc"}]}).limit(limit).offset(offset)
    return _bounded_recovery_response([_recovery_summary(node) for node in query.all(flat=True)])


def get_recovery_execution(identifier: str) -> dict[str, Any]:
    """Read bounded direct provenance and stored scheduler metadata, never science payloads."""
    try:
        if not isinstance(identifier, str) or str(UUID(identifier)) != identifier:
            raise ValueError("Noncanonical UUID")
    except (ValueError, TypeError, AttributeError) as exception:
        raise ValueError("A full canonical process UUID is required") from exception
    try:
        node = orm.load_node(identifier)
    except NotExistent as exception:
        raise LookupError("Recovery execution not found") from exception
    if not isinstance(node, orm.WorkChainNode) or node.process_type != RECOVERY_PROCESS_TYPE:
        raise LookupError("Recovery execution not found")
    response = _recovery_summary(node)
    try:
        plan_node = node.inputs.plan
        plan = read_plan(plan_node)  # Existing 16 MiB read bound, no artifact arrays.
        fingerprint = plan["fingerprint"]
        if not isinstance(fingerprint, str) or not fingerprint:
            raise ValueError("Missing fingerprint")
    except Exception as exception:
        raise ValueError("Invalid recovery plan metadata") from exception
    execution_input = _recovery_optional(node.inputs, "execution_id")
    execution_id = getattr(execution_input, "value", None)
    if execution_input is not None and (
            not isinstance(execution_id, str) or not 1 <= len(execution_id) <= 128):
        raise ValueError("Invalid recovery execution_id input")
    response.update(
        plan={"uuid": plan_node.uuid, "fingerprint": fingerprint},
        execution_id=execution_id, attempt=_recovery_attempt(node),
        recovery={"input_bundle_uuid": _recovery_uuid(node.inputs, "recovery_bundle"),
                  "input_prior_archive_uuid": _recovery_uuid(node.inputs, "prior_archive")},
        children=[])
    query = orm.QueryBuilder().append(
        orm.WorkChainNode, filters={"id": node.pk}, tag="recovery")
    # Select every direct CALL_CALC edge before validating its type. Filtering by
    # process_type here would hide malformed/foreign provenance from the reader.
    query.append(orm.Node, with_incoming="recovery", tag="child", project="*",
                 edge_filters={"type": LinkType.CALL_CALC.value}, edge_project="label")
    query.order_by({"child": [{"ctime": "asc"}, {"id": "asc"}]}).limit(101)
    children = query.all()
    if len(children) > 100:
        raise ValueError("Recovery has more than 100 direct child links")
    for child, call_label in children:
        if not isinstance(child, orm.CalcJobNode) or child.process_type != RECOVERY_CALC_TYPE:
            raise ValueError("Invalid recovery child process type")
        state = child.get_scheduler_state()
        summary = _summary(child)
        summary.update(
            attempt=_recovery_attempt(child), call_label=call_label,
            job_id=child.get_job_id(), scheduler_state=state.value if state is not None else None,
            retrieved_uuid=_recovery_uuid(child.outputs, "retrieved"),
            result_uuid=_recovery_uuid(child.outputs, "result"))
        response["children"].append(summary)
    return _bounded_recovery_response(response)


def get_run(identifier: str) -> dict[str, Any]:
    """Read the current workflow and scientific outputs, including partial results."""
    node = _node(identifier)
    response = _summary(node)
    plan = read_plan(node.inputs.plan)
    response["plan_fingerprint"] = plan["fingerprint"]
    response["plan_structure"] = project_run_structure(plan)
    response["plan_points"] = [
        {key: point[key] for key in ("id", "execution_id", "temperature_K", "voltage_per_period_V")}
        for point in plan["points"]
    ]
    response["results"] = {}
    response["children"] = []
    remaining = MAX_RESPONSE_BYTES - len(json.dumps(response, ensure_ascii=False).encode("utf-8")) - 1024
    if remaining < 1:
        raise ValueError("Run metadata exceeds the service response limit; use AiiDA directly")
    for child in _children(node):
        execution_id = child.inputs.execution_id.value
        summary = _summary(child)
        summary["execution_id"] = execution_id
        summary["attempt"] = _attempt(child)
        summary["retrieved_uuid"] = child.outputs.retrieved.uuid if "retrieved" in child.outputs else None
        remaining -= len(json.dumps(summary, ensure_ascii=False).encode("utf-8")) + 256
        if remaining < 1:
            raise ValueError("Run metadata exceeds the service response limit; use AiiDA directly")
        response["children"].append(summary)
        if "result" in child.outputs:
            result = read_json(child.outputs.result, limit=remaining)
            remaining -= len(json.dumps(result, ensure_ascii=False).encode("utf-8"))
            if remaining < 1:
                raise ValueError("Run metadata exceeds the service response limit; use AiiDA directly")
            try:
                selected = _select_child(node, execution_id)
            except (ValueError, LookupError):
                continue
            if selected.uuid == child.uuid:
                response["results"][execution_id] = result
    if len(json.dumps(response, ensure_ascii=False).encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise ValueError("Run metadata exceeds the service response limit; use AiiDA directly")
    return response


def get_export_plan(identifier: str, execution_id: str, *, attempt=None, calcjob_uuid=None) -> dict[str, Any]:
    """Read exact frozen bytes while the caller's AiiDA actor owns every handle.

    Prefer the retrieved worker snapshot. Only an absent repository entry permits
    fallback to the original file-backed workflow input; a corrupt, unreadable or
    different retrieved plan must fail the export rather than disappear silently.
    The returned value contains bytes and scalar provenance, never an ORM node.
    """
    node = _node(identifier)
    child = _select_child(node, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid)
    if "retrieved" not in child.outputs:
        raise LookupError("Retrieved execution provenance is unavailable")
    return _frozen_plan_for_child(node, child, execution_id)


def _frozen_plan_for_child(node, child, execution_id) -> dict[str, Any]:
    """Read one already selected child's frozen plan, closing all actor handles.

    Running children may have only the original input. If a retrieved object is
    present, its corruption or read failure must never become an input fallback.
    Callers needing completed export provenance enforce that gate separately.
    """

    def read_bounded(handle) -> bytes:
        raw = handle.read(MAX_PLAN_BYTES + 1)
        if len(raw) > MAX_PLAN_BYTES:
            raise ValueError("Frozen export plan exceeds the metadata size limit")
        return raw

    try:
        input_plan = node.inputs.plan
    except (AttributeError, KeyError) as exception:
        raise ValueError("Frozen file-backed input plan is unavailable") from exception
    with input_plan.open(mode="rb") as handle:
        input_bytes = read_bounded(handle)
    expected = validate_plan(decode(input_bytes, maximum=MAX_PLAN_BYTES))
    if execution_id not in {item["id"] for item in expected["executions"]}:
        raise LookupError("Execution is absent from the frozen input plan")

    if "retrieved" not in child.outputs:
        return {"plan": input_bytes, "source": "aiida.input.plan",
                "sha256": hashlib.sha256(input_bytes).hexdigest(), "bytes": len(input_bytes)}
    repository = child.outputs.retrieved.base.repository
    path = "result/scientific_plan.json"
    try:
        repository.get_object(path)
    except FileNotFoundError:
        raw, source = input_bytes, "aiida.input.plan"
    else:
        # Presence is established before opening: read/open failures never take
        # the missing-entry fallback, including a corrupt repository backend.
        with repository.open(path, "rb") as handle:
            raw = read_bounded(handle)
        actual = validate_plan(decode(raw, maximum=MAX_PLAN_BYTES))
        if actual != expected:
            raise ValueError("Retrieved frozen plan differs from workflow input provenance")
        source = "aiida.retrieved:result/scientific_plan.json"
    return {"plan": raw, "source": source, "sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw)}


def _model_help_release(node, child) -> str | None:
    """Use immutable process inputs only; current deployment is not history."""
    recorded = []
    for owner in (node, child):
        try:
            release = owner.inputs.release_id
        except (AttributeError, KeyError):
            continue
        try:
            value = release.value
        except (AttributeError, KeyError) as exception:
            raise ValueError("Recorded release identity is invalid") from exception
        if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
            raise ValueError("Recorded release identity is invalid")
        recorded.append(value)
    if len(set(recorded)) > 1:
        raise ValueError("Selected child and workflow release identities differ")
    return recorded[0] if recorded else None


def get_model_help_input(run_uuid: str, execution_id: str, point_id: str,
                         attempt: int | None = None,
                         calcjob_uuid: str | None = None) -> dict[str, Any]:
    """Return exact selected context and bounded bytes, without inferred values.

    This read-only seam supplies no retained source binding or declared/used
    projection. A point's recorded configuration hash is identity metadata, not
    proof of a reconstructed configuration or scientific acceptance.
    """
    # Malformed selectors stay ordinary validation errors, outside the small
    # selection-conflict DTO boundary below.
    run_uuid = _export_uuid(run_uuid, "A full process UUID is required")
    for value in (execution_id, point_id):
        if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
            raise ValueError("Execution and point IDs must be portable identifiers")
    if attempt is not None and (type(attempt) is not int or attempt < 1):
        raise ValueError("Require a positive attempt")
    if calcjob_uuid is not None:
        calcjob_uuid = _export_uuid(calcjob_uuid, "A full CalcJob UUID is required")
    node = _node(run_uuid)
    try:
        child, selected_attempt, selected_uuid = _export_child(
            run_uuid, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid,
            _bound_node=node)
    except ValueError:
        return {"error": "selection_conflict"}
    # No error after selection is relabelled as a conflict or normal unknown.
    frozen = _frozen_plan_for_child(node, child, execution_id)
    plan = decode(frozen["plan"], maximum=MAX_PLAN_BYTES)
    points = [point for point in plan["points"]
              if point["id"] == point_id and point["execution_id"] == execution_id]
    if len(points) != 1:
        raise LookupError("Point is absent from the selected frozen execution")
    point = points[0]
    passport = point["effective_inputs"]
    if any(passport[key] != point[key] for key in ("temperature_K", "voltage_per_period_V")):
        raise ValueError("Frozen point passport axes disagree with the selected point")
    return {
        "context": {"run_uuid": _export_uuid(node.uuid, "Run identity is invalid"),
                    "execution_id": execution_id, "point_id": point_id,
                    "attempt": selected_attempt, "calcjob_uuid": selected_uuid,
                    "plan_fingerprint": plan["fingerprint"],
                    "scientific_fingerprint": plan["scientific_fingerprint"],
                    "configuration_hash": passport["configuration_hash"],
                    "model_revision": plan["model_revision"]},
        "frozen_plan": {"bytes": frozen["plan"], "sha256": frozen["sha256"]},
        "snapshot": None,
        "input_status": {"status": "unavailable", "reason": "inventory_unavailable"},
        "release_id": _model_help_release(node, child),
    }


def submit_plan(plan: dict[str, Any] | str | bytes, code_uuid: str, resources: dict[str, Any], label: str = "", *,
                scratch_root: str | None = None, release_id: str | None = None, max_attempts: int = 3,
                retry_backoff_seconds: int = 10, archive_byte_budget: int = MAX_ARCHIVE_BYTES,
                b0_slot=None) -> dict[str, Any]:
    """Submit a frozen plan to the daemon after validating every admission input."""
    if b0_slot is not None:
        from .b0 import submit_bound_plan
        from .validation import validated_plan_bytes
        raw, digest, metadata = validated_plan_bytes(plan)
        return submit_bound_plan(raw, digest, metadata, code_uuid, resources, label,
            slot=b0_slot, release_id=release_id, scratch_root=scratch_root,
            max_attempts=max_attempts)
    plan_node = plan_data(plan)
    scheduler_options(resources)
    if type(max_attempts) is not int or not 1 <= max_attempts <= 100:
        raise ValueError("max_attempts must be between 1 and 100")
    if type(retry_backoff_seconds) is not int or not 0 <= retry_backoff_seconds <= 300:
        raise ValueError("retry_backoff_seconds must be between 0 and 300")
    if type(archive_byte_budget) is not int or archive_byte_budget < 1:
        raise ValueError("archive_byte_budget must be positive")
    options = {"max_attempts": orm.Int(max_attempts), "retry_backoff_seconds": orm.Int(retry_backoff_seconds),
               "archive_byte_budget": orm.Int(archive_byte_budget)}
    deployment_release = os.environ.get("QCL_NEGF_RELEASE_ID")
    deployment_solver = os.environ.get("QCL_NEGF_SOLVER_EXECUTABLE")
    deployment_gate = os.environ.get("QCL_NEGF_RELEASE_GATE")
    if deployment_release is not None or deployment_solver is not None or deployment_gate is not None:
        if not deployment_release or not deployment_solver:
            raise ValueError("Deployment release_id and solver executable must be supplied together")
        if not IDENTIFIER.fullmatch(deployment_release):
            raise ValueError("Deployment release_id must be a portable immutable identity")
        solver_path = PurePosixPath(deployment_solver)
        if (not deployment_solver.startswith("/nix/store/") or ".." in solver_path.parts
                or str(solver_path) != deployment_solver
                or any(ord(character) < 32 for character in deployment_solver)):
            raise ValueError("Deployment solver executable must be an immutable Nix path")
        if release_id is not None and release_id != deployment_release:
            raise ValueError("Explicit release_id conflicts with the deployment release")
        gate_path = deployment_gate or "/srv/qcl-negf/jobs/.release-admission.json"
        if (not Path(gate_path).is_absolute() or ".." in PurePosixPath(gate_path).parts
                or str(PurePosixPath(gate_path)) != gate_path or any(ord(character) < 32 for character in gate_path)):
            raise ValueError("Deployment application admission path must be an absolute normalized path")
        try:
            with Path(gate_path).open("rb") as stream:
                admission = decode(stream.read(64 * 1024 + 1), maximum=64 * 1024)
        except (OSError, ValueError) as exception:
            raise ValueError("Deployment application admission gate is unavailable or malformed") from exception
        if (not isinstance(admission, dict) or admission.get("open") is not True
                or admission.get("release_id") != deployment_release):
            raise ValueError("Deployment application admission is closed or selects a different release")
        release_id = deployment_release
    if release_id is not None:
        if not isinstance(release_id, str) or not IDENTIFIER.fullmatch(release_id):
            raise ValueError("release_id must be a portable immutable identity")
        options["release_id"] = orm.Str(release_id)
    if scratch_root is not None:
        options["scratch_root"] = orm.Str(validate_scratch_root(scratch_root))
    if not isinstance(label, str) or len(label) > 255:
        raise ValueError("Label must be at most 255 characters")
    try:
        UUID(code_uuid)
        code = orm.load_code(code_uuid)
    except (ValueError, TypeError, AttributeError, NotExistent) as exception:
        raise ValueError("A valid installed Code UUID is required") from exception
    if not isinstance(code, orm.InstalledCode):
        raise ValueError("Only an installed QCL-NEGF executable is supported")
    if code.default_calc_job_plugin != "qcl_negf.execution":
        raise ValueError("The Code must use the qcl_negf.execution plugin")
    if release_id is not None and not str(code.filepath_executable).startswith("/nix/store/"):
        raise ValueError("Release-pinned execution requires an immutable Nix InstalledCode path")
    if deployment_solver is not None and str(code.filepath_executable) != deployment_solver:
        raise ValueError("Selected Code executable differs from the deployment solver executable")
    if not get_daemon_client().is_daemon_running:
        raise RuntimeError("AiiDA daemon is not running")
    node = submit(QCLPlanWorkChain, code=code, plan=plan_node, resources=orm.Dict(dict=resources), metadata={"label": label}, **options)
    return _summary(node)


def _bound_release_code(code_uuid, release_id):
    """Same deployed gate/InstalledCode predicates, mandatory for B0.

    Only called inside an authenticated controller operation covering the gate
    file, profile, ORM lookup and daemon/broker producer paths.
    """
    deployment_release = os.environ.get("QCL_NEGF_RELEASE_ID")
    deployment_solver = os.environ.get("QCL_NEGF_SOLVER_EXECUTABLE")
    gate = os.environ.get("QCL_NEGF_RELEASE_GATE", "/srv/qcl-negf/jobs/.release-admission.json")
    if not deployment_release or not deployment_solver:
        raise ValueError("B0 requires deployed release and immutable solver")
    if not IDENTIFIER.fullmatch(deployment_release) or release_id not in (None, deployment_release):
        raise ValueError("B0 release identity mismatch")
    path = PurePosixPath(deployment_solver)
    if (not deployment_solver.startswith("/nix/store/") or ".." in path.parts
            or str(path) != deployment_solver or any(ord(c) < 32 for c in deployment_solver)):
        raise ValueError("B0 solver must be an immutable Nix path")
    if (not Path(gate).is_absolute() or ".." in PurePosixPath(gate).parts
            or str(PurePosixPath(gate)) != gate or any(ord(c) < 32 for c in gate)):
        raise ValueError("Invalid B0 release admission path")
    with Path(gate).open("rb") as stream:
        admission = decode(stream.read(64 * 1024 + 1), maximum=64 * 1024)
    if (not isinstance(admission, dict) or admission.get("open") is not True
            or admission.get("release_id") != deployment_release):
        raise ValueError("B0 deployment admission gate is closed or mismatched")
    code = orm.load_code(code_uuid)
    if (not isinstance(code, orm.InstalledCode)
            or code.default_calc_job_plugin != "qcl_negf.execution"
            or str(code.filepath_executable) != deployment_solver
            or code.computer.scheduler_type != "core.slurm"):
        raise ValueError("B0 requires matching immutable InstalledCode and Slurm Computer")
    if not get_daemon_client().is_daemon_running:
        raise RuntimeError("AiiDA daemon is not running")
    return code, deployment_release


def _agent_report_uuid(identifier: str) -> str:
    try:
        canonical = str(UUID(identifier))
    except (ValueError, TypeError, AttributeError) as exception:
        raise ValueError("A canonical full UUID is required") from exception
    if canonical != identifier:
        raise ValueError("A canonical full UUID is required")
    return identifier


def _agent_report_receipt(node, anchor_uuid: str) -> dict[str, Any]:
    """Read bounded scalar metadata only; never open an agent file here."""
    if not isinstance(node, orm.SinglefileData):
        raise LookupError("Agent report not found")
    attributes = node.base.attributes
    if attributes.get("schema") != AGENT_REPORT_SCHEMA or attributes.get("anchor_run_uuid") != anchor_uuid:
        raise LookupError("Agent report not found")
    root_id = attributes.get("root_definition_id")
    root_kind = attributes.get("root_kind")
    fingerprint = attributes.get("anchor_plan_fingerprint")
    size = attributes.get("bytes")
    digest = attributes.get("sha256")
    if (node.filename != "agent-report.json"
            or not isinstance(root_id, str) or IDENTIFIER.fullmatch(root_id) is None
            or root_kind not in ("study", "meta")
            or not isinstance(fingerprint, str) or len(fingerprint) != 64
            or any(character not in "0123456789abcdef" for character in fingerprint)
            or type(size) is not int or not 0 <= size <= MAX_AGENT_REPORT_BYTES
            or not isinstance(digest, str) or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)):
        raise ValueError("Corrupt agent report metadata")
    return {"uuid": _agent_report_uuid(node.uuid), "filename": node.filename,
            "bytes": size, "sha256": digest, "ctime": node.ctime.isoformat(),
            "anchor": {"run_uuid": anchor_uuid, "root_definition_id": root_id,
                       "root_kind": root_kind, "plan_fingerprint": fingerprint}}


def save_agent_report(anchor_uuid: str, raw: str | bytes) -> dict[str, Any]:
    """Append one author file after admitting every exact frozen run reference.

    Author prose is independent of scientific results and machine assessments.
    The validated bytes are retained unchanged; repeated calls create new nodes.
    """
    if isinstance(raw, str):
        raw = raw.encode("utf-8", "strict")
    report = validate_agent_report(raw)
    _agent_report_uuid(anchor_uuid)
    roots = {}

    def frozen(identifier):
        if identifier not in roots:
            node = _node(identifier)
            roots[identifier] = (node, read_plan(node.inputs.plan))
        return roots[identifier]

    anchor_node, anchor_plan = frozen(anchor_uuid)
    expected_anchor = {"run_uuid": anchor_node.uuid,
                       "root_definition_id": anchor_plan["root_definition_id"],
                       "root_kind": anchor_plan["root_kind"],
                       "plan_fingerprint": anchor_plan["fingerprint"]}
    if report["anchor"] != expected_anchor:
        raise ValueError("Agent report anchor differs from the frozen run")
    for reference in report["used_runs"]:
        node, plan = frozen(reference["run_uuid"])
        if reference["plan_fingerprint"] != plan["fingerprint"]:
            raise ValueError("Used run fingerprint differs from the frozen plan")
        executions = [item for item in plan["executions"] if item["id"] == reference["execution_id"]]
        if (len(executions) != 1
                or executions[0]["definition_id"] != reference["definition_id"]
                or executions[0]["variant_id"] != reference["variant_id"]):
            raise ValueError("Used execution differs from the frozen plan")
        _select_child(node, reference["execution_id"], attempt=reference["attempt"],
                      calcjob_uuid=reference["calcjob_uuid"])
    # No file constructor, attribute write or store may precede ALL admissions.
    node = orm.SinglefileData(file=io.BytesIO(raw), filename="agent-report.json")
    node.base.attributes.set_many({"schema": AGENT_REPORT_SCHEMA,
        "anchor_run_uuid": anchor_uuid, "root_definition_id": expected_anchor["root_definition_id"],
        "root_kind": expected_anchor["root_kind"], "anchor_plan_fingerprint": expected_anchor["plan_fingerprint"],
        "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
    node.store()
    return _agent_report_receipt(node, anchor_uuid)


def list_agent_reports(anchor_uuid: str, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
    """List immutable file receipts using bounded, ordered database selection."""
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
        raise ValueError("Require 1 <= limit <= 100 and offset >= 0")
    _agent_report_uuid(anchor_uuid)
    _node(anchor_uuid)
    query = orm.QueryBuilder().append(orm.SinglefileData, filters={
        "attributes.schema": AGENT_REPORT_SCHEMA, "attributes.anchor_run_uuid": anchor_uuid}, tag="report")
    query.order_by({"report": {"ctime": "desc"}}).limit(limit).offset(offset)
    reports = [_agent_report_receipt(node, anchor_uuid) for node in query.all(flat=True)]
    response = json.dumps({"reports": reports}, ensure_ascii=False, allow_nan=False,
                          separators=(",", ":")).encode("utf-8")
    if len(response) > MAX_RESPONSE_BYTES:
        raise ValueError("Agent report metadata exceeds the service response limit")
    return reports


def read_agent_report(anchor_uuid: str, report_uuid: str) -> bytes:
    """Read exact bounded file bytes and verify their immutable binding metadata."""
    _agent_report_uuid(anchor_uuid)
    _agent_report_uuid(report_uuid)
    _node(anchor_uuid)
    try:
        node = orm.load_node(report_uuid)
    except NotExistent as exception:
        raise LookupError("Agent report not found") from exception
    receipt = _agent_report_receipt(node, anchor_uuid)
    with node.open(mode="rb") as handle:
        raw = handle.read(MAX_AGENT_REPORT_BYTES + 1)
    report = validate_agent_report(raw)
    if (len(raw) != receipt["bytes"] or hashlib.sha256(raw).hexdigest() != receipt["sha256"]
            or report["anchor"] != receipt["anchor"]):
        raise ValueError("Agent report file differs from its stored metadata")
    return raw


def get_run_report(identifier: str) -> list[dict[str, Any]]:
    """Return workflow and child reports ordered by timestamp."""
    node = _node(identifier)
    processes = {process.pk: process for process in [node, *node.called_descendants]
                 if isinstance(process, orm.ProcessNode)}
    logs = orm.Log.collection.find(filters={"dbnode_id": {"in": list(processes)}}, order_by=[{"time": "desc"}], limit=MAX_REPORTS)
    reports = [{"level": report.levelname, "message": report.message, "time": report.time.isoformat(), "process_uuid": processes[report.dbnode_id].uuid} for report in logs]
    reports = sorted(reports, key=lambda report: report["time"])
    response_bytes = json.dumps(
        {"entries": reports}, ensure_ascii=False, allow_nan=False,
        indent=None, separators=(",", ":"),
    ).encode("utf-8")
    if len(response_bytes) > MAX_RESPONSE_BYTES:
        raise ValueError("Report metadata exceeds the service response limit; use AiiDA directly")
    return reports


def kill_run(identifier: str) -> dict[str, Any]:
    """Request daemon cancellation of this workflow and its active child processes.

    Cancellation is asynchronous; return current state, never manufacture a killed
    state. AiiDA propagates the kill to children and scheduler jobs.
    """
    node = _node(identifier)
    if node.is_terminated:
        return _summary(node)
    try:
        daemon_running = get_daemon_client().is_daemon_running
    except Exception as exception:
        raise RuntimeError("AiiDA could not confirm cancellation; inspect the current process state") from exception
    if not daemon_running:
        raise RuntimeError("AiiDA daemon is not running; cancellation cannot be delivered")
    try:
        kill_processes([node], msg_text="Cancelled through the QCL-NEGF service", timeout=10.0)
        refreshed = _node(identifier)
    except Exception as exception:
        raise RuntimeError("AiiDA could not confirm cancellation; inspect the current process state") from exception
    if not refreshed.is_terminated:
        raise RuntimeError("Cancellation is not confirmed; inspect the current process state")
    return _summary(refreshed)


def _retrieved(identifier: str, execution_id: str, *, attempt=None, calcjob_uuid=None) -> orm.FolderData:
    child = _select_child(_node(identifier), execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid)
    if "retrieved" not in child.outputs:
        raise LookupError("Retrieved execution artifacts are not available")
    return child.outputs.retrieved


def list_artifacts(identifier: str) -> list[dict[str, Any]]:
    """Read the size inventory generated once during parsing, without artifact I/O."""
    files = []
    for child in _children(_node(identifier)):
        if "inventory" not in child.outputs:
            continue
        inventory = child.outputs.inventory.get_dict()
        if not inventory["complete"] or len(files) + len(inventory["files"]) > MAX_ARTIFACTS:
            raise ValueError("Artifact listing exceeds the service limit; use AiiDA directly")
        files.extend({"execution_id": child.inputs.execution_id.value, "attempt": _attempt(child),
                      "calcjob_uuid": child.uuid, **item} for item in inventory["files"])
    return sorted(files, key=lambda item: (item["execution_id"], item["attempt"], item["path"]))


def _artifact_path(path: str) -> None:
    candidate = PurePosixPath(path)
    if not path or candidate.is_absolute() or ".." in candidate.parts or "\\" in path or str(candidate) != path:
        raise ValueError("Artifact path must be a normalized relative POSIX path")


def get_artifact_metadata(identifier: str, execution_id: str, path: str, *, attempt=None, calcjob_uuid=None) -> dict[str, Any]:
    """Resolve one child and read its cached inventory without repository I/O."""
    _artifact_path(path)
    if calcjob_uuid is not None:
        try:
            calcjob_uuid = str(UUID(calcjob_uuid))
        except (ValueError, TypeError, AttributeError) as exception:
            raise ValueError("A full CalcJob UUID is required") from exception
    child = _select_child(_node(identifier), execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid)
    if "retrieved" not in child.outputs or "inventory" not in child.outputs:
        raise LookupError("Retrieved artifact inventory is unavailable")
    inventory = child.outputs.inventory.get_dict()
    if not inventory["complete"] or len(inventory["files"]) > MAX_ARTIFACTS:
        raise ValueError("Artifact inventory exceeds the service limit or is incomplete")
    entries = [item for item in inventory["files"] if item["path"] == path]
    if len(entries) > 1:
        raise ValueError("Duplicate artifact inventory path")
    if not entries:
        raise LookupError("Artifact not found")
    return {"execution_id": execution_id, "attempt": _attempt(child),
            "calcjob_uuid": child.uuid, "path": path, "size": entries[0]["size"]}


@contextmanager
def open_artifact(identifier: str, execution_id: str, path: str, *, attempt=None, calcjob_uuid=None) -> Iterator[BinaryIO]:
    """Stream a file from the selected run's retrieved repository only."""
    _artifact_path(path)
    repository = _retrieved(identifier, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid).base.repository
    try:
        with repository.open(path, "rb") as handle:
            yield handle
    except (FileNotFoundError, IsADirectoryError, KeyError) as exception:
        raise LookupError("Artifact not found") from exception


# Native immutable research metadata; no workflow/plan mutation. These functions
# are called through the same loaded-profile owner as the execution readers.
from .research_cards import (  # noqa: F401
    attach_research_card_report,
    attach_research_card_run,
    create_research_card,
    edit_research_card,
    get_research_card,
    get_research_card_request,
    list_research_card_reports,
    list_research_card_runs,
    list_research_card_versions,
    list_research_cards,
    read_research_card_version,
)


# Saved-source reads are detached owner operations. Registration remains a
# separately admitted administrative call, with no research HTTP route.
def get_saved_sources(run_uuid, execution_id, *, attempt, calcjob_uuid):
    from .saved_sources import get_saved_sources as operation
    return operation(run_uuid, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid)


def get_saved_source(run_uuid, execution_id, *, attempt, calcjob_uuid,
                     source_uuid=None, expected_source_sha256=None):
    from .saved_sources import get_saved_source as operation
    return operation(run_uuid, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid,
                     source_uuid=source_uuid, expected_source_sha256=expected_source_sha256)


def get_performance_source(run_uuid, execution_id, *, attempt, calcjob_uuid):
    """Exact execution summary, never a cost attributed to borrowed point data."""
    from .performance_binding import retained_source
    child, _, _ = _export_child(run_uuid, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid)
    try:
        return retained_source(child)
    except (ValueError, TypeError, KeyError, AttributeError, OSError, ImportError):
        return {"status": "unavailable", "reason": "performance_binding_invalid_or_unavailable"}


def get_live_history_source(run_uuid, execution_id, *, attempt, calcjob_uuid,
                            point_id, source_attempt, namespace_id):
    from .live_sources import get_live_history_source as operation
    return operation(run_uuid, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid,
                     point_id=point_id, source_attempt=source_attempt, namespace_id=namespace_id)
