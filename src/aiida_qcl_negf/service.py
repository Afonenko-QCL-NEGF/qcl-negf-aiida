"""Small application boundary over a loaded AiiDA profile.

    The caller owns authentication, permitted Code UUIDs, and request limits.
    This module never starts a second scheduler or opens arbitrary host paths.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import PurePosixPath
from typing import Any, BinaryIO
from uuid import UUID

from aiida import orm
from aiida.common.exceptions import NotExistent
from aiida.engine import submit
from aiida.engine.daemon.client import get_daemon_client
from aiida.engine.processes.control import kill_processes
from qcl_negf_contracts.messages import decode

from .data import plan_data, read_json, read_plan
from .validation import (
    MAX_PLAN_BYTES,
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
    return sorted((child for child in node.called if isinstance(child, orm.CalcJobNode)), key=lambda child: child.ctime)


def list_runs(limit: int = 50, offset: int = 0) -> list[dict[str, Any]]:
    """List only QCL-NEGF workflows, newest first."""
    if type(limit) is not int or not 1 <= limit <= 200 or type(offset) is not int or offset < 0:
        raise ValueError("Require 1 <= limit <= 200 and offset >= 0")
    query = orm.QueryBuilder().append(orm.WorkChainNode, filters={"process_type": PROCESS_TYPE}, tag="run")
    query.order_by({"run": {"ctime": "desc"}}).limit(limit).offset(offset)
    return [_summary(node) for node in query.all(flat=True)]


def get_run(identifier: str) -> dict[str, Any]:
    """Read the current workflow and scientific outputs, including partial results."""
    node = _node(identifier)
    response = _summary(node)
    response["plan_fingerprint"] = read_plan(node.inputs.plan)["fingerprint"]
    response["results"] = {}
    response["children"] = []
    remaining = MAX_RESPONSE_BYTES - len(json.dumps(response, ensure_ascii=False).encode("utf-8")) - 1024
    for child in _children(node):
        execution_id = child.inputs.execution_id.value
        summary = _summary(child)
        summary["execution_id"] = execution_id
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
            response["results"][execution_id] = result
    if len(json.dumps(response, ensure_ascii=False).encode("utf-8")) > MAX_RESPONSE_BYTES:
        raise ValueError("Run metadata exceeds the service response limit; use AiiDA directly")
    return response


def get_export_plan(identifier: str, execution_id: str) -> dict[str, Any]:
    """Read exact frozen bytes while the caller's AiiDA actor owns every handle.

    Prefer the retrieved worker snapshot. Only an absent repository entry permits
    fallback to the original file-backed workflow input; a corrupt, unreadable or
    different retrieved plan must fail the export rather than disappear silently.
    The returned value contains bytes and scalar provenance, never an ORM node.
    """
    node = _node(identifier)
    child = next((item for item in _children(node)
                  if item.inputs.execution_id.value == execution_id), None)
    if child is None or "retrieved" not in child.outputs:
        raise LookupError("Retrieved execution provenance is unavailable")

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


def submit_plan(plan: dict[str, Any] | str | bytes, code_uuid: str, resources: dict[str, Any], label: str = "", *, scratch_root: str | None = None) -> dict[str, Any]:
    """Submit a frozen plan to the daemon after validating every admission input."""
    plan_node = plan_data(plan)
    scheduler_options(resources)
    options = {}
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
    if not get_daemon_client().is_daemon_running:
        raise RuntimeError("AiiDA daemon is not running")
    node = submit(QCLPlanWorkChain, code=code, plan=plan_node, resources=orm.Dict(dict=resources), metadata={"label": label}, **options)
    return _summary(node)


def get_run_report(identifier: str) -> list[dict[str, Any]]:
    """Return workflow and child reports ordered by timestamp."""
    node = _node(identifier)
    processes = {process.pk: process for process in [node, *_children(node)]}
    logs = orm.Log.collection.find(filters={"dbnode_id": {"in": list(processes)}}, order_by=[{"time": "desc"}], limit=MAX_REPORTS)
    reports = [{"level": report.levelname, "message": report.message, "time": report.time.isoformat(), "process_uuid": processes[report.dbnode_id].uuid} for report in logs]
    return sorted(reports, key=lambda report: report["time"])


def kill_run(identifier: str) -> dict[str, Any]:
    """Request daemon cancellation of this workflow and its active child processes.

    Cancellation is asynchronous; return current state, never manufacture a killed
    state. AiiDA propagates the kill to children and scheduler jobs.
    """
    node = _node(identifier)
    if node.is_terminated:
        return _summary(node)
    if not get_daemon_client().is_daemon_running:
        raise RuntimeError("AiiDA daemon is not running; cancellation cannot be delivered")
    try:
        kill_processes([node], msg_text="Cancelled through the QCL-NEGF service", timeout=10.0)
    except Exception as exception:
        raise RuntimeError("AiiDA could not confirm cancellation; inspect the current process state") from exception
    refreshed = _node(identifier)
    if not refreshed.is_terminated:
        raise RuntimeError("Cancellation is not confirmed; inspect the current process state")
    return _summary(refreshed)


def _retrieved(identifier: str, execution_id: str) -> orm.FolderData:
    for child in _children(_node(identifier)):
        if child.inputs.execution_id.value == execution_id and "retrieved" in child.outputs:
            return child.outputs.retrieved
    raise LookupError("Retrieved execution artifacts are not available")


def list_artifacts(identifier: str) -> list[dict[str, Any]]:
    """Read the size inventory generated once during parsing, without artifact I/O."""
    files = []
    for child in _children(_node(identifier)):
        if "inventory" not in child.outputs:
            continue
        inventory = child.outputs.inventory.get_dict()
        if not inventory["complete"] or len(files) + len(inventory["files"]) > MAX_ARTIFACTS:
            raise ValueError("Artifact listing exceeds the service limit; use AiiDA directly")
        files.extend({"execution_id": child.inputs.execution_id.value, **item} for item in inventory["files"])
    return sorted(files, key=lambda item: (item["execution_id"], item["path"]))


@contextmanager
def open_artifact(identifier: str, execution_id: str, path: str) -> Iterator[BinaryIO]:
    """Stream a file from the selected run's retrieved repository only."""
    candidate = PurePosixPath(path)
    if not path or candidate.is_absolute() or ".." in candidate.parts or "\\" in path or str(candidate) != path:
        raise ValueError("Artifact path must be a normalized relative POSIX path")
    repository = _retrieved(identifier, execution_id).base.repository
    try:
        with repository.open(path, "rb") as handle:
            yield handle
    except (FileNotFoundError, IsADirectoryError, KeyError) as exception:
        raise LookupError("Artifact not found") from exception
