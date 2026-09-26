"""Validate the supported workflow without modifying its scientific identity."""

from __future__ import annotations

import re
import json
from pathlib import PurePosixPath
from typing import Any

from jsonschema import ValidationError
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.messages import scientific_plan

IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
MAX_PLAN_BYTES = 16 * 1024 * 1024
MAX_RESULT_BYTES = 32 * 1024 * 1024


def validate_scratch_root(value: str) -> str:
    """Validate a trusted worker-side path without inspecting the controller filesystem."""
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise ValueError("scratch_root must be an absolute worker directory")
    path = PurePosixPath(value)
    if (not path.is_absolute() or value == "/" or str(path) != value or
            ".." in path.parts or "\\" in value or any(ord(char) < 32 for char in value)):
        raise ValueError("scratch_root must be a normalized absolute worker directory")
    return value


def validate_plan(value: dict[str, Any]) -> dict[str, Any]:
    """Accept a frozen ordinary plan of independent executions.

    Julia validates the canonical scientific fingerprints before any calculation.
    This validation catches schema and workflow errors before daemon submission.
    """
    if len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")) > MAX_PLAN_BYTES:
        raise ValueError("Frozen plan exceeds the input size limit")
    scientific_plan(value)
    try:
        schema_validator("scientific-plan.schema.json").validate(value)
    except ValidationError as exception:
        path = ".".join(str(part) for part in exception.absolute_path)
        raise ValueError(f"Invalid scientific plan at {path or '$'}: {exception.message[:300]}") from exception
    if value.get("campaign") is not None:
        raise ValueError("Campaign admission policies are not supported; submit an ordinary plan")
    executions = {execution["id"]: execution for execution in value["executions"]}
    if not executions:
        raise ValueError("A plan must contain at least one execution")
    if any(not IDENTIFIER.fullmatch(identifier) for identifier in executions):
        raise ValueError("Execution identifiers must be safe path components")
    nodes = value.get("nodes", [])
    if {node["execution_id"] for node in nodes} != set(executions) or len(nodes) != len(executions):
        raise ValueError("Every execution must have exactly one execution node")
    for node in nodes:
        if node.get("depends_on") or node.get("required_evidence") or node.get("forbidden_evidence") or node.get("reserve"):
            raise ValueError("Only independent ordinary executions are supported")
    point_ids = {point["id"] for point in value["points"]}
    previous: dict[str, dict[str, Any]] = {}
    for point in value["points"]:
        execution_id = point["execution_id"]
        if execution_id not in executions:
            raise ValueError("Point references an unknown execution")
        predecessor = point.get("predecessor_id")
        if predecessor is not None:
            parent = previous.get(predecessor)
            if parent is None or any(parent[key] != point[key] for key in ("execution_id", "branch", "temperature_K")):
                raise ValueError("Continuation must follow an earlier point in the same execution, branch and temperature")
        previous[point["id"]] = point
    for identifier, execution in executions.items():
        membership = {point["id"] for point in value["points"] if point["execution_id"] == identifier}
        if set(execution["point_ids"]) != membership or not membership <= point_ids:
            raise ValueError("Execution point membership disagrees with plan points")
    return value


def scheduler_options(resources: dict[str, Any]) -> dict[str, Any]:
    """Translate the public resource request to AiiDA single-process job options.

    Independent executions scale across nodes. The solver itself is threaded and
    cannot use MPI or a multi-node allocation.
    """
    allowed = {"num_machines", "num_mpiprocs_per_machine", "num_cores_per_mpiproc", "max_wallclock_seconds", "max_memory_kb", "queue_name", "account"}
    if unknown := resources.keys() - allowed:
        raise ValueError(f"Unsupported resource fields: {', '.join(sorted(unknown))}")
    if any(type(resources.get(field, 1)) is not int or resources.get(field, 1) != 1
           for field in ("num_machines", "num_mpiprocs_per_machine")):
        raise ValueError("Each execution requires one node and one threaded process")
    integer_fields = {"num_cores_per_mpiproc": 1, "max_wallclock_seconds": 3600}
    values: dict[str, Any] = {}
    for field, default in integer_fields.items():
        number = resources.get(field, default)
        if type(number) is not int or number < 1:
            raise ValueError(f"{field} must be a positive integer")
        values[field] = number
    options: dict[str, Any] = {
        "resources": {"num_machines": 1, "num_mpiprocs_per_machine": 1, "num_cores_per_mpiproc": values["num_cores_per_mpiproc"]},
        "max_wallclock_seconds": values["max_wallclock_seconds"],
        "withmpi": False,
        "environment_variables": {"JULIA_NUM_THREADS": str(values["num_cores_per_mpiproc"]), "OPENBLAS_NUM_THREADS": "1"},
    }
    if "max_memory_kb" in resources:
        if type(resources["max_memory_kb"]) is not int or resources["max_memory_kb"] < 1:
            raise ValueError("max_memory_kb must be a positive integer")
        options["max_memory_kb"] = resources["max_memory_kb"]
    for field in ("queue_name", "account"):
        if field in resources:
            if not isinstance(resources[field], str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", resources[field]):
                raise ValueError(f"Invalid {field}")
            options[field] = resources[field]
    return options
