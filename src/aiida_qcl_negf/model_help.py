"""Bounded C1 metadata adapter. Availability is never an origin/science verdict."""
from __future__ import annotations

import hashlib
import re
from pathlib import PurePosixPath
from collections.abc import Callable

from jsonschema import ValidationError
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.messages import decode, encode

HELP_MAX_BYTES = 65536
HELP_TOTAL_BYTES = 8388608
HELP_INDEX_BYTES = 2097152
_ID = r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}"
_HELP = re.compile(rf"result/executions/({_ID})/({_ID})/attempt-([1-9][0-9]*)/model-help-inputs\.json")


class _HelpSnapshotError(ValueError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def _help_path(path: str) -> tuple[str, str, int] | None:
    if not isinstance(path, str):
        return None
    match = _HELP.fullmatch(path)
    if not match:
        return None
    try:
        return match[1], match[2], int(match[3])
    except ValueError:
        return None


def _help_context(plan: dict, execution_id: str, point_id: str, attempt: int) -> dict:
    if (not isinstance(execution_id, str) or re.fullmatch(_ID, execution_id) is None
            or not isinstance(point_id, str) or re.fullmatch(_ID, point_id) is None
            or type(attempt) is not int or attempt < 1):
        raise ValueError("Invalid model-help identity")
    executions = [row for row in plan["executions"] if row["id"] == execution_id]
    points = [row for row in plan["points"] if row["id"] == point_id]
    if (len(executions) != 1 or len(points) != 1 or points[0]["execution_id"] != execution_id
            or executions[0]["point_ids"].count(point_id) != 1):
        raise ValueError("Model-help point is absent or ambiguous in the frozen execution")
    effective = points[0].get("effective_inputs", {})
    if not isinstance(effective, dict):
        raise ValueError("Invalid retained point passport")
    configuration_hash = effective.get("configuration_hash")
    if "configuration_hash" in effective and (not isinstance(configuration_hash, str)
            or re.fullmatch(r"[a-f0-9]{64}", configuration_hash) is None):
        raise ValueError("Invalid retained configuration hash")
    return {"plan_fingerprint": plan["fingerprint"],
            "scientific_fingerprint": plan["scientific_fingerprint"],
            "model_revision": plan["model_revision"], "execution_id": execution_id,
            "point_id": point_id, "attempt": attempt, "configuration_hash": configuration_hash}


def _validate_help_snapshot(raw: bytes, *, plan: dict, execution_id: str,
                            point_id: str, attempt: int) -> dict:
    try:
        document = decode(raw, maximum=HELP_MAX_BYTES)
        version = document.get("format")
        if not isinstance(version, str) or not version:
            raise _HelpSnapshotError("malformed")
        if version != "qcl-negf.model-help-inputs.v1":
            raise _HelpSnapshotError("unsupported_version")
        schema_validator("model-help.schema.json").validate(document)
        paths = [row["path"] for row in document["inputs"]]
        claims = [(row["reference_id"], row["phase"]) for row in document["origin_bindings"]]
        if len(paths) != len(set(paths)) or len(claims) != len(set(claims)):
            raise _HelpSnapshotError("malformed")
    except _HelpSnapshotError:
        raise
    except (ValueError, TypeError, KeyError, UnicodeError, ValidationError, RecursionError) as exc:
        raise _HelpSnapshotError("malformed") from exc
    try:
        expected = _help_context(plan, execution_id, point_id, attempt)
        if document["context"] != expected:
            raise ValueError("Snapshot local identity differs")
    except (ValueError, TypeError, KeyError) as exc:
        raise _HelpSnapshotError("identity_mismatch") from exc
    return document


def _inventory_rows(inventory: dict) -> list[dict]:
    if (not isinstance(inventory, dict) or set(inventory) != {"complete", "files"}
            or inventory["complete"] is not True or not isinstance(inventory["files"], list)
            or len(inventory["files"]) > 10000):
        raise ValueError("Incomplete artifact inventory")
    seen = set()
    for row in inventory["files"]:
        if not isinstance(row, dict) or set(row) != {"path", "size"}:
            raise ValueError("Invalid artifact row")
        path, size = row["path"], row["size"]
        if (not isinstance(path, str) or not path
                or any(ord(char) < 32 or ord(char) == 127 for char in path)
                or "\\" in path or PurePosixPath(path).is_absolute()
                or ".." in PurePosixPath(path).parts or str(PurePosixPath(path)) != path
                or path in seen or type(size) is not int or size < 0):
            raise ValueError("Invalid artifact path/size")
        seen.add(path)
    return inventory["files"]


def _help_candidates(inventory: dict, plan: dict) -> list[dict]:
    rows = sorted((row for row in _inventory_rows(inventory) if _help_path(row["path"])),
                  key=lambda row: row["path"])
    if (len(rows) > min(len(plan["points"]), 10000)
            or any(row["size"] > HELP_MAX_BYTES for row in rows)
            or sum(row["size"] for row in rows) > HELP_TOTAL_BYTES):
        raise ValueError("Model-help inventory exceeds its finite bounds")
    return rows


def _build_help_index(*, inventory: dict, plan: dict, execution_id: str,
                      attempt: int, read: Callable[[str, int], bytes]) -> dict:
    incomplete = {"format": "qcl-negf.model-help-index.v1", "complete": False, "entries": []}
    try:
        rows = _help_candidates(inventory, plan)  # All caps before the first payload read.
        entries = []
        for row in rows:
            path = row["path"]
            identity = _help_path(path)
            raw = read(path, row["size"])
            if type(raw) is not bytes or len(raw) != row["size"]:
                return incomplete
            entry = {"path": path, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                     "execution_id": identity[0], "point_id": identity[1], "attempt": identity[2],
                     "status": "valid", "reason": "ok"}
            try:
                _validate_help_snapshot(raw, plan=plan, execution_id=identity[0],
                                        point_id=identity[1], attempt=identity[2])
                if (identity[0], identity[2]) != (execution_id, attempt):
                    raise _HelpSnapshotError("identity_mismatch")
            except _HelpSnapshotError as exc:
                entry.update(status="invalid", reason=exc.reason)
            entries.append(entry)
        result = {"format": "qcl-negf.model-help-index.v1", "complete": True, "entries": entries}
        if len(encode(result)) > HELP_INDEX_BYTES:
            return incomplete
        schema_validator("model-help.schema.json").validate(result)
        return result
    except (ValueError, TypeError, KeyError, OSError, ValidationError, RecursionError):
        return incomplete
