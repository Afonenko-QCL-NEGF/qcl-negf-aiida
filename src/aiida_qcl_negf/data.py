"""Lossless JSON provenance nodes.

AiiDA Dict attributes normalize floating-point values. Frozen scientific plans
and numerical results therefore belong in file-backed nodes, not ORM attributes.
"""

from __future__ import annotations

import io
import json
from typing import Any

from aiida import orm
from qcl_negf_contracts.messages import decode

from .validation import MAX_PLAN_BYTES, MAX_RESULT_BYTES, validate_plan


def read_json(node: orm.SinglefileData, limit: int = MAX_RESULT_BYTES) -> dict[str, Any]:
    with node.open(mode="rb") as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("JSON provenance node exceeds the size limit")
    return decode(raw, maximum=limit)


def plan_data(plan: dict[str, Any] | str | bytes) -> orm.SinglefileData:
    """Validate and store a plan without AiiDA attribute float normalization."""
    if isinstance(plan, dict):
        raw = json.dumps(plan, ensure_ascii=False, allow_nan=False).encode("utf-8")
    elif isinstance(plan, str):
        raw = plan.encode("utf-8")
    elif isinstance(plan, bytes):
        raw = plan
    else:
        raise ValueError("A plan must be a JSON object, string or bytes")
    if len(raw) > MAX_PLAN_BYTES:
        raise ValueError("Frozen plan exceeds the input size limit")
    validate_plan(decode(raw, maximum=MAX_PLAN_BYTES))
    return orm.SinglefileData(file=io.BytesIO(raw), filename="plan.json")


def read_plan(node: orm.SinglefileData) -> dict[str, Any]:
    return read_json(node, MAX_PLAN_BYTES)
