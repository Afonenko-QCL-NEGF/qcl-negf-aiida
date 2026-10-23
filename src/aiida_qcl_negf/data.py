"""Lossless JSON provenance nodes.

AiiDA Dict attributes normalize floating-point values. Frozen scientific plans
and numerical results therefore belong in file-backed nodes, not ORM attributes.
"""

from __future__ import annotations

import io
from typing import Any

from aiida import orm
from qcl_negf_contracts.messages import decode

from .validation import MAX_PLAN_BYTES, MAX_RESULT_BYTES


def read_json(node: orm.SinglefileData, limit: int = MAX_RESULT_BYTES) -> dict[str, Any]:
    with node.open(mode="rb") as handle:
        raw = handle.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("JSON provenance node exceeds the size limit")
    return decode(raw, maximum=limit)


def plan_data(plan: dict[str, Any] | str | bytes) -> orm.SinglefileData:
    """Validate and store a plan without AiiDA attribute float normalization."""
    from .validation import validated_plan_bytes
    raw, _, _ = validated_plan_bytes(plan)
    return orm.SinglefileData(file=io.BytesIO(raw), filename="plan.json")


def read_plan(node: orm.SinglefileData) -> dict[str, Any]:
    return read_json(node, MAX_PLAN_BYTES)
