"""Pure, allowlisted presentation of a retained plan; never scientific admission."""
from __future__ import annotations

from math import isfinite


def _text(value):
    if type(value) is not str or not value:
        raise ValueError("Expected nonempty text")
    return value


def _refs(value):
    if type(value) is not list:
        raise ValueError("Expected references")
    result = [_text(item) for item in value]
    if len(set(result)) != len(result):
        raise ValueError("Duplicate reference")
    return result


def _rows(value, fields):
    if type(value) is not list:
        raise ValueError("Expected rows")
    rows = [{key: row[key] for key in fields} for row in value if type(row) is dict]
    if len(rows) != len(value):
        raise ValueError("Expected objects")
    ids = [_text(row["id"]) for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("Duplicate identity")
    return rows


def project_run_structure(plan: dict) -> dict:
    """Project exact plan-v2 scalars, or report unavailable metadata.

    Inclusion membership is many-to-many. Only explicit predecessors form edges;
    presentation checks do not call the plan validator or reconstruct defaults.
    """
    if type(plan) is not dict or plan.get("schema") != "qcl-negf-scientific-plan-v2":
        return {"status": "unavailable", "reason": "unsupported_plan_schema"}
    try:
        root = {"kind": _text(plan["root_kind"]), "definition_id": _text(plan["root_definition_id"])}
        if root["kind"] not in ("study", "meta"):
            raise ValueError("Unknown root kind")
        inclusions = _rows(plan["inclusions"], ("id", "parent_id", "definition_id", "label", "path", "execution_ids"))
        executions = _rows(plan["executions"], ("id", "definition_id", "variant_id", "label", "operation", "point_ids"))
        points = _rows(plan["points"], ("id", "execution_id", "branch", "order", "initialization", "predecessor_id", "temperature_K", "voltage_per_period_V"))
        inclusion_by_id = {row["id"]: row for row in inclusions}
        execution_by_id = {row["id"]: row for row in executions}
        covered = set()
        for row in inclusions:
            _text(row["definition_id"])
            if type(row["label"]) is not str:
                raise ValueError("Invalid label")
            # Paths are ordered definition references; repeated definitions are valid.
            if type(row["path"]) is not list or not row["path"]:
                raise ValueError("Invalid inclusion path")
            row["path"] = [_text(item) for item in row["path"]]
            row["execution_ids"] = _refs(row["execution_ids"])
            if not set(row["execution_ids"]) <= execution_by_id.keys():
                raise ValueError("Dangling execution")
            covered.update(row["execution_ids"])
        if covered != execution_by_id.keys():
            raise ValueError("Missing inclusion membership")
        # Iterative coloring keeps deep valid inclusion trees independent of recursion limits.
        finished = set()
        for row in inclusions:
            chain = set()
            current = row["id"]
            while current is not None and current not in finished:
                if type(current) is not str or current not in inclusion_by_id or current in chain:
                    raise ValueError("Dangling or cyclic inclusion")
                chain.add(current)
                current = inclusion_by_id[current]["parent_id"]
            finished.update(chain)
        for source, row in zip(plan["executions"], executions):
            for key in ("definition_id", "variant_id", "operation"):
                _text(row[key])
            if type(row["label"]) is not str:
                raise ValueError("Invalid label")
            row["point_ids"] = _refs(row["point_ids"])
            row["voltage_mode"] = _text(source["policies"]["voltage"]["mode"])
        previous = {}
        membership = {identifier: set() for identifier in execution_by_id}
        for point in points:
            for key in ("execution_id", "branch", "initialization"):
                _text(point[key])
            if point["execution_id"] not in execution_by_id:
                raise ValueError("Dangling point execution")
            if type(point["order"]) is not int:
                raise ValueError("Invalid point order")
            for key in ("temperature_K", "voltage_per_period_V"):
                if type(point[key]) not in (int, float) or not isfinite(point[key]):
                    raise ValueError("Invalid coordinate")
            predecessor = point["predecessor_id"]
            if predecessor is not None:
                _text(predecessor)
                parent = previous.get(predecessor)
                if parent is None or any(parent[key] != point[key] for key in ("execution_id", "branch", "temperature_K")):
                    raise ValueError("Foreign or non-earlier predecessor")
            previous[point["id"]] = point
            membership[point["execution_id"]].add(point["id"])
        for row in executions:
            if set(row["point_ids"]) != membership[row["id"]]:
                raise ValueError("Contradictory point membership")
        return {"status": "available", "root": root, "inclusions": inclusions, "executions": executions, "points": points}
    except (KeyError, TypeError, ValueError, OverflowError):
        return {"status": "unavailable", "reason": "unsupported_structure"}
