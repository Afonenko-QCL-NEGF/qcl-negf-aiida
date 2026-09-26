"""Test-only CLI contract simulator; it performs no physical calculation."""

import json
import sys
from pathlib import Path

command, plan_path, output, flag, identifier = sys.argv[1:]
assert command == "run-plan" and flag == "--execution-id"
plan = json.loads(Path(plan_path).read_text())
execution = next(item for item in plan["executions"] if item["id"] == identifier)
success = "fail" not in identifier
points = []
for point in plan["points"]:
    if point["execution_id"] != identifier:
        continue
    points.append({
        "id": point["id"], "execution_id": identifier, "attempt": 1,
        "coordinates": {key: point[key] for key in ("temperature_K", "voltage_per_period_V", "branch", "order")},
        "initialization": {"kind": "synthetic", "source_point_id": None, "checkpoint": None, "fallback_reason": None},
        "status": "completed", "quality": "strict" if success else "unconverged", "converged": success,
        "warnings": [], "observables": {"operator_checks_passed": success}, "data": {}, "postprocessing": {},
    })
result = {
    "schema": "qcl-negf-series-result-v3", "contract_set": "qcl-negf.results.v1",
    "plan_fingerprint": plan["fingerprint"], "plan_scientific_fingerprint": plan["scientific_fingerprint"],
    "root_definition_id": plan["root_definition_id"], "name": plan["name"],
    "status": "completed", "model_version": "0.2.0", "inclusions": plan["inclusions"],
    "executions": [{key: execution[key] for key in ("id", "definition_id", "variant_id", "method_id", "purpose", "label", "operation", "point_ids", "repetition")}],
    "expected_point_count": len(points), "available_point_count": len(points), "points": points,
    "selected_execution_id": identifier,
}
if identifier == "wrong-conditions":
    result["points"][0]["coordinates"]["temperature_K"] = "wrong-temperature"
directory = Path(output)
directory.mkdir()
(directory / "series_result.json").write_text(json.dumps(result))
(directory / "nested").mkdir()
(directory / "nested/evidence.txt").write_text("Synthetic transport evidence\n")
print(json.dumps(result))
