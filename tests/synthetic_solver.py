"""Test-only CLI contract simulator; it performs no physical calculation."""

import json
import hashlib
import sys
import shutil
from pathlib import Path

command, plan_path, output, *args = sys.argv[1:]
arguments = dict(zip(args[::2], args[1::2], strict=True))
identifier = arguments["--execution-id"]
attempt = int(arguments.get("--attempt", 1))
assert command == "run-plan"
plan = json.loads(Path(plan_path).read_text())
execution = next(item for item in plan["executions"] if item["id"] == identifier)
success = "fail" not in identifier
paused = identifier.startswith("always-pause") or (identifier.startswith("pause-once") and attempt == 1)
crashed = identifier == "crash-once" and attempt == 1
multi = identifier == "pause-once-multipoint"
directory = Path(output)
directory.mkdir()
prior = []
if attempt > 1:
    imported = Path(arguments["--recovery-bundle"])
    assert json.loads((imported / "receipt.json").read_text())["identity"]["attempt"] == attempt - 1
    progress = json.loads((imported / "execution_progress.json").read_text())
    if progress["completed_points"]:
        archive = Path(arguments["--archive-bundle"])
        for completed in progress["completed_points"]:
            prefix = Path("archive") / Path(completed["final_commit"]).parent
            for record in completed["files"]:
                source = archive / prefix / record["path"]
                raw = source.read_bytes()
                assert len(raw) == record["bytes"] and hashlib.sha256(raw).hexdigest() == record["sha256"]
                target = directory / prefix / record["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
            prior.append(completed["point"])
points = []
for point in plan["points"]:
    if point["execution_id"] != identifier:
        continue
    if point["id"] in {item["id"] for item in prior}:
        points.append(next(item for item in prior if item["id"] == point["id"]))
        continue
    point_paused = paused and (not multi or point["id"] == "point-2")
    points.append({
        "id": point["id"], "execution_id": identifier, "attempt": attempt,
        "coordinates": {key: point[key] for key in ("temperature_K", "voltage_per_period_V", "branch", "order")},
        "initialization": {"kind": "synthetic", "source_point_id": None, "checkpoint": None, "fallback_reason": None},
        "status": "paused" if point_paused else "completed", "quality": "strict" if success else "unconverged", "converged": success and not point_paused,
        "warnings": [], "observables": {"operator_checks_passed": success}, "data": {}, "postprocessing": {},
    })
completed_records = []
if multi:
    for point in points:
        if point["status"] != "completed":
            continue
        final = Path("archive") / identifier / point["id"] / "final"
        target = directory / final
        if point not in prior:
            target.mkdir(parents=True)
            identity = {"execution_id": identifier, "point_id": point["id"], "attempt": attempt, "plan_fingerprint": plan["fingerprint"]}
            artifacts = []
            for name, data in (("evidence.txt", f"Synthetic point {point['id']} attempt {attempt}".encode()),
                               ("physics.h5", b"synthetic full-state bytes"), ("history.h5", b"synthetic typed history"),
                               ("resolved_configuration.json", b"{}")):
                (target / name).write_bytes(data)
                artifacts.append({"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
            commit = {"schema": "qcl-negf.artifact-commit.v2", "identity": identity, "state_id": f"final-{point['id']}-{attempt}",
                      "state_sequence": 1, "storage_class": "archive", "checkpoint_ready": True, "artifacts": artifacts}
            raw = json.dumps(commit).encode()
            (target / "commit.json").write_bytes(raw)
            receipt = {"schema": "qcl-negf-recovery-receipt-v1", "status": "verified", "identity": identity,
                       "commit_sha256": hashlib.sha256(raw).hexdigest(), "state_id": commit["state_id"], "state_sequence": 1,
                       "publication_scope": "local_filesystem"}
            (target / "receipt.json").write_text(json.dumps(receipt))
            point["data"]["result_commit"] = str(final / "commit.json")
        receipt = json.loads((target / "receipt.json").read_text())
        files = [{"path": file.name, "bytes": file.stat().st_size, "sha256": hashlib.sha256(file.read_bytes()).hexdigest()} for file in sorted(target.iterdir())]
        completed_records.append({"point": point, "final_commit": f"{identifier}/{point['id']}/final/commit.json", "receipt": receipt, "files": files})
result = {
    "schema": "qcl-negf-series-result-v3", "contract_set": "qcl-negf.results.v1",
    "plan_fingerprint": plan["fingerprint"], "plan_scientific_fingerprint": plan["scientific_fingerprint"],
    "root_definition_id": plan["root_definition_id"], "name": plan["name"],
    "status": "paused" if paused else "completed", "model_version": "0.2.0", "inclusions": plan["inclusions"],
    "executions": [{key: execution[key] for key in ("id", "definition_id", "variant_id", "method_id", "purpose", "label", "operation", "point_ids", "repetition")}],
    "expected_point_count": len(points), "available_point_count": len(points), "points": points,
    "selected_execution_id": identifier,
}
if identifier == "wrong-conditions":
    result["points"][0]["coordinates"]["temperature_K"] = "wrong-temperature"
if paused or crashed:
    active = points[-1] if multi else points[0]
    prefix = Path("recovery") / identifier / active["id"] / f"generation-{attempt:06d}"
    target = directory / prefix
    target.mkdir(parents=True)
    identity = {"execution_id": identifier, "point_id": active["id"], "attempt": attempt, "plan_fingerprint": plan["fingerprint"]}
    artifacts = []
    for name in ("physics.h5", "history.h5", "recovery.json", "resolved_configuration.json"):
        data = f"Synthetic transport {name}, cumulative iteration {attempt}".encode()
        (target / name).write_bytes(data)
        artifacts.append({"path": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
    progress = {"schema": "qcl-negf-execution-progress-v1", "contract_set": "qcl-negf.results.v1", "identity": identity,
                "plan_fingerprint": plan["fingerprint"], "execution_id": identifier, "active_point_id": active["id"],
                "completed_points": completed_records}
    raw_progress = json.dumps(progress).encode()
    (target / "execution_progress.json").write_bytes(raw_progress)
    artifacts.append({"path": "execution_progress.json", "bytes": len(raw_progress), "sha256": hashlib.sha256(raw_progress).hexdigest()})
    commit = {"schema": "qcl-negf.artifact-commit.v2", "identity": identity, "state_id": f"state-{attempt}",
              "state_sequence": attempt, "artifacts": artifacts, "checkpoint_ready": True, "storage_class": "recovery",
              "restart_coordinates": {"last_inner": attempt, "maximum_scba_iterations": 3}}
    raw = json.dumps(commit).encode()
    (target / "commit.json").write_bytes(raw)
    receipt = {"schema": "qcl-negf-recovery-receipt-v1", "status": "verified", "identity": identity,
               "state_id": commit["state_id"], "state_sequence": attempt, "commit_sha256": hashlib.sha256(raw).hexdigest(),
               "publication_scope": "local_filesystem"}
    (target / "receipt.json").write_text(json.dumps(receipt))
    if paused:
        pause = {"schema": "qcl-negf-pause-receipt-v1", "status": "paused", "execution_id": identifier,
                 "attempt": attempt, "commit_path": str(prefix / "commit.json"),
                 "commit_sha256": receipt["commit_sha256"], "state_id": commit["state_id"], "state_sequence": attempt}
        (directory / "pause-receipt.json").write_text(json.dumps(pause))
elif not multi:
    target = directory / "archive" / identifier / points[0]["id"] / "final"
    target.mkdir(parents=True)
    (target / "evidence.txt").write_text("Synthetic transport evidence\n")
if not crashed:
    (directory / "series_result.json").write_text(json.dumps(result))
(directory / "scientific_plan.json").write_text(json.dumps(plan))
# Abandoned full-state directories must not enter AiiDA retrieval.
(directory / "abandoned").mkdir()
(directory / "abandoned/physics.h5").write_text("not selected")
print(json.dumps(result))
if crashed:
    sys.exit(1)
