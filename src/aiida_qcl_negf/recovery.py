"""Verify portable recovery closures before returning them as AiiDA provenance."""

from __future__ import annotations

import hashlib
from pathlib import PurePosixPath

from qcl_negf_contracts.messages import decode
from qcl_negf_contracts.artifacts import validate_execution_progress

MAX_BUNDLE_BYTES = 8 * 1024**3
MAX_ARCHIVE_BYTES = 64 * 1024**3
MAX_METADATA_BYTES = 16 * 1024**2


def relative_path(value):
    if not isinstance(value, str) or not value:
        raise ValueError("Recovery reference must be a relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or str(path) != value or "\\" in value:
        raise ValueError("Recovery reference escapes its immutable bundle")
    return value


def read_metadata(repository, path):
    with repository.open(path, "rb") as stream:
        raw = stream.read(MAX_METADATA_BYTES + 1)
    if len(raw) > MAX_METADATA_BYTES:
        raise ValueError("Recovery metadata exceeds the size limit")
    return decode(raw, maximum=MAX_METADATA_BYTES), raw


def verify_bundle(repository, prefix, *, execution_id, attempt, plan_fingerprint):
    prefix = relative_path(prefix) if prefix else ""
    def path(name):
        return f"{prefix}/{name}" if prefix else name
    receipt, receipt_raw = read_metadata(repository, path("receipt.json"))
    commit, raw = read_metadata(repository, path("commit.json"))
    if (receipt.get("schema") != "qcl-negf-recovery-receipt-v1"
            or receipt.get("status") != "verified"
            or receipt.get("publication_scope") != "local_filesystem"
            or commit.get("checkpoint_ready") is not True
            or commit.get("storage_class") != "recovery"):
        raise ValueError("Recovery has no verified publication receipt")
    identity = commit.get("identity", {})
    if (identity.get("execution_id") != execution_id or identity.get("attempt") != attempt
            or identity.get("plan_fingerprint") != plan_fingerprint or receipt.get("identity") != identity):
        raise ValueError("Recovery identity conflicts with the selected attempt")
    digest = hashlib.sha256(raw).hexdigest()
    if receipt.get("commit_sha256") != digest:
        raise ValueError("Recovery commit digest differs")
    for key in ("state_id", "state_sequence"):
        if key not in commit or receipt.get(key) != commit[key]:
            raise ValueError("Recovery state receipt differs from its commit")
    if type(commit["state_sequence"]) is not int or commit["state_sequence"] < 1:
        raise ValueError("Recovery state sequence must be positive")
    names, total = set(), len(raw) + len(receipt_raw)
    paths = ["commit.json", "receipt.json"]
    for artifact in commit.get("artifacts", []):
        name = relative_path(artifact["path"])
        if name in names or name in paths:
            raise ValueError("Duplicate recovery artifact")
        names.add(name)
        size = artifact["bytes"]
        if type(size) is not int or size < 0 or total + size > MAX_BUNDLE_BYTES:
            raise ValueError("Recovery bundle exceeds its byte budget")
        total += size
        actual_size, checksum = 0, hashlib.sha256()
        with repository.open(path(name), "rb") as stream:
            while block := stream.read(1024**2):
                actual_size += len(block)
                if actual_size > size:
                    raise ValueError("Recovery artifact byte count differs")
                checksum.update(block)
        if actual_size != size or checksum.hexdigest() != artifact["sha256"]:
            raise ValueError("Recovery artifact digest or byte count differs")
        paths.append(name)
    if not {"physics.h5", "history.h5", "recovery.json", "resolved_configuration.json"} <= names:
        raise ValueError("Recovery bundle lacks required state dependencies")
    for artifact in commit["artifacts"]:
        if not set(artifact.get("dependencies", [])) <= names:
            raise ValueError("Recovery dependency is outside its portable closure")
    progress = None
    if "execution_progress.json" in names:
        progress, _ = read_metadata(repository, path("execution_progress.json"))
        validate_execution_progress(progress)
        if (progress.get("schema") != "qcl-negf-execution-progress-v1"
                or progress.get("identity") != identity or progress.get("execution_id") != execution_id
                or progress.get("plan_fingerprint") != plan_fingerprint
                or progress.get("active_point_id") != identity.get("point_id")
                or not isinstance(progress.get("completed_points"), list)):
            raise ValueError("Execution progress differs from its recovery identity")
    return {"prefix": prefix, "files": paths, "commit_sha256": digest, "identity": identity,
            "state_id": commit["state_id"], "state_sequence": commit["state_sequence"],
            "restart_coordinates": commit.get("restart_coordinates", {}), "bytes": total, "progress": progress}


def verify_prior_archive(repository, progress, *, byte_budget=MAX_ARCHIVE_BYTES, prefix="", plan_points=None):
    """Check and select the completed finals needed by one portable execution."""
    if type(byte_budget) is not int or byte_budget < 1:
        raise ValueError("Archive transfer byte budget must be positive")
    if progress is None:
        raise ValueError("Operational recovery lacks immutable execution progress")
    identity = progress["identity"]
    active = identity["point_id"]
    paths, point_ids, total = [], set(), 0
    for completed in progress["completed_points"]:
        point = completed["point"]
        if (point["id"] in point_ids or point["id"] == active
                or point["execution_id"] != identity["execution_id"]
                or type(point["attempt"]) is not int or not 1 <= point["attempt"] <= identity["attempt"]
                or point["status"] not in ("completed", "completed_with_warnings")):
            raise ValueError("Prior final is not a unique completed point of this execution")
        point_ids.add(point["id"])
        if plan_points is not None:
            expected = plan_points[point["id"]]
            if (expected["execution_id"] != identity["execution_id"]
                    or expected["order"] >= plan_points[active]["order"]
                    or any(point["coordinates"][key] != expected[key]
                           for key in ("temperature_K", "voltage_per_period_V", "branch", "order"))):
                raise ValueError("Prior final coordinates disagree with the frozen plan")
        reference = relative_path(completed["final_commit"])
        expected_reference = f"{identity['execution_id']}/{point['id']}/final/commit.json"
        if reference != expected_reference or point.get("data", {}).get("result_commit") != "archive/" + reference:
            raise ValueError("Prior final commit is outside its canonical archive")
        directory = f"archive/{reference.removesuffix('/commit.json')}"
        def repository_path(name):
            return f"{prefix}/{directory}/{name}" if prefix else f"{directory}/{name}"
        if repository is None:
            raise ValueError("Completed points require an explicit prior archive bundle")
        commit, raw = read_metadata(repository, repository_path("commit.json"))
        receipt, _ = read_metadata(repository, repository_path("receipt.json"))
        expected_identity = {"point_id": point["id"], "execution_id": identity["execution_id"],
                             "attempt": point["attempt"], "plan_fingerprint": identity["plan_fingerprint"]}
        if (commit.get("storage_class") != "archive"
                or any(commit.get("identity", {}).get(key) != value for key, value in expected_identity.items())
                or receipt.get("status") != "verified" or receipt.get("identity") != commit["identity"]
                or receipt.get("commit_sha256") != hashlib.sha256(raw).hexdigest()
                or any(receipt.get(key) != commit.get(key) for key in ("state_id", "state_sequence"))
                or any(completed["receipt"].get(key) != receipt.get(key)
                       for key in ("identity", "state_id", "state_sequence", "commit_sha256"))):
            raise ValueError("Prior final receipt conflicts with its immutable commit")
        records = completed["files"]
        declared = {relative_path(item["path"]): item for item in records}
        artifacts = {relative_path(item["path"]): item for item in commit["artifacts"]}
        if (len(declared) != len(records) or len(artifacts) != len(commit["artifacts"])
                or not {"commit.json", "receipt.json"} <= declared.keys()
                or not artifacts.keys() <= declared.keys()
                or declared.keys() - artifacts.keys() - {"commit.json", "receipt.json", "optical.h5"}
                or not {"physics.h5", "history.h5", "resolved_configuration.json"} <= artifacts.keys()):
            raise ValueError("Prior archive transfer lacks the full declared closure")
        for name, artifact in artifacts.items():
            if any(artifact[key] != declared[name][key] for key in ("bytes", "sha256")):
                raise ValueError("Prior archive transfer disagrees with artifact digest")
            if not set(artifact.get("dependencies", [])) <= artifacts.keys():
                raise ValueError("Prior final dependency is outside its archive closure")
        for name, record in declared.items():
            size = record["bytes"]
            if type(size) is not int or size < 0 or total + size > byte_budget:
                raise ValueError("Prior archive exceeds its transfer byte budget")
            total += size
            checksum, count = hashlib.sha256(), 0
            with repository.open(repository_path(name), "rb") as stream:
                while block := stream.read(1024**2):
                    count += len(block)
                    if count > size:
                        raise ValueError("Prior archive artifact byte count differs")
                    checksum.update(block)
            if count != size or checksum.hexdigest() != record["sha256"]:
                raise ValueError("Prior archive artifact digest differs")
            paths.append(f"{directory}/{name}")
    return {"files": paths, "bytes": total, "point_ids": sorted(point_ids)}


def prior_archive_data(repository, selected, *, prefix="result"):
    """Explicit portable copy; all original CalcJob artifacts remain provenance."""
    from aiida import orm
    bundle = orm.FolderData()
    for name in selected["files"]:
        with repository.open(f"{prefix}/{name}" if prefix else name, "rb") as stream:
            bundle.base.repository.put_object_from_filelike(stream, name)
    return bundle


def select_recovery(repository, files, *, execution_id, attempt, plan_fingerprint, point_order=None,
                    plan_points=None, archive_byte_budget=MAX_ARCHIVE_BYTES):
    candidates, rejected = [], []
    for name in sorted(files):
        if not name.startswith("result/recovery/") or not name.endswith("/receipt.json"):
            continue
        prefix = name.removesuffix("/receipt.json")
        try:
            candidate = verify_bundle(repository, prefix, execution_id=execution_id,
                attempt=attempt, plan_fingerprint=plan_fingerprint)
            if plan_points is not None:
                candidate["prior_archive"] = verify_prior_archive(repository, candidate["progress"],
                    prefix="result", plan_points=plan_points, byte_budget=archive_byte_budget)
            candidates.append(candidate)
        except (OSError, KeyError, ValueError) as exception:
            rejected.append(f"{prefix}: {exception}")
    if not candidates:
        return None, rejected
    def coordinate(candidate):
        point = candidate["identity"].get("point_id")
        return (point_order or {}).get(point, 0), candidate["state_sequence"]
    candidates.sort(key=coordinate)
    if len(candidates) > 1 and coordinate(candidates[-1]) == coordinate(candidates[-2]):
        raise ValueError("Recovery selection is ambiguous across states")
    return candidates[-1], rejected


def verify_pause_receipt(repository, pause, *, execution_id, attempt, plan_fingerprint):
    if (pause.get("schema") != "qcl-negf-pause-receipt-v1" or pause.get("status") != "paused"
            or pause.get("execution_id") != execution_id or pause.get("attempt") != attempt):
        raise ValueError("Pause receipt differs from the selected attempt")
    path = relative_path(pause["commit_path"])
    if not path.startswith("recovery/") or not path.endswith("/commit.json"):
        raise ValueError("Pause receipt must identify a recovery commit")
    selected = verify_bundle(repository, f"result/{path.removesuffix('/commit.json')}",
        execution_id=execution_id, attempt=attempt, plan_fingerprint=plan_fingerprint)
    if any(pause.get(key) != selected[key] for key in ("commit_sha256", "state_id", "state_sequence")):
        raise ValueError("Pause receipt refers to another state")
    return selected


def recovery_data(repository, selected):
    """Store exactly one verified generation, without abandoned worker files."""
    from aiida import orm
    bundle = orm.FolderData()
    for name in selected["files"]:
        with repository.open(f"{selected['prefix']}/{name}", "rb") as handle:
            bundle.base.repository.put_object_from_filelike(handle, name)
    return bundle


def pin_result_commits(repository, points, *, plan_fingerprint, require_receipt=False):
    """Bind each compact row to its exact immutable state, including prior finals."""
    selected = {}
    for point in points:
        reference = point.get("data", {}).get("result_commit")
        if reference is None:
            if require_receipt and point.get("status") in ("completed", "completed_with_warnings"):
                raise ValueError("Stationary final lacks its committed full-state reference")
            continue
        path = "result/" + relative_path(reference)
        commit, raw = read_metadata(repository, path)
        identity = commit.get("identity", {})
        expected = {"point_id": point["id"], "execution_id": point["execution_id"],
                    "attempt": point["attempt"], "plan_fingerprint": plan_fingerprint}
        if any(identity.get(key) != value for key, value in expected.items()):
            raise ValueError("Result commit identity disagrees with the exact point attempt")
        prefix = path.removesuffix("/commit.json")
        digest = hashlib.sha256(raw).hexdigest()
        receipt = None
        try:
            receipt, _ = read_metadata(repository, f"{prefix}/receipt.json")
        except (OSError, KeyError):
            if require_receipt:
                raise ValueError("Published final has no verified state receipt")
        if receipt is not None:
            if (receipt.get("status") != "verified" or receipt.get("commit_sha256") != digest
                    or receipt.get("identity") != identity
                    or any(receipt.get(key) != commit.get(key) for key in ("state_id", "state_sequence"))):
                raise ValueError("Result state receipt conflicts with its commit")
        names = set()
        for artifact in commit.get("artifacts", []):
            name = relative_path(artifact["path"])
            if name in names:
                raise ValueError("Duplicate final artifact")
            names.add(name)
            checksum, count = hashlib.sha256(), 0
            with repository.open(f"{prefix}/{name}", "rb") as stream:
                while block := stream.read(1024**2):
                    count += len(block)
                    if count > artifact["bytes"]:
                        raise ValueError("Final artifact byte count differs")
                    checksum.update(block)
            if count != artifact["bytes"] or checksum.hexdigest() != artifact["sha256"]:
                raise ValueError("Final artifact digest differs")
        if require_receipt and commit.get("storage_class") == "archive" and "physics.h5" not in names:
            raise ValueError("Stationary final has no complete numerical state")
        selected[point["id"]] = {"path": reference, "commit_sha256": digest,
            "identity": identity, "state_id": commit.get("state_id"),
            "state_sequence": commit.get("state_sequence"),
            "receipt_status": "verified" if receipt is not None else "not_available"}
    return selected
