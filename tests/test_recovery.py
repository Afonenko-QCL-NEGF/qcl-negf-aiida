"""Hash-bound portable state fixtures; these bytes are not scientific solutions."""

import hashlib
import io
import json
import importlib
from contextlib import contextmanager

import pytest


class Repository:
    def __init__(self):
        self.files = {}

    @contextmanager
    def open(self, path, mode):
        assert mode == "rb"
        yield io.BytesIO(self.files[path])


def generation(repository, sequence):
    prefix = f"result/recovery/execution-1/point-1/generation-{sequence:06d}"
    identity = {"execution_id": "execution-1", "point_id": "point-1", "attempt": 1, "plan_fingerprint": "a" * 64}
    artifacts = []
    for name in ("physics.h5", "history.h5", "recovery.json", "resolved_configuration.json"):
        data = f"synthetic {name} {sequence}".encode()
        repository.files[f"{prefix}/{name}"] = data
        artifacts.append({"path": name, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)})
    commit = {"schema": "qcl-negf.artifact-commit.v2", "identity": identity,
              "state_id": f"state-{sequence}", "state_sequence": sequence, "artifacts": artifacts,
              "checkpoint_ready": True, "storage_class": "recovery"}
    raw = json.dumps(commit).encode()
    repository.files[f"{prefix}/commit.json"] = raw
    receipt = {"schema": "qcl-negf-recovery-receipt-v1", "status": "verified", "identity": identity,
               "state_id": commit["state_id"], "state_sequence": sequence,
               "commit_sha256": hashlib.sha256(raw).hexdigest(), "publication_scope": "local_filesystem"}
    repository.files[f"{prefix}/receipt.json"] = json.dumps(receipt).encode()
    return prefix


def test_corrupted_newest_bundle_falls_back_with_integrity_reason():
    repository = Repository()
    previous, latest = generation(repository, 1), generation(repository, 2)
    repository.files[f"{latest}/physics.h5"] = b"damaged"
    assert importlib.util.find_spec("aiida_qcl_negf.recovery") is not None
    recovery = importlib.import_module("aiida_qcl_negf.recovery")
    selected, rejected = recovery.select_recovery(repository, list(repository.files),
        execution_id="execution-1", attempt=1, plan_fingerprint="a" * 64)
    assert selected["prefix"] == previous
    assert rejected and latest in rejected[0]


def test_pause_receipt_binds_exact_state_without_silent_fallback():
    repository = Repository()
    prefix = generation(repository, 1)
    manifest = json.loads(repository.files[f"{prefix}/receipt.json"])
    pause = {"schema": "qcl-negf-pause-receipt-v1", "status": "paused", "execution_id": "execution-1",
             "attempt": 1, "commit_path": f"{prefix.removeprefix('result/')}/commit.json",
             "commit_sha256": manifest["commit_sha256"], "state_id": "state-1", "state_sequence": 99}
    assert importlib.util.find_spec("aiida_qcl_negf.recovery") is not None
    recovery = importlib.import_module("aiida_qcl_negf.recovery")
    with pytest.raises(ValueError, match="state"):
        recovery.verify_pause_receipt(repository, pause, execution_id="execution-1", attempt=1, plan_fingerprint="a" * 64)


def test_compact_result_refuses_commit_from_other_attempt():
    repository = Repository()
    prefix = generation(repository, 1)
    module = importlib.import_module("aiida_qcl_negf.recovery")
    assert hasattr(module, "pin_result_commits")
    point = {"id": "point-1", "execution_id": "execution-1", "attempt": 2,
             "data": {"result_commit": f"{prefix.removeprefix('result/')}/commit.json"}}
    with pytest.raises(ValueError, match="identity"):
        module.pin_result_commits(repository, [point], plan_fingerprint="a" * 64)


def test_latest_generation_with_missing_prior_final_falls_back_to_complete_closure():
    repository = Repository()
    previous, latest = generation(repository, 1), generation(repository, 2)
    for prefix in (previous, latest):
        commit = json.loads(repository.files[f"{prefix}/commit.json"])
        prior = []
        if prefix == latest:
            prior = [{"point": {"id": "earlier", "execution_id": "execution-1", "attempt": 1,
                     "status": "completed", "coordinates": {"temperature_K": 300, "voltage_per_period_V": 0.1,
                     "branch": "forward", "order": 1}, "data": {"result_commit": "archive/execution-1/earlier/final/commit.json"}},
                     "final_commit": "execution-1/earlier/final/commit.json", "files": [
                         {"path": "physics.h5", "bytes": 1, "sha256": "b" * 64}], "receipt": {
                         "identity": {"execution_id": "execution-1"}, "state_id": "prior", "state_sequence": 1,
                         "commit_sha256": "c" * 64}}]
        progress = {"schema": "qcl-negf-execution-progress-v1", "contract_set": "qcl-negf.results.v1", "identity": commit["identity"],
                    "execution_id": "execution-1", "plan_fingerprint": "a" * 64,
                    "active_point_id": "point-1", "completed_points": prior}
        raw = json.dumps(progress).encode()
        repository.files[f"{prefix}/execution_progress.json"] = raw
        commit["artifacts"].append({"path": "execution_progress.json", "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})
        encoded = json.dumps(commit).encode()
        repository.files[f"{prefix}/commit.json"] = encoded
        receipt = json.loads(repository.files[f"{prefix}/receipt.json"])
        receipt["commit_sha256"] = hashlib.sha256(encoded).hexdigest()
        repository.files[f"{prefix}/receipt.json"] = json.dumps(receipt).encode()
    recovery = importlib.import_module("aiida_qcl_negf.recovery")
    selected, rejected = recovery.select_recovery(repository, list(repository.files),
        execution_id="execution-1", attempt=1, plan_fingerprint="a" * 64,
        plan_points={"point-1": {"order": 2}, "earlier": {"execution_id": "execution-1", "order": 1,
                     "temperature_K": 300, "voltage_per_period_V": 0.1, "branch": "forward"}})
    assert selected["prefix"] == previous
    assert rejected and latest in rejected[0]
