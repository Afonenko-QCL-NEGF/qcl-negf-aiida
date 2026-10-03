"""Bounded checks for the synthetic fixture and actual AiiDA retrieval boundary."""

from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import h5py
import pytest
import transport_fixture as fixture
from aiida_qcl_negf.validation import validate_plan
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.artifacts import validate_commit
from qcl_negf_results.export import export_snapshot
from qcl_negf_results.native import validate_native_handle


def frozen(tmp_path, *, mode="negative", native_mib=1, wait_seconds=0):
    template = Path(__file__).parent / "fixtures/plan.json"
    target = tmp_path / f"{mode}.json"
    fixture.freeze_plan(
        template, target, mode=mode, native_mib=native_mib, wait_seconds=wait_seconds
    )
    return target, json.loads(target.read_bytes())


def test_frozen_plan_is_marked_bounded_and_keeps_the_template_configuration(tmp_path):
    target, plan = frozen(tmp_path)
    template = json.loads((Path(__file__).parent / "fixtures/plan.json").read_bytes())
    validate_plan(plan)
    specification = plan["executions"][0]["provenance"]["synthetic_transport"]
    assert specification["scope"] == "transport-only; no scientific validation"
    assert specification["native_bytes"] == 1024 * 1024
    assert plan["model_revision"] == template["model_revision"]
    assert plan["resources"]["maximum_active_executions"] == 1
    assert plan["maximum_solver_runs"] == 1
    assert (
        plan["executions"][0]["resolved_configuration"]
        == template["executions"][0]["resolved_configuration"]
    )
    original = target.read_bytes()
    with pytest.raises(FileExistsError):
        fixture.freeze_plan(
            target, target, mode="negative", native_mib=1, wait_seconds=0
        )
    assert target.read_bytes() == original


def test_cli_exit_zero_publishes_negative_native_commit_and_whole_object_export(
    tmp_path,
):
    target, plan = frozen(tmp_path)
    result = tmp_path / "result"
    assert (
        fixture.main(
            [
                "run-plan",
                str(target),
                str(result),
                "--execution-id",
                "execution-1",
                "--fixture-native-mib",
                "1",
            ]
        )
        == 0
    )
    series = json.loads((result / "series_result.json").read_bytes())
    schema_validator("scientific-worker-result.schema.json").validate(series)
    point = series["points"][0]
    assert point["status"] == "completed" and point["converged"] is False
    assert point["quality"] == "unconverged"
    commit_path = result / point["data"]["result_commit"]
    commit = json.loads(commit_path.read_bytes())
    validate_commit(commit)
    assert commit["scientific_accepted"] is False
    assert commit["identity"]["plan_fingerprint"] == plan["fingerprint"]
    assert (
        commit["synthetic_provenance"]["plan_bytes_sha256"]
        == hashlib.sha256(target.read_bytes()).hexdigest()
    )
    native = commit_path.parent / "synthetic-analysis.h5"
    with h5py.File(native) as handle:
        validate_native_handle(
            handle, "physics.analysis", "qcl-negf-physics-analysis-v4"
        )
        assert handle["synthetic_transport/payload"].shape == (1024 * 1024,)
        assert (
            handle["metadata"].attrs["producer_kind"]
            == "synthetic-infrastructure-fixture"
        )
        assert len(handle["diagnostics/scba/sequence"]) == 0
        assert len(handle["diagnostics/physical_markers/available"]) == 0
    receipt = export_snapshot(
        result,
        tmp_path / "export",
        profile="full-state",
        plan=plan,
        job_status="completed_with_warnings",
    )
    import tarfile

    with tarfile.open(tmp_path / "export" / receipt["archive"], "r:xz") as archive:
        manifest = json.load(archive.extractfile("manifest.json"))
        assert (
            manifest["records"][0]["identity"]["producer_kind"]
            == "synthetic-infrastructure-fixture"
        )
        assert manifest["records"][0]["scientific_accepted"] is False
        native_object = next(
            item
            for item in manifest["files"]
            if item["media_type"] == "application/x-hdf5"
        )
        restored = archive.extractfile(native_object["path"]).read()
        plan_object = next(
            item
            for item in manifest["records"][0]["included"]
            if item["role"] == "plan"
        )
        assert archive.extractfile(plan_object["object"]).read() == target.read_bytes()
        assert (
            plan_object["object"]
            in next(
                item
                for item in manifest["records"][0]["included"]
                if item["role"] == "physics.analysis"
            )["dependencies"]
        )
    assert restored == native.read_bytes()
    assert receipt["profile"] == "full-state" and receipt["snapshot_consistent"] is True


@pytest.mark.integration
@pytest.mark.parametrize("attempt", [1, 7])
def test_calcjob_cli_and_retrieve_projection_keep_negative_native_result(
    tmp_path, code, attempt
):
    from aiida import orm
    from aiida.common.folders import SandboxFolder
    from aiida.common.links import LinkType
    from aiida.engine.daemon.execmanager import retrieve_files_from_list
    from aiida.engine.utils import instantiate_process
    from aiida.manage import get_manager
    from aiida.transports.plugins.local import LocalTransport
    from aiida_qcl_negf.calculation import QCLExecutionCalculation
    from aiida_qcl_negf.data import plan_data, read_json
    from aiida_qcl_negf.parser import QCLExecutionParser
    from aiida_qcl_negf.validation import scheduler_options

    target, plan = frozen(tmp_path)
    # Noncanonical spacing makes an accidental JSON rewrite visible.
    raw = json.dumps(plan, ensure_ascii=False, indent=3).encode()
    process = instantiate_process(
        get_manager().get_runner(), QCLExecutionCalculation,
        code=code, plan=plan_data(raw), execution_id=orm.Str("execution-1"),
        attempt=orm.Int(attempt), metadata={"options": scheduler_options({})},
    )
    try:
        with SandboxFolder() as folder:
            info = process.prepare_for_submission(folder)
            command = [sys.executable, str(Path(fixture.__file__).resolve()),
                       *info.codes_info[0].cmdline_params]
            completed = subprocess.run(command, cwd=folder.abspath, capture_output=True,
                                       timeout=30, check=False)
            assert completed.returncode == 0, completed.stderr.decode()
            Path(folder.abspath, "solver.stdout").write_bytes(completed.stdout)
            Path(folder.abspath, "solver.stderr").write_bytes(completed.stderr)
            # A source-tree-only decoy must never be visible to the parser.
            Path(folder.abspath, "result/unretrieved.txt").write_text("excluded")
            projection = tmp_path / "retrieved"
            projection.mkdir()
            process.node.set_remote_workdir(folder.abspath)
            with LocalTransport() as transport:
                asyncio.run(retrieve_files_from_list(
                    process.node, transport, str(projection), info.retrieve_list))
            retrieved = orm.FolderData(tree=str(projection)).store()
            retrieved.base.links.add_incoming(process.node, link_type=LinkType.CREATE,
                                              link_label="retrieved")
            subject = QCLExecutionParser(process.node)
            assert subject.parse().status == 303
            outputs = subject.outputs
            assert outputs["inventory"].get_dict()["complete"] is True
            assert "result/unretrieved.txt" not in {
                row["path"] for row in outputs["inventory"].get_dict()["files"]}
            assert retrieved.base.repository.get_object_content(
                "result/scientific_plan.json", mode="rb") == raw
            point = read_json(outputs["result"])["points"][0]
            assert point["attempt"] == attempt and point["converged"] is False
            commit_path = "result/" + point["data"]["result_commit"]
            commit_raw = retrieved.base.repository.get_object_content(commit_path, mode="rb")
            commit = json.loads(commit_raw)
            validate_commit(commit)
            assert commit["scientific_accepted"] is False
            assert commit["identity"]["attempt"] == attempt
            assert commit["synthetic_provenance"]["plan_bytes_sha256"] == hashlib.sha256(raw).hexdigest()
            prefix = str(Path(commit_path).parent)
            pointer = json.loads(retrieved.base.repository.get_object_content(
                str(Path(prefix).parent) + "/current.json", mode="rb"))
            assert pointer["commit_path"] == "generation-000001/commit.json"
            assert pointer["sha256"] == hashlib.sha256(commit_raw).hexdigest()
            for artifact in commit["artifacts"]:
                content = retrieved.base.repository.get_object_content(
                    prefix + "/" + artifact["path"], mode="rb")
                assert len(content) == artifact["bytes"]
                assert hashlib.sha256(content).hexdigest() == artifact["sha256"]
                if artifact["role"] == "plan":
                    assert content == raw
                assert content == Path(folder.abspath, prefix, artifact["path"]).read_bytes()
            selection = outputs["selection"].get_dict()
            assert selection["attempt"] == attempt
            assert selection["commits"][point["id"]]["commit_sha256"] == hashlib.sha256(commit_raw).hexdigest()
    finally:
        process.close()


def test_cli_rejects_zero_attempt_before_publication(tmp_path):
    target, _ = frozen(tmp_path)
    with pytest.raises(ValueError, match="attempt must be positive"):
        fixture.main(["run-plan", str(target), str(tmp_path / "result"),
                      "--execution-id", "execution-1", "--attempt", "0"])
    assert not (tmp_path / "result").exists()


@pytest.mark.parametrize("executable,label", [
    ("/tmp/qcl-negf-synthetic-transport-1", "SYNTHETIC-test"),
    ("/nix/store/fixture/bin/qcl-negf", "SYNTHETIC-test"),
    ("/nix/store/fixture/bin/qcl-negf-synthetic-transport-1", "production"),
])
def test_installed_harness_rejects_unmarked_code_before_profile_access(
    tmp_path, executable, label
):
    import run_transport_acceptance as acceptance

    with pytest.raises(ValueError, match="explicitly SYNTHETIC"):
        acceptance.main(["--computer", "slurm", "--executable", executable,
                         "--label", label, "--evidence", str(tmp_path / "evidence")])
    assert not (tmp_path / "evidence").exists()


@pytest.mark.parametrize("record,status", [
    ("JobId=73 JobState=COMPLETED ExitCode=0:0", "pass"),
    ("JobId=73 JobState=FAILED ExitCode=1:0", "fail"),
    ("JobId=73 JobState=COMPLETED ExitCode=2:0", "fail"),
    ("JobId=73 JobState=COMPLETED ExitCode=0:9", "fail"),
    ("JobId=73 JobState=RUNNING ExitCode=0:0", "fail"),
    ("JobId=74 JobState=COMPLETED ExitCode=0:0", "fail"),
    ("", "not_measured"),
    ("JobId=73 JobState=COMPLETED", "not_measured"),
])
def test_scheduler_receipt_requires_owned_completed_zero_exit_record(record, status):
    import run_transport_acceptance as acceptance

    assert acceptance.parse_scheduler_record(record, "73")["status"] == status


def test_scheduler_query_has_finite_budget_and_never_queries_unscoped_job(monkeypatch):
    import run_transport_acceptance as acceptance

    calls = []
    def query(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 1, "", "Invalid job id specified")
    monkeypatch.setattr(acceptance.subprocess, "run", query)
    assert acceptance.scheduler_receipt("73")["status"] == "not_measured"
    assert calls[0][0] == ["scontrol", "--oneliner", "show", "job", "73"]
    assert calls[0][1]["timeout"] == 30 and calls[0][1]["shell"] is False
    assert acceptance.scheduler_receipt("73; cancel-all")["status"] == "not_measured"
    assert len(calls) == 1


@pytest.mark.parametrize(
    "native_mib,wait_seconds", [(0, 0), (271, 0), (1, 301), (1, -1)]
)
def test_fixture_budget_rejects_before_publication(tmp_path, native_mib, wait_seconds):
    with pytest.raises(ValueError):
        frozen(tmp_path, native_mib=native_mib, wait_seconds=wait_seconds)
    assert not (tmp_path / "negative.json").exists()


def test_fixture_rejects_unmarked_production_plan_and_payload_mismatch(tmp_path):
    result = tmp_path / "result"
    template = Path(__file__).parent / "fixtures/plan.json"
    with pytest.raises(ValueError, match="synthetic"):
        fixture.main(
            ["run-plan", str(template), str(result), "--execution-id", "execution-1"]
        )
    target, _ = frozen(tmp_path, native_mib=270)
    with pytest.raises(ValueError, match="payload"):
        fixture.main(
            ["run-plan", str(target), str(result), "--execution-id", "execution-1"]
        )
    assert not result.exists()


def test_cancel_mode_waits_before_committing_and_does_not_claim_staging(
    tmp_path, monkeypatch
):
    target, _ = frozen(tmp_path, mode="cancel", wait_seconds=300)

    def interrupted(_):
        raise InterruptedError("cancelled fixture")

    monkeypatch.setattr(fixture.time, "sleep", interrupted)
    with pytest.raises(InterruptedError):
        fixture.main(
            [
                "run-plan",
                str(target),
                str(tmp_path / "result"),
                "--execution-id",
                "execution-1",
            ]
        )
    assert not (tmp_path / "result/series_result.json").exists()
    assert (tmp_path / "result/synthetic-started.json").is_file()
    with pytest.raises(ValueError, match="staging"):
        fixture.main(
            [
                "run-plan",
                str(target),
                str(tmp_path / "another-result"),
                "--execution-id",
                "execution-1",
                "--scratch-root",
                "/scratch/qcl-negf",
            ]
        )
