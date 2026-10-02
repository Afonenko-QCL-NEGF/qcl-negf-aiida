"""Pure, tiny checks for the explicitly synthetic infrastructure fixture."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

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


def test_real_parser_keeps_negative_result_and_returns_303_without_a_profile(
    tmp_path, monkeypatch
):
    from aiida_qcl_negf import parser

    target, plan = frozen(tmp_path)
    fixture.main(
        [
            "run-plan",
            str(target),
            str(tmp_path / "result"),
            "--execution-id",
            "execution-1",
        ]
    )

    class Repository:
        def walk(self):
            for path, directories, names in os.walk(tmp_path / "result"):
                yield Path(path).relative_to(tmp_path), directories, names

        def open(self, path, mode):
            return (tmp_path / path).open(mode)

    outputs = {}
    subject = SimpleNamespace(
        retrieved=SimpleNamespace(base=SimpleNamespace(repository=Repository())),
        node=SimpleNamespace(
            inputs=SimpleNamespace(
                plan=None, execution_id=SimpleNamespace(value="execution-1")
            )
        ),
        out=lambda name, value: outputs.update({name: value}),
        exit_codes=SimpleNamespace(
            ERROR_MISSING_RESULT=300,
            ERROR_INVALID_RESULT=301,
            ERROR_SCIENTIFIC_FAILURE=302,
            ERROR_UNCONVERGED_RESULT=303,
        ),
    )
    monkeypatch.setattr(parser, "read_plan", lambda _: plan)
    monkeypatch.setattr(parser.orm, "Dict", lambda *, dict: dict)
    monkeypatch.setattr(
        parser.orm, "SinglefileData", lambda *, file, filename: json.load(file)
    )
    assert parser.QCLExecutionParser.parse(subject) == 303
    assert outputs["inventory"]["complete"] is True
    assert outputs["result"]["points"][0]["converged"] is False


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
