"""Inspect generated submission scripts without contacting a Slurm server."""

import base64
import json
import tomllib
import inspect
from pathlib import Path

import pytest
from aiida import orm
from aiida.common.folders import SandboxFolder
from aiida.engine.utils import instantiate_process
from aiida.manage import get_manager

from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.data import plan_data
from aiida_qcl_negf.validation import scheduler_options


def test_restart_entrypoint_is_published_in_package_metadata():
    metadata = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())
    assert metadata["project"]["entry-points"]["aiida.workflows"]["qcl_negf.execution_restart"] == "aiida_qcl_negf.workflow:QCLExecutionRestartWorkChain"


def test_service_exposes_explicit_retry_and_release_policy():
    from aiida_qcl_negf.service import submit_plan
    fields = inspect.signature(submit_plan).parameters
    assert {"release_id", "max_attempts", "retry_backoff_seconds", "archive_byte_budget"} <= fields.keys()


@pytest.mark.integration
def test_slurm_script_binds_attempt_and_disables_independent_requeue(code, plan):
    code.computer.scheduler_type = "core.slurm"
    process = instantiate_process(get_manager().get_runner(), QCLExecutionCalculation,
        code=code, plan=plan_data(plan), execution_id=orm.Str("execution-1"), attempt=orm.Int(7),
        metadata={"options": scheduler_options({})})
    try:
        with SandboxFolder() as folder:
            process.presubmit(folder)
            script = Path(folder.abspath, "_aiidasubmit.sh").read_text()
        assert "#SBATCH --no-requeue" in script
        marker = next(line.split("qcl-negf-attempt-v1:", 1)[1] for line in script.splitlines() if "qcl-negf-attempt-v1:" in line)
        descriptor = json.loads(base64.urlsafe_b64decode(marker))
        assert descriptor == {"execution_id": "execution-1", "attempt": 7, "output_directory": "result",
                              "solver_executable": str(code.filepath_executable)}
    finally:
        process.close()


@pytest.mark.integration
def test_release_pin_refuses_mutable_installed_executable(code, plan):
    process = instantiate_process(get_manager().get_runner(), QCLExecutionCalculation,
        code=code, plan=plan_data(plan), execution_id=orm.Str("execution-1"), release_id=orm.Str("release-1"),
        metadata={"options": scheduler_options({})})
    try:
        with SandboxFolder() as folder, pytest.raises(ValueError, match="immutable Nix"):
            process.prepare_for_submission(folder)
    finally:
        process.close()


@pytest.mark.integration
def test_release_guard_uses_pinned_identity_before_solver(code, plan):
    pinned = orm.InstalledCode(computer=code.computer, filepath_executable="/nix/store/synthetic/bin/qcl-negf",
                              default_calc_job_plugin="qcl_negf.execution")
    process = instantiate_process(get_manager().get_runner(), QCLExecutionCalculation,
        code=pinned, plan=plan_data(plan), execution_id=orm.Str("execution-1"), release_id=orm.Str("release-1"),
        metadata={"options": scheduler_options({})})
    try:
        with SandboxFolder() as folder:
            process.presubmit(folder)
            script = Path(folder.abspath, "_aiidasubmit.sh").read_text()
        guard = "qcl-negf-release-guard --release-id release-1 --solver-executable /nix/store/synthetic/bin/qcl-negf || exit 78"
        assert guard in script
        assert script.index(guard) < script.rindex("'run-plan'")
    finally:
        process.close()
