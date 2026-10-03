"""Exercise the actual service admission path with isolated native AiiDA nodes."""

import json
from types import SimpleNamespace

import pytest
from aiida import orm
from aiida.common.folders import SandboxFolder
from aiida.engine.utils import instantiate_process
from aiida.manage import get_manager

from aiida_qcl_negf import service
from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.validation import scheduler_options

SOLVER = "/nix/store/deployment-solver/bin/qcl-negf"


@pytest.fixture
def submission(code, monkeypatch, tmp_path):
    """Replace only daemon dispatch; retain service validation and native inputs."""
    monkeypatch.delenv("QCL_NEGF_RELEASE_ID", raising=False)
    monkeypatch.delenv("QCL_NEGF_SOLVER_EXECUTABLE", raising=False)
    monkeypatch.delenv("QCL_NEGF_RELEASE_GATE", raising=False)
    monkeypatch.setattr(service, "get_daemon_client", lambda: SimpleNamespace(is_daemon_running=True))
    processes = []
    def dispatch(process_class, **inputs):
        process = instantiate_process(get_manager().get_runner(), process_class, **inputs)
        processes.append(process)
        return process.node
    monkeypatch.setattr(service, "submit", dispatch)
    pinned = orm.InstalledCode(computer=code.computer, filepath_executable=SOLVER,
                              default_calc_job_plugin="qcl_negf.execution").store()
    gate = tmp_path / "admission.json"
    gate.write_text(json.dumps({"open": True, "release_id": "release-1"}))
    yield code, pinned, processes, gate
    for process in processes:
        process.close()


@pytest.mark.integration
@pytest.mark.parametrize("explicit", [None, "release-1"])
def test_service_binds_deployment_release_to_exact_installed_code_and_guard(submission, plan, monkeypatch, explicit):
    _, pinned, processes, gate = submission
    monkeypatch.setenv("QCL_NEGF_RELEASE_ID", "release-1")
    monkeypatch.setenv("QCL_NEGF_SOLVER_EXECUTABLE", SOLVER)
    monkeypatch.setenv("QCL_NEGF_RELEASE_GATE", str(gate))
    summary = service.submit_plan(plan, pinned.uuid, {}, release_id=explicit)
    assert summary["uuid"] == processes[0].node.uuid
    assert processes[0].inputs.release_id.value == "release-1"
    process = instantiate_process(get_manager().get_runner(), QCLExecutionCalculation,
        code=processes[0].inputs.code, plan=processes[0].inputs.plan,
        execution_id=orm.Str("execution-1"), release_id=processes[0].inputs.release_id,
        metadata={"options": scheduler_options({})})
    try:
        with SandboxFolder() as folder:
            process.presubmit(folder)
            with folder.open("_aiidasubmit.sh") as stream:
                script = stream.read()
        assert f"/run/current-system/sw/bin/qcl-negf-release guard --release-id release-1 --solver-executable {SOLVER} || exit 78" in script
    finally:
        process.close()


@pytest.mark.integration
def test_standalone_service_without_deployment_environment_does_not_invent_release(submission, plan):
    code, _, processes, _ = submission
    service.submit_plan(plan, code.uuid, {})
    assert "release_id" not in processes[0].inputs


@pytest.mark.integration
@pytest.mark.parametrize("release, solver, explicit, chosen, reason", [
    ("release-1", SOLVER, "release-other", "pinned", "conflict"),
    ("release-1", "/nix/store/other-solver/bin/qcl-negf", None, "pinned", "executable"),
    ("release-1", SOLVER, None, "mutable", "immutable"),
    ("release-1", None, None, "pinned", "together"),
    (None, SOLVER, None, "pinned", "together"),
    ("release-1", "/nix/store/../mutable/solver", None, "pinned", "immutable"),
    ("release invalid", SOLVER, None, "pinned", "release_id"),
])
def test_deployed_service_refuses_conflicting_or_incomplete_release_before_dispatch(
        submission, plan, monkeypatch, release, solver, explicit, chosen, reason):
    code, pinned, processes, gate = submission
    monkeypatch.setenv("QCL_NEGF_RELEASE_GATE", str(gate))
    if release is not None:
        monkeypatch.setenv("QCL_NEGF_RELEASE_ID", release)
    if solver is not None:
        monkeypatch.setenv("QCL_NEGF_SOLVER_EXECUTABLE", solver)
    with pytest.raises(ValueError, match=reason):
        service.submit_plan(plan, pinned.uuid if chosen == "pinned" else code.uuid, {}, release_id=explicit)
    assert processes == []


@pytest.mark.integration
@pytest.mark.parametrize("contents", [{"open": False, "release_id": "release-1"},
    {"open": True, "release_id": "release-other"}, None, "malformed", {"open": 1, "release_id": "release-1"}])
def test_deployed_service_refuses_closed_missing_or_conflicting_shared_gate(submission, plan, monkeypatch, contents):
    _, pinned, processes, gate = submission
    monkeypatch.setenv("QCL_NEGF_RELEASE_ID", "release-1")
    monkeypatch.setenv("QCL_NEGF_SOLVER_EXECUTABLE", SOLVER)
    monkeypatch.setenv("QCL_NEGF_RELEASE_GATE", str(gate))
    if contents is None:
        gate.unlink()
    elif contents == "malformed":
        gate.write_text("not-json")
    else:
        gate.write_text(json.dumps(contents))
    with pytest.raises(ValueError, match="admission"):
        service.submit_plan(plan, pinned.uuid, {})
    assert processes == []
