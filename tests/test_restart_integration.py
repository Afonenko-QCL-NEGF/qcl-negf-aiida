"""Real localhost CalcJobs exercise retry provenance without running Julia."""

import pytest
from copy import deepcopy
from aiida import orm
from aiida.engine import run_get_node
from aiida.manage import get_manager
from aiida.common.folders import SandboxFolder
from aiida.engine.utils import instantiate_process

from aiida_qcl_negf.data import plan_data
from aiida_qcl_negf.workflow import QCLExecutionRestartWorkChain
from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.workflow import QCLPlanWorkChain
from aiida_qcl_negf.service import get_export_plan, get_run, get_run_report, open_artifact
from aiida_qcl_negf.validation import scheduler_options


def retarget(plan, identifier):
    plan["executions"][0]["id"] = identifier
    plan["nodes"][0]["execution_id"] = identifier
    plan["points"][0]["execution_id"] = identifier


@pytest.mark.integration
@pytest.mark.parametrize("identifier", ["pause-once", "crash-once"])
def test_verified_pause_or_crash_resumes_as_one_sequential_attempt(code, plan, identifier):
    retarget(plan, identifier)
    get_manager().get_runner()._poll_interval = 0.1
    outputs, node = run_get_node(QCLExecutionRestartWorkChain, code=code, plan=plan_data(plan),
        execution_id=orm.Str(identifier), options=orm.Dict(dict=scheduler_options({})),
        max_iterations=orm.Int(2), backoff_seconds=orm.Int(0))
    assert node.is_finished_ok, node.exit_message
    children = sorted(node.called, key=lambda child: child.inputs.attempt.value)
    assert [child.inputs.attempt.value for child in children] == [1, 2]
    assert children[0].is_terminated and children[1].inputs.recovery_bundle.uuid == children[0].outputs.recovery_bundle.uuid
    assert children[0].outputs.recovery_receipt.get_dict()["restart_coordinates"]["last_inner"] == 1
    assert outputs["selection"].get_dict()["attempt"] == 2
    assert "abandoned" not in outputs["retrieved"].base.repository.list_object_names("result")


@pytest.mark.integration
def test_pause_retry_budget_exhausts_without_reset(code, plan):
    retarget(plan, "always-pause")
    get_manager().get_runner()._poll_interval = 0.1
    _, node = run_get_node(QCLExecutionRestartWorkChain, code=code, plan=plan_data(plan),
        execution_id=orm.Str("always-pause"), options=orm.Dict(dict=scheduler_options({})),
        max_iterations=orm.Int(2), backoff_seconds=orm.Int(0))
    assert node.exit_status == 401
    assert len(node.called) == 2
    assert sorted(child.inputs.attempt.value for child in node.called) == [1, 2]


@pytest.mark.integration
def test_plan_pins_completed_attempt_for_artifacts_and_export(code, plan):
    retarget(plan, "pause-once")
    get_manager().get_runner()._poll_interval = 0.1
    _, node = run_get_node(QCLPlanWorkChain, code=code, plan=plan_data(plan),
        resources=orm.Dict(dict={}), max_attempts=orm.Int(2), retry_backoff_seconds=orm.Int(0))
    assert node.is_finished_ok
    snapshot = get_run(node.uuid)
    assert snapshot["results"]["pause-once"]["points"][0]["attempt"] == 2
    assert get_export_plan(node.uuid, "pause-once")["source"].startswith("aiida.retrieved")
    with open_artifact(node.uuid, "pause-once", "result/archive/pause-once/point-1/final/evidence.txt") as handle:
        assert handle.read() == b"Synthetic transport evidence\n"
    assert node.outputs.selections.execution_0.get_dict()["attempt"] == 2
    assert any("resuming verified state" in record["message"] for record in get_run_report(node.uuid))


@pytest.mark.integration
def test_portable_restart_preserves_prior_final_without_reexecuting_point(code, plan):
    retarget(plan, "pause-once-multipoint")
    successor = deepcopy(plan["points"][0])
    successor.update(id="point-2", order=2, predecessor_id="point-1")
    plan["points"].append(successor)
    plan["executions"][0]["point_ids"].append("point-2")
    plan["computation_count"] += 1
    get_manager().get_runner()._poll_interval = 0.1
    outputs, node = run_get_node(QCLExecutionRestartWorkChain, code=code, plan=plan_data(plan),
        execution_id=orm.Str("pause-once-multipoint"), options=orm.Dict(dict=scheduler_options({})),
        max_iterations=orm.Int(2), backoff_seconds=orm.Int(0))
    assert node.is_finished_ok, node.exit_message
    children = sorted(node.called, key=lambda child: child.inputs.attempt.value)
    assert len(children) == 2
    assert children[1].inputs.prior_archive.uuid == children[0].outputs.prior_archive.uuid
    result = __import__("json").loads(outputs["result"].get_content())
    assert [(point["id"], point["attempt"]) for point in result["points"]] == [("point-1", 1), ("point-2", 2)]
    selected = outputs["selection"].get_dict()["commits"]
    assert selected["point-1"]["identity"]["attempt"] == 1
    repository = outputs["retrieved"].base.repository
    assert "attempt 1" in repository.get_object_content("result/archive/pause-once-multipoint/point-1/final/evidence.txt")
    damaged = orm.FolderData()
    original = children[0].outputs.prior_archive.base.repository
    for root, _, names in original.walk():
        for name in names:
            path = str(root / name)
            with original.open(path, "rb") as stream:
                damaged.base.repository.put_object_from_filelike(stream, path)
    damaged.base.repository.put_object_from_filelike(__import__("io").BytesIO(b"corrupt"),
        "archive/pause-once-multipoint/point-1/final/physics.h5")
    for archive, budget, reason in [(None, 1024**2, "explicit prior archive"),
                                    (children[0].outputs.prior_archive, 1, "byte budget"),
                                    (damaged, 1024**2, "digest|byte count")]:
        extra = {"prior_archive": archive} if archive is not None else {}
        process = instantiate_process(get_manager().get_runner(), QCLExecutionCalculation,
            code=code, plan=node.inputs.plan, execution_id=node.inputs.execution_id, attempt=orm.Int(2),
            recovery_bundle=children[0].outputs.recovery_bundle, archive_byte_budget=orm.Int(budget),
            metadata={"options": scheduler_options({})}, **extra)
        try:
            with SandboxFolder() as folder, pytest.raises(ValueError, match=reason):
                process.prepare_for_submission(folder)
        finally:
            process.close()
