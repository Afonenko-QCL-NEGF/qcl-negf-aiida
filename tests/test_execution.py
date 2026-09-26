"""Exercise real AiiDA execution, retrieval, parser and provenance on localhost."""

from copy import deepcopy
import json

import pytest
from aiida import orm
from aiida.engine import run_get_node
from aiida.manage import get_manager
from aiida.plugins import CalculationFactory, ParserFactory, WorkflowFactory

from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.data import plan_data, read_json, read_plan
from aiida.engine.processes.calcjobs.tasks import PreSubmitException
from aiida.common.folders import SandboxFolder
from aiida.engine.utils import instantiate_process
from aiida_qcl_negf.service import get_run, get_run_report, kill_run, list_artifacts, list_runs, open_artifact
from aiida_qcl_negf.validation import scheduler_options, validate_plan, validate_scratch_root
from aiida_qcl_negf.workflow import QCLPlanWorkChain


def append_execution(plan, identifier):
    execution = deepcopy(plan["executions"][0])
    execution["id"] = identifier
    execution["point_ids"] = [f"point-{identifier}"]
    point = deepcopy(plan["points"][0])
    point.update(id=f"point-{identifier}", execution_id=identifier)
    node = deepcopy(plan["nodes"][0])
    node.update(execution_id=identifier, depends_on=[])
    plan["executions"].append(execution)
    plan["points"].append(point)
    plan["nodes"].append(node)
    plan["computation_count"] += 1


def test_entry_points():
    assert CalculationFactory("qcl_negf.execution") is QCLExecutionCalculation
    assert WorkflowFactory("qcl_negf.plan") is QCLPlanWorkChain
    assert ParserFactory("qcl_negf.execution").__name__ == "QCLExecutionParser"


def test_ambiguous_json_rejected_without_profile():
    with pytest.raises(ValueError, match="duplicate JSON key"):
        plan_data('{"schema": "one", "schema": "two"}')
    with pytest.raises(ValueError, match="number must be finite"):
        plan_data('{"value": NaN}')


def test_invalid_schema_becomes_value_error(plan):
    plan["executions"][0]["repetition"] = 0
    with pytest.raises(ValueError, match="executions.0.repetition"):
        validate_plan(plan)


@pytest.mark.parametrize("value", ["", "/", "relative", "/scratch/../elsewhere", "/scratch/", "/scratch//data", "/scratch\x00data", "/scratch\ndata"])
def test_reject_ambiguous_scratch_path(value):
    with pytest.raises(ValueError, match="scratch_root"):
        validate_scratch_root(value)


def test_scratch_path_can_contain_spaces():
    assert validate_scratch_root("/scratch/scientific data") == "/scratch/scientific data"


def test_plan_rejects_policy_and_dependencies(plan):
    validate_plan(plan)
    plan["nodes"][0]["required_evidence"] = ["converged"]
    with pytest.raises(ValueError, match="Invalid scientific plan"):
        validate_plan(plan)
    plan["nodes"][0]["required_evidence"] = []
    plan["nodes"][0]["depends_on"] = [plan["nodes"][0]["execution_id"]]
    with pytest.raises(ValueError, match="Invalid scientific plan"):
        validate_plan(plan)


@pytest.mark.parametrize("resources", [
    {"num_machines": 2}, {"num_mpiprocs_per_machine": 2}, {"num_cores_per_mpiproc": True},
    {"num_machines": True}, {"num_mpiprocs_per_machine": 1.0},
    {"max_wallclock_seconds": -1}, {"queue_name": "name\n#SBATCH x"}, {"custom_scheduler_commands": "command"},
])
def test_reject_unsafe_resources(resources):
    with pytest.raises(ValueError):
        scheduler_options(resources)


@pytest.mark.integration
@pytest.mark.usefixtures("aiida_profile_clean")
def test_calcjob_retrieves_and_parses(code, plan):
    get_manager().get_runner()._poll_interval = 0.1
    outputs, node = run_get_node(QCLExecutionCalculation, code=code, plan=plan_data(plan),
        execution_id=orm.Str("execution-1"), metadata={"options": scheduler_options({})})
    assert node.is_finished_ok, node.exit_message
    assert read_json(outputs["result"])["points"][0]["converged"] is True
    assert outputs["retrieved"].base.repository.get_object_content("result/nested/evidence.txt") == "Synthetic transport evidence\n"
    assert read_plan(node.inputs.plan) == plan


@pytest.mark.integration
@pytest.mark.usefixtures("aiida_profile_clean")
def test_plan_bytes_preserved(plan):
    raw = json.dumps(plan, ensure_ascii=False, indent=3).encode("utf-8")
    node = plan_data(raw).store()
    with node.open(mode="rb") as handle:
        assert handle.read() == raw
    assert read_plan(node) == plan


@pytest.mark.integration
@pytest.mark.usefixtures("aiida_profile_clean")
def test_scratch_argument_is_preserved_as_one_argument(code, plan):
    process = instantiate_process(get_manager().get_runner(), QCLExecutionCalculation,
        code=code, plan=plan_data(plan), execution_id=orm.Str("execution-1"),
        scratch_root=orm.Str("/scratch/scientific data"),
        metadata={"options": scheduler_options({})})
    try:
        with SandboxFolder() as folder:
            info = process.prepare_for_submission(folder)
        assert info.codes_info[0].cmdline_params == [
            "run-plan", "plan.json", "result", "--execution-id", "execution-1",
            "--scratch-root", "/scratch/scientific data",
        ]
    finally:
        process.close()


@pytest.mark.integration
@pytest.mark.usefixtures("aiida_profile_clean")
def test_parser_rejects_wrong_conditions_under_correct_fingerprint(code, plan):
    append_execution(plan, "wrong-conditions")
    get_manager().get_runner()._poll_interval = 0.1
    outputs, node = run_get_node(QCLExecutionCalculation, code=code, plan=plan_data(plan),
        execution_id=orm.Str("wrong-conditions"), metadata={"options": scheduler_options({})})
    assert node.exit_status == 301
    assert "result" not in outputs
    assert "retrieved" in outputs


@pytest.mark.integration
@pytest.mark.usefixtures("aiida_profile_clean")
def test_workflow_continues_independent_executions_after_failure(code, plan):
    append_execution(plan, "fail-execution")
    append_execution(plan, "independent")
    get_manager().get_runner()._poll_interval = 0.1
    _, node = run_get_node(QCLPlanWorkChain, code=code, plan=plan_data(plan), resources=orm.Dict(dict={}), max_concurrent=orm.Int(2))
    assert node.exit_status == 400
    children = {child.inputs.execution_id.value: child for child in node.called}
    assert set(children) == {"execution-1", "fail-execution", "independent"}
    assert children["fail-execution"].exit_status == 303  # CLI exited zero.
    assert children["independent"].is_finished_ok
    response = get_run(node.uuid)
    assert response["results"]["fail-execution"]["points"][0]["converged"] is False
    assert list_runs()[0]["uuid"] == node.uuid
    assert any("failed" in report["message"] for report in get_run_report(node.uuid))
    assert kill_run(node.uuid)["exit_status"] == 400
    files = list_artifacts(node.uuid)
    assert any(item["path"] == "result/nested/evidence.txt" for item in files)
    with open_artifact(node.uuid, "execution-1", "result/nested/evidence.txt") as handle:
        assert handle.read() == b"Synthetic transport evidence\n"
    with pytest.raises(ValueError):
        with open_artifact(node.uuid, "execution-1", "../outside"):
            pass


@pytest.mark.integration
@pytest.mark.usefixtures("aiida_profile_clean")
def test_direct_calcjob_rejects_thread_mismatch(code, plan):
    options = scheduler_options({"num_cores_per_mpiproc": 2})
    options["environment_variables"]["JULIA_NUM_THREADS"] = "8"
    with pytest.raises(PreSubmitException) as raised:
        run_get_node(QCLExecutionCalculation, code=code, plan=plan_data(plan),
            execution_id=orm.Str("execution-1"), metadata={"options": options})

    assert isinstance(raised.value.__cause__, ValueError)
    assert "JULIA_NUM_THREADS" in str(raised.value.__cause__)
