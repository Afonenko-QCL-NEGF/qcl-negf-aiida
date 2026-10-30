"""Preparation only: real plugin ports, no scheduler dispatch or numerical solve."""
import hashlib
import json

import pytest
from aiida import orm
from aiida.common.folders import SandboxFolder
from aiida.engine.utils import instantiate_process
from aiida.manage import get_manager
from aiida.schedulers.plugins.slurm import SlurmScheduler

from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.data import plan_data
from aiida_qcl_negf.validation import scheduler_options
from test_native_actor import native_graph


@pytest.mark.integration
def test_new_ordinary_input_fixed_spec_and_sidecar_only_retrieval(code, plan):
    plan["executions"][0]["outputs"]["recovery"] = {
        "enabled": True, "interval_seconds": 1800., "retain_generations": 2,
        "byte_budget": 512 * 1024**2, "reserve_bytes": 64 * 1024**2}
    raw = json.dumps(plan, indent=3).encode()
    process = instantiate_process(get_manager().get_runner(), QCLExecutionCalculation,
        code=code, plan=plan_data(raw), execution_id=orm.Str("execution-1"),
        metadata={"options": scheduler_options({})})
    try:
        with SandboxFolder() as folder:
            info = process.prepare_for_submission(folder)
            with folder.open("plan.json", "rb") as stream:
                assert stream.read() == raw
            with folder.open("capture-spec.json", "rb") as stream:
                spec = json.loads(stream.read())
        assert spec["selection"]["calcjob_uuid"] == process.node.uuid
        assert spec["plan"]["sha256"] == hashlib.sha256(raw).hexdigest()
        assert spec["allocated_logical_cpus"] is None
        assert "export QCL_NEGF_CAPTURE_SPEC=capture-spec.json" in info.prepend_text
        assert "performance-binding.json" in info.retrieve_list
        assert not any(isinstance(path, str) and path.startswith("performance/") for path in info.retrieve_list)
        assert info.codes_info[0].cmdline_params[:3] == ["run-plan", "plan.json", "result"]
    finally:
        process.close()


@pytest.mark.integration
def test_native_origin_literal_eleven_args_and_capture_unset(native_graph,monkeypatch):
    f=native_graph
    monkeypatch.setattr(orm.Computer,"get_scheduler",lambda self:SlurmScheduler())
    monkeypatch.setenv("QCL_NEGF_NATIVE_JOBS_ROOT","/srv/qcl-negf/jobs")
    process=instantiate_process(get_manager().get_runner(),QCLExecutionCalculation,
        code=f["code"],plan=f["plan"],source_graph=f["graph"],native_execution=f["native"],
        execution_id=orm.Str(f["execution"]["execution_id"]),attempt=orm.Int(7),
        release_id=orm.Str(f["execution"]["code"]["release_id"]),
        metadata={"options":scheduler_options({})})
    try:
        with SandboxFolder() as folder:
            info=process.prepare_for_submission(folder)
            assert info.codes_info[0].cmdline_params==[
                "run-plan","plan.json","/srv/qcl-negf/jobs/native/new-domain",
                "--execution-id",f["execution"]["execution_id"],"--source-graph","source-graph.json",
                "--native-execution","native-execution.json","--native-calcjob-uuid",process.node.uuid]
            for name,wanted in (("plan.json",f["raw_plan"]),("source-graph.json",f["raw_graph"]),
                                ("native-execution.json",f["raw_native"])):
                with folder.open(name,"rb") as stream:assert stream.read()==wanted
            assert not folder.isfile("capture-spec.json")
        assert info.prepend_text.endswith("\nunset QCL_NEGF_CAPTURE_SPEC")
        assert info.retrieve_list==["series_result.json","native-selection.json","native-close.json"]
    finally:
        process.close()
