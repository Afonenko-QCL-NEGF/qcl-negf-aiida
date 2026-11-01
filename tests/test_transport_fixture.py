"""Native replacement for the retired Python transport producer.

These are SOURCE/L controls using real isolated ORM links. Broker submission and
selected access are bounded test ports; they do not qualify an installed actor.
The actual Runner-emitted constructor/export proof is a separate explicit L gate.
"""
from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiida import orm
from aiida.common.folders import SandboxFolder
from aiida.engine.utils import instantiate_process
from aiida.manage import get_manager
from aiida.schedulers.plugins.slurm import SlurmScheduler
from plumpy import ProcessState
from qcl_negf_contracts.messages import encode

from aiida_qcl_negf import native_backend, service
from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.native_actor import NativeActorUnavailable
from aiida_qcl_negf.validation import scheduler_options
from aiida_qcl_negf.workflow import QCLPlanWorkChain, attempt_decision
from test_native_actor import metadata_graph, native_graph
from test_native_parser import absent_raw, access, retrieved


@pytest.mark.integration
@pytest.mark.parametrize("native_graph", ["publication"], indirect=True)
def test_publication_service_admits_exact_original_nodes_with_one_transport_child(
    native_graph, monkeypatch,
):
    """Dropping original-plan reuse or the native namespace breaks this ingress."""
    f = native_graph
    for name in ("QCL_NEGF_RELEASE_ID", "QCL_NEGF_SOLVER_EXECUTABLE", "QCL_NEGF_RELEASE_GATE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(service, "get_daemon_client", lambda: SimpleNamespace(is_daemon_running=True))
    processes = []

    def enqueue(cls, **inputs):
        process = instantiate_process(get_manager().get_runner(), cls, **inputs)
        processes.append(process)
        return process.node

    monkeypatch.setattr(service, "submit", enqueue)
    try:
        result = service.submit_plan(
            f["raw_plan"], f["code"].uuid, {}, label="SYNTHETIC-native-publication",
            release_id=f["execution"]["code"]["release_id"], max_attempts=1,
            source_graph=f["graph"], native_executions={"execution-1": f["native"]},
        )
        process, = processes
        assert result["uuid"] == process.node.uuid
        assert process.setup() is None
        assert process.ctx.pending == ["execution-1"]
        assert process.inputs.max_attempts.value == 1
        assert process.inputs.code.uuid == f["code"].uuid
        assert process.inputs.release_id.value == f["execution"]["code"]["release_id"]
        for node, original, raw in (
            (process.inputs.plan, f["plan"], f["raw_plan"]),
            (process.inputs.source_graph, f["graph"], f["raw_graph"]),
            (process.inputs.native_executions.execution_0, f["native"], f["raw_native"]),
        ):
            assert node.uuid == original.uuid
            with node.open(mode="rb") as stream:
                assert stream.read() == raw
    finally:
        for process in processes:
            process.close()


@pytest.mark.integration
@pytest.mark.parametrize("native_graph", ["publication"], indirect=True)
@pytest.mark.parametrize("mutation", ["physical", "grid", "source_history"])
def test_reserved_publication_profile_refuses_changed_model_grid_or_history_before_upload(
    native_graph, monkeypatch, mutation,
):
    """A selector matching only the reserved ID must fail these three controls."""
    plan = json.loads(native_graph["raw_plan"])
    execution = plan["executions"][0]
    if mutation == "physical":
        execution["resolved_configuration"]["physical"]["lo_phonon_energy"] = "35 meV"
    elif mutation == "grid":
        execution["resolved_configuration"]["numerical"]["energy_nodes"] += 2
    else:
        history = execution["provenance"]["sources"]
        key = next(iter(history))
        history[key] = [*history[key], history[key][-1] + "-changed"]
    f = metadata_graph(native_graph, encode(plan))
    monkeypatch.setattr(orm.Computer, "get_scheduler", lambda self: SlurmScheduler())
    monkeypatch.setenv("QCL_NEGF_NATIVE_JOBS_ROOT", "/srv/qcl-negf/jobs")
    process = instantiate_process(
        get_manager().get_runner(), QCLExecutionCalculation,
        **{name: f["calc"].inputs[name] for name in
           ("code", "plan", "source_graph", "native_execution", "execution_id", "attempt", "release_id")},
        metadata={"options": scheduler_options({})},
    )
    try:
        with SandboxFolder() as folder:
            with pytest.raises(NativeActorUnavailable, match="engineering_fixture_profile"):
                process.prepare_for_submission(folder)
            assert list(Path(folder.abspath).iterdir()) == []
        assert process.node.get_job_id() is None
        assert "native_submit_script" not in process.node.outputs
    finally:
        process.close()


@pytest.mark.integration
def test_failed_native_parser_pair_survives_root_failure_without_retry(native_graph, monkeypatch):
    """Parser302 must retain exact CREATE outputs and become terminal root400."""
    f = native_graph
    monkeypatch.setattr(orm.Computer, "get_scheduler", lambda self: SlurmScheduler())
    root = instantiate_process(
        get_manager().get_runner(), QCLPlanWorkChain,
        code=f["code"], plan=f["plan"], source_graph=f["graph"],
        native_executions={"execution_0": f["native"]},
        release_id=f["calc"].inputs.release_id, max_attempts=orm.Int(1),
        resources=orm.Dict(dict={}),
    )
    process = None
    try:
        assert root.setup() is None
        process = QCLExecutionCalculation(
            runner=get_manager().get_runner(), parent_pid=root.node.pk,
            inputs={
                **{name: f["calc"].inputs[name] for name in
                   ("code", "plan", "source_graph", "native_execution", "execution_id", "attempt", "release_id")},
                "metadata": {"call_link_label": "negative", "options": scheduler_options({})},
            },
        )
        assert process.node.caller.uuid == root.node.uuid
        raw = absent_raw()  # Existing honest failed/absent source; no Python scientific state.
        retrieved(process.node, raw)
        monkeypatch.setattr(native_backend, "open_native_selected_sources", lambda *a, **k: access(raw))
        status = process.parse_retrieved_output()
        assert status.status == 302
        process.update_outputs()
        for name in ("result", "native_binding", "selection"):
            assert process.node.outputs[name].creator.uuid == process.node.uuid
        with process.node.outputs.result.open(mode="rb") as stream:
            assert stream.read() == raw
        binding = json.loads(process.node.outputs.native_binding.get_content())
        assert binding["points"][0]["source"]["kind"] == "absent"
        assert binding["points"][0]["creator"] is None
        assert json.loads(raw)["points"][0]["converged"] is False
        assert attempt_decision(302, scheduler_stopped=False, recovery_available=False) == "scientific_terminal"
        process.node.set_exit_status(302)
        process.node.set_process_state(ProcessState.FINISHED)
        root.ctx.pending = []
        root.ctx.batch = {"execution_0": f["execution"]["execution_id"]}
        root.ctx.execution_0 = process.node
        root.inspect_batch()
        assert root.finalize().status == 400
        root.update_outputs()
        assert root.node.outputs.results.execution_0.uuid == process.node.outputs.result.uuid
        assert root.node.outputs.artifacts.execution_0.uuid == process.node.outputs.retrieved.uuid
        assert root.node.outputs.selections.execution_0.uuid == process.node.outputs.selection.uuid
        with root.node.outputs.results.execution_0.open(mode="rb") as stream:
            assert stream.read() == raw
    finally:
        if process is not None:
            process.close()
        root.close()
