"""Actual isolated ORM facts; these fixtures never qualify an installed actor."""
from copy import deepcopy
import hashlib
import importlib
import importlib.util
import inspect
import io
import os
import shlex
import subprocess
import sys
from pathlib import Path
import tomllib
from types import SimpleNamespace

import pytest
from aiida import orm
from aiida.common.links import LinkType
from aiida.common.folders import SandboxFolder
from aiida.engine.utils import instantiate_process
from aiida.manage import get_manager
from aiida.schedulers.plugins.slurm import SlurmScheduler
from qcl_negf_contracts.messages import encode

from aiida_qcl_negf.service import submit_plan
from aiida_qcl_negf import service
from aiida_qcl_negf import science_association as association
from aiida_qcl_negf.workflow import QCLExecutionRestartWorkChain,QCLPlanWorkChain
from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.validation import scheduler_options
from test_native_binding import SEED


def actor():
    assert importlib.util.find_spec("aiida_qcl_negf.native_actor"), "missing public native actor loader"
    return importlib.import_module("aiida_qcl_negf.native_actor")


def blob(raw, filename="input.json"):
    return orm.SinglefileData(file=io.BytesIO(raw), filename=filename)


def digest(node, raw):
    return dict(node_uuid=node.uuid, bytes=str(len(raw)), sha256=hashlib.sha256(raw).hexdigest())


@pytest.fixture
def native_graph(aiida_profile_clean, aiida_computer_local):
    computer = aiida_computer_local()
    # Public properties on this isolated Computer. No installed plugin claim.
    computer.scheduler_type = "qcl_negf.native_slurm"
    computer.transport_type = "qcl_negf.native_local"
    code = orm.InstalledCode(computer=computer, filepath_executable="/nix/store/synthetic/bin/qcl-negf",
        default_calc_job_plugin="qcl_negf.execution").store()
    frozen=deepcopy(SEED["plan"])
    frozen["nodes"]=[dict(estimated_memory_bytes=256,stage_id="standalone",required_evidence=[],
        comparable_fingerprint=frozen["fingerprint"],execution_id=frozen["executions"][0]["id"],
        comparison_reference=None,forbidden_evidence=[],priority=0,reserve=False,
        comparison_kind="controlled_axis",controlled_paths=[],depends_on=[])]
    raw_plan = b" \n" + encode(frozen) + b"\n"
    raw_graph = b"\n" + encode(SEED["graph"])
    plan, graph = blob(raw_plan).store(), blob(raw_graph).store()
    native = blob(b"{}")
    execution = deepcopy(SEED["execution_binding"])
    execution["input_plan"] = digest(plan, raw_plan)
    execution["source_graph"] = digest(graph, raw_graph)
    execution["code"].update(code_uuid=code.uuid, computer_uuid=computer.uuid,
        solver_executable=str(code.filepath_executable))
    execution["storage"].update(computer_uuid=computer.uuid, execution_binding_uuid=native.uuid,
        relative_execution_root="native/new-domain")
    raw_native = b" \n" + encode(execution)
    native.set_file(io.BytesIO(raw_native), filename="native-execution.json")
    native.store()
    release=orm.Str(execution["code"]["release_id"]).store()
    run = orm.WorkChainNode(process_type=association._PLAN_PROCESS)
    for name, node in (("plan",plan),("source_graph",graph),("code",code),
                       ("native_executions__execution_0",native),("release_id",release)):
        run.base.links.add_incoming(node, LinkType.INPUT_WORK, name)
    run.store()
    restart = orm.WorkChainNode(process_type=association._RESTART_PROCESS)
    restart.base.links.add_incoming(run, LinkType.CALL_WORK, "execution_0")
    attempt, identifier = orm.Int(7).store(), orm.Str(execution["execution_id"]).store()
    inputs = {"plan":plan,"source_graph":graph,"code":code,"native_execution":native,
              "attempt":attempt,"execution_id":identifier,"release_id":release}
    for name,node in inputs.items(): restart.base.links.add_incoming(node,LinkType.INPUT_WORK,name)
    restart.store()
    calc = orm.CalcJobNode(computer=computer, process_type=association._CALC_PROCESS)
    calc.base.links.add_incoming(restart,LinkType.CALL_CALC,"iteration_01")
    for name,node in inputs.items(): calc.base.links.add_incoming(node,LinkType.INPUT_CALC,name)
    calc.set_option("submit_script_filename","_aiidasubmit.sh")
    for key,value in scheduler_options({}).items():calc.set_option(key,value)
    calc.store()
    calc.set_remote_workdir("/srv/qcl-negf/jobs/upload/unique")
    script = blob(b"#!/bin/bash\n# synthetic original script\n", "_aiidasubmit.sh")
    script.base.links.add_incoming(calc,LinkType.CREATE,"native_submit_script")
    script.store()
    return dict(calc=calc, run=run, restart=restart, plan=plan, graph=graph, native=native,
        code=code, script=script, raw_native=raw_native, raw_plan=raw_plan, raw_graph=raw_graph,
        execution=execution)


def test_native_public_metadata_and_service_ports():
    params=inspect.signature(submit_plan).parameters
    assert {"source_graph","native_executions"} <= params.keys(), "missing explicit native ingress"
    metadata=tomllib.loads((Path(__file__).parents[1]/"pyproject.toml").read_text())
    assert metadata["project"]["scripts"]["qcl-negf-native-actor"].endswith(":main")
    assert "qcl_negf.native_slurm" in metadata["project"]["entry-points"]["aiida.schedulers"]


def test_cli_invalid_budget_refuses_before_profile_access(monkeypatch,capsys):
    import aiida
    import sys
    module=actor();calls=[]
    monkeypatch.setattr(aiida,"load_profile",lambda name:calls.append(name))
    monkeypatch.setattr(sys,"argv",["qcl-negf-native-actor","--profile","unopened","launch",
        "00000000-0000-4000-8000-000000000001","--maximum-metadata-bytes","0"])
    assert module.main()==75
    assert calls==[]
    assert len(capsys.readouterr().err.encode())<=2048


def test_cli_missing_existing_config_refuses_without_creating_dirs(monkeypatch,tmp_path):
    import aiida
    import sys
    module=actor();calls=[];missing=tmp_path/"uncreated"
    monkeypatch.setenv("AIIDA_PATH",str(missing))
    monkeypatch.setattr(aiida,"load_profile",lambda name:calls.append(name))
    monkeypatch.setattr(sys,"argv",["qcl-negf-native-actor","--profile","unopened","launch",
        "00000000-0000-4000-8000-000000000001","--maximum-metadata-bytes","8388608"])
    assert module.main()==75
    assert calls==[] and not missing.exists()


def test_fresh_cli_import_cannot_initialize_missing_aiida_instance(tmp_path):
    missing=tmp_path/"must-stay-absent"
    completed=subprocess.run([sys.executable,"-c",
        "from aiida_qcl_negf.native_actor import main;raise SystemExit(main())",
        "--profile","missing","launch","00000000-0000-4000-8000-000000000001",
        "--maximum-metadata-bytes","8388608"],env={**os.environ,"AIIDA_PATH":str(missing)},
        stdin=subprocess.DEVNULL,capture_output=True,timeout=15)
    assert completed.returncode==75
    assert not missing.exists(),"CLI import initialized a missing AiiDA instance"


@pytest.mark.integration
def test_active_unsealed_real_input_call_create_original_bytes(native_graph):
    module=actor(); f=native_graph
    assert not f["calc"].is_sealed and not f["calc"].is_terminated
    launch=module.load_native_launch(f["calc"].uuid,maximum_metadata_bytes=8*1024*1024)
    assert launch["run_uuid"]==f["run"].uuid
    assert launch["transport_attempt"]==7
    assert launch["files"]["plan"]["sha256"]==hashlib.sha256(f["raw_plan"]).hexdigest()
    assert module.read_native_execution(f["calc"].uuid,maximum_metadata_bytes=8*1024*1024)==f["raw_native"]
    assert set(launch["files"])=={"plan","source_graph","native_execution","submit_script"}


def origin_epoch(fixture, nonce="0"):
    return "c1-"+fixture["calc"].uuid.replace("-","")+"-"+fixture["native"].uuid.replace("-","")+"-"+nonce


@pytest.mark.integration
@pytest.mark.parametrize("nonce",["0","18446744073709551615"])
def test_origin_port_reads_exact_originals_without_runtime_selection(native_graph,monkeypatch,nonce):
    module=actor();f=native_graph
    f["calc"].base.attributes.delete("remote_workdir")
    def forbidden(*args,**kwargs):
        raise AssertionError("offline original read touched mutable runtime selection")
    monkeypatch.setattr(orm.CalcJobNode,"get_remote_workdir",forbidden)
    monkeypatch.setattr(orm.CalcJobNode,"get_job_id",forbidden)
    monkeypatch.setattr(orm,"load_node",forbidden)
    raw_native,raw_script=module.read_native_origin_inputs(f["calc"].uuid,
        source_epoch=origin_epoch(f,nonce),maximum_metadata_bytes=8*1024*1024)
    assert raw_native==f["raw_native"]
    assert raw_script==b"#!/bin/bash\n# synthetic original script\n"


@pytest.mark.integration
@pytest.mark.parametrize("fault",["foreign_calc","foreign_input"])
def test_origin_port_requires_actual_calcjob_and_original_input_uuid(native_graph,fault):
    module=actor();f=native_graph;epoch=origin_epoch(f)
    if fault=="foreign_calc":epoch=epoch.replace(f["calc"].uuid.replace("-",""),"a"*32)
    else:epoch=epoch.replace(f["native"].uuid.replace("-",""),"b"*32)
    with pytest.raises(module.NativeActorUnavailable):
        module.read_native_origin_inputs(f["calc"].uuid,source_epoch=epoch,
            maximum_metadata_bytes=8*1024*1024)


@pytest.mark.parametrize("epoch",[
    "epoch-1","c1-"+"a"*32+"-"+"b"*32+"-00",
    "c1-"+"A"*32+"-"+"b"*32+"-1",
    "c1-"+"a"*32+"-"+"B"*32+"-1",
    "c1-"+"a"*32+"-"+"b"*32+"-18446744073709551616",
    "c1-"+"a"*32+"-"+"b"*32+"-1\n",
    "c1-"+"a"*32+"-"+"b"*32+"--1",None,1])
def test_origin_port_rejects_noncanonical_epoch_before_orm(monkeypatch,epoch):
    module=actor()
    monkeypatch.setattr(module,"_orm",lambda:pytest.fail("malformed c1 opened ORM"))
    with pytest.raises(module.NativeActorUnavailable):
        module.read_native_origin_inputs("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            source_epoch=epoch,maximum_metadata_bytes=8*1024*1024)


@pytest.mark.integration
@pytest.mark.parametrize("fault",["missing_script","raw_repo_only","input_script","RETURN_script","calcfunction_script",
    "missing_call","foreign_call","wrong_plan_input","nonfile_script","oversized_script","small_budget"])
def test_origin_port_requires_original_typed_graph_and_bounded_files(native_graph,fault):
    module=actor();f=native_graph
    calc=orm.CalcJobNode(computer=f["calc"].computer,process_type=association._CALC_PROCESS)
    if fault!="missing_call":
        parent=f["restart"]
        if fault=="foreign_call":parent=orm.WorkChainNode(process_type="foreign.workchain").store()
        calc.base.links.add_incoming(parent,LinkType.CALL_CALC,"other")
    for link in f["calc"].base.links.get_incoming(link_type=LinkType.INPUT_CALC).all():
        node=blob(f["raw_plan"]).store() if fault=="wrong_plan_input" and link.link_label=="plan" else link.node
        calc.base.links.add_incoming(node,LinkType.INPUT_CALC,link.link_label)
    if fault=="input_script":calc.base.links.add_incoming(f["script"],LinkType.INPUT_CALC,"native_submit_script")
    if fault=="raw_repo_only":
        calc.base.repository.put_object_from_filelike(io.BytesIO(b"#!/bin/bash\noriginal bytes\n"),"_aiidasubmit.sh")
    calc.store()
    if fault not in ("missing_script","raw_repo_only","input_script"):
        script=orm.Str("not a file") if fault=="nonfile_script" else blob(
            b"x"*65537 if fault=="oversized_script" else b"#!/bin/bash\noriginal bytes\n")
        creator=calc
        kind=LinkType.CREATE
        if fault=="calcfunction_script":creator=orm.CalcFunctionNode(process_type="foreign.function").store()
        if fault=="RETURN_script":
            creator=orm.WorkChainNode(process_type="foreign.workchain").store();kind=LinkType.RETURN
            script.store()
        script.base.links.add_incoming(creator,kind,"native_submit_script")
        if not script.is_stored:script.store()
    with pytest.raises(module.NativeActorUnavailable):
        module.read_native_origin_inputs(calc.uuid,
            source_epoch="c1-"+calc.uuid.replace("-","")+"-"+f["native"].uuid.replace("-","")+"-0",
            maximum_metadata_bytes=1 if fault=="small_budget" else 8*1024*1024)


@pytest.mark.integration
def test_origin_port_calcfunction_cannot_impersonate_calcjob(native_graph):
    module=actor();f=native_graph
    function=orm.CalcFunctionNode(process_type=association._CALC_PROCESS).store()
    epoch="c1-"+function.uuid.replace("-","")+"-"+f["native"].uuid.replace("-","")+"-0"
    with pytest.raises(module.NativeActorUnavailable,match="calcjob_unavailable"):
        module.read_native_origin_inputs(function.uuid,source_epoch=epoch,maximum_metadata_bytes=8*1024*1024)


@pytest.mark.integration
@pytest.mark.parametrize("parent_kind",["direct_plan","genuine_restart","workfunction_restart"])
def test_native_original_chain_requires_actual_workchain_ancestor(native_graph,parent_kind):
    module=actor();f=native_graph
    parent=f["run"] if parent_kind=="direct_plan" else f["restart"]
    if parent_kind=="workfunction_restart":
        parent=orm.WorkFunctionNode(process_type=association._RESTART_PROCESS)
        parent.base.links.add_incoming(f["run"],LinkType.CALL_WORK,"wrong_restart_class")
        for link in f["restart"].base.links.get_incoming(link_type=LinkType.INPUT_WORK).all():
            parent.base.links.add_incoming(link.node,LinkType.INPUT_WORK,link.link_label)
        parent.store()
        assert type(parent) is orm.WorkFunctionNode
    calc=orm.CalcJobNode(computer=f["calc"].computer,process_type=association._CALC_PROCESS)
    calc.base.links.add_incoming(parent,LinkType.CALL_CALC,"typed_chain")
    for link in f["calc"].base.links.get_incoming(link_type=LinkType.INPUT_CALC).all():
        calc.base.links.add_incoming(link.node,LinkType.INPUT_CALC,link.link_label)
    calc.set_option("submit_script_filename","_aiidasubmit.sh")
    calc.store();calc.set_remote_workdir("/srv/qcl-negf/jobs/typed-chain")
    script=blob(b"#!/bin/bash\n# original typed-chain script\n","_aiidasubmit.sh")
    script.base.links.add_incoming(calc,LinkType.CREATE,"native_submit_script");script.store()
    epoch="c1-"+calc.uuid.replace("-","")+"-"+f["native"].uuid.replace("-","")+"-0"
    for name in ("load_native_launch","read_native_execution","read_native_origin_inputs"):
        kwargs={"maximum_metadata_bytes":8*1024*1024}
        if name=="read_native_origin_inputs":kwargs["source_epoch"]=epoch
        if parent_kind=="workfunction_restart":
            with pytest.raises(module.NativeActorUnavailable):getattr(module,name)(calc.uuid,**kwargs)
        else:
            actual=getattr(module,name)(calc.uuid,**kwargs)
            if name=="load_native_launch":assert actual["run_uuid"]==f["run"].uuid
            elif name=="read_native_execution":assert actual==f["raw_native"]
            else:assert actual==(f["raw_native"],b"#!/bin/bash\n# original typed-chain script\n")


@pytest.mark.integration
@pytest.mark.parametrize("port",["load_native_launch","read_native_execution"])
@pytest.mark.parametrize("changed",["workdir","job_id"])
def test_existing_runtime_ports_reject_changed_selection(native_graph,monkeypatch,port,changed):
    module=actor();f=native_graph;load=orm.load_node
    def changed_selection(uuid):
        fresh=load(uuid)
        if changed=="workdir":fresh.set_remote_workdir("/srv/qcl-negf/jobs/changed")
        else:fresh.set_job_id("123")
        return fresh
    monkeypatch.setattr(orm,"load_node",changed_selection)
    with pytest.raises(module.NativeActorUnavailable,match="runtime_selection_changed"):
        getattr(module,port)(f["calc"].uuid,maximum_metadata_bytes=8*1024*1024)


@pytest.mark.integration
def test_exact_workdir_or_job_query_and_ambiguity(native_graph):
    module=actor();calc=native_graph["calc"];computer=calc.computer.uuid
    assert module.native_calcjob_uuid(computer_uuid=computer,remote_workdir=calc.get_remote_workdir(),maximum_metadata_bytes=8*1024*1024)==calc.uuid
    calc.set_job_id("321")
    assert module.native_calcjob_uuid(computer_uuid=computer,job_id="321",maximum_metadata_bytes=8*1024*1024)==calc.uuid
    duplicate=orm.CalcJobNode(computer=calc.computer,process_type=association._CALC_PROCESS).store()
    duplicate.set_job_id("321")
    with pytest.raises(module.NativeActorUnavailable):
        module.native_calcjob_uuid(computer_uuid=computer,job_id="321",maximum_metadata_bytes=8*1024*1024)


@pytest.mark.integration
@pytest.mark.parametrize("fault",["missing_script","foreign_script","raw_repo_only","extras","code","small_budget"])
def test_distinguishing_native_graph_failures(native_graph,fault):
    module=actor();f=native_graph;calc=f["calc"]
    if fault in ("missing_script","foreign_script","raw_repo_only"):
        # A second genuine CalcJob with immutable same inputs; never alter stored links.
        other=orm.CalcJobNode(computer=calc.computer,process_type=association._CALC_PROCESS)
        other.base.links.add_incoming(f["restart"],LinkType.CALL_CALC,"other")
        for link in calc.base.links.get_incoming(link_type=LinkType.INPUT_CALC).all():
            other.base.links.add_incoming(link.node,LinkType.INPUT_CALC,link.link_label)
        other.set_option("submit_script_filename","_aiidasubmit.sh")
        if fault=="foreign_script":other.base.links.add_incoming(f["script"],LinkType.INPUT_CALC,"native_submit_script")
        if fault=="raw_repo_only":other.base.repository.put_object_from_filelike(io.BytesIO(b"#!/bin/bash\n"),"_aiidasubmit.sh")
        other.store();other.set_remote_workdir(calc.get_remote_workdir())
        calc=other
    if fault=="extras":calc.base.extras.set("native_actor_verified",True);calc.set_remote_workdir("relative/untrusted")
    if fault=="code":calc.computer.scheduler_type="core.slurm"
    with pytest.raises(module.NativeActorUnavailable):
        module.load_native_launch(calc.uuid,maximum_metadata_bytes=1 if fault=="small_budget" else 8*1024*1024)


@pytest.mark.integration
def test_stored_inputs_cannot_be_rebound(native_graph):
    calc=native_graph["calc"]
    with pytest.raises(ValueError,match="already stored"):
        calc.base.links.add_incoming(blob(b"{}"),LinkType.INPUT_CALC,"source_graph")


@pytest.mark.integration
def test_current_computer_shell_customization_cannot_authorize_script(native_graph):
    module=actor();f=native_graph
    f["calc"].computer.set_prepend_text("echo unsafe")
    with pytest.raises(module.NativeActorUnavailable):
        module.load_native_launch(f["calc"].uuid,maximum_metadata_bytes=8*1024*1024)


@pytest.mark.integration
def test_native_presubmit_genuine_create_original_script_and_repeat(native_graph,monkeypatch):
    f=native_graph
    # Source fixture factory seam only. Native wheel entry-point installation
    # remains a separate Root check; actual presubmit/ORM APIs are exercised.
    monkeypatch.setattr(orm.Computer,"get_scheduler",lambda self:SlurmScheduler())
    monkeypatch.setenv("QCL_NEGF_NATIVE_JOBS_ROOT","/srv/qcl-negf/jobs")
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=f['restart'].pk,inputs=dict(
        code=f["code"],plan=f["plan"],source_graph=f["graph"],native_execution=f["native"],
        execution_id=f["calc"].inputs.execution_id,attempt=f["calc"].inputs.attempt,
        release_id=f["calc"].inputs.release_id,
        metadata={"options":scheduler_options({})}))
    try:
        with SandboxFolder() as folder:
            info=process.presubmit(folder)
            original=Path(folder.abspath,"_aiidasubmit.sh").read_bytes()
            script=process.node.outputs.native_submit_script
            assert script.creator.uuid==process.node.uuid
            with script.open(mode="rb") as stream:assert stream.read()==original
            assert "--attempt" not in info.codes_info[0].cmdline_params
            assert info.codes_info[0].cmdline_params[2]=="/srv/qcl-negf/jobs/native/new-domain"
            expected=["run-plan","plan.json","/srv/qcl-negf/jobs/native/new-domain",
                "--execution-id",f["execution"]["execution_id"],"--source-graph","source-graph.json",
                "--native-execution","native-execution.json","--native-calcjob-uuid",process.node.uuid]
            assert info.codes_info[0].cmdline_params==expected
            command=next(line for line in original.decode().splitlines() if "run-plan" in line)
            words=shlex.split(command)
            assert words[0]==str(f["code"].filepath_executable) and words[1:12]==expected
            from aiida_qcl_negf.model_help import HELP_RETRIEVE
            assert tuple(info.retrieve_list)==("series_result.json","native-selection.json","native-close.json","performance-source.json",HELP_RETRIEVE,"_scheduler-stdout.txt","_scheduler-stderr.txt")
            assert actor()._help_retrieval(info.retrieve_list)
        with SandboxFolder() as second:
            process.presubmit(second)
            assert process.node.outputs.native_submit_script.uuid==script.uuid
    finally:
        process.close()


@pytest.mark.integration
def test_native_presubmit_store_primary_failure_precedes_any_submit(native_graph,monkeypatch):
    f=native_graph
    monkeypatch.setattr(orm.Computer,"get_scheduler",lambda self:SlurmScheduler())
    monkeypatch.setenv("QCL_NEGF_NATIVE_JOBS_ROOT","/srv/qcl-negf/jobs")
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=f['restart'].pk,inputs=dict(
        code=f["code"],plan=f["plan"],source_graph=f["graph"],native_execution=f["native"],
        execution_id=f["calc"].inputs.execution_id,attempt=f["calc"].inputs.attempt,
        release_id=f["calc"].inputs.release_id,metadata={"options":scheduler_options({})}))
    original=OSError("synthetic CREATE storage failure")
    monkeypatch.setattr(process,"update_outputs",lambda:(_ for _ in ()).throw(original))
    try:
        with SandboxFolder() as folder,pytest.raises(OSError) as caught:process.presubmit(folder)
        assert caught.value is original
        assert process.node.get_job_id() is None
    finally:process.close()


@pytest.mark.integration
def test_direct_native_restart_cannot_enable_legacy_retry_owner(native_graph):
    f=native_graph
    process=instantiate_process(get_manager().get_runner(),QCLExecutionRestartWorkChain,
        code=f["code"],plan=f["plan"],source_graph=f["graph"],native_execution=f["native"],
        execution_id=orm.Str(f["execution"]["execution_id"]),attempt=orm.Int(1),
        release_id=orm.Str(f["execution"]["code"]["release_id"]),max_iterations=orm.Int(2),
        options=orm.Dict(dict=scheduler_options({})))
    try:
        result=process.setup()
        assert result is not None,"native restart must reject legacy retries before any child"
        assert result.status==412
    finally:process.close()


@pytest.mark.integration
def test_native_service_reuses_explicit_original_plan_node_without_uuid_guess(native_graph,monkeypatch):
    f=native_graph;processes=[]
    for key in ("QCL_NEGF_RELEASE_ID","QCL_NEGF_SOLVER_EXECUTABLE","QCL_NEGF_RELEASE_GATE"):
        monkeypatch.delenv(key,raising=False)
    monkeypatch.setattr(service,"get_daemon_client",lambda:SimpleNamespace(is_daemon_running=True))
    def enqueue(cls,**inputs):
        process=instantiate_process(get_manager().get_runner(),cls,**inputs)
        processes.append(process)
        return process.node
    monkeypatch.setattr(service,"submit",enqueue)
    try:
        result=service.submit_plan(f["raw_plan"],f["code"].uuid,{},max_attempts=1,
            release_id=f["execution"]["code"]["release_id"],source_graph=f["graph"],
            native_executions={f["execution"]["execution_id"]:f["native"]})
        assert result["uuid"]==processes[0].node.uuid
        assert processes[0].inputs.plan.uuid==f["plan"].uuid
        assert processes[0].inputs.native_executions.execution_0.uuid==f["native"].uuid
        assert processes[0].setup() is None
    finally:
        for process in processes:process.close()


@pytest.mark.integration
def test_default_legacy_plan_does_not_opt_into_empty_native_namespace(native_graph):
    f=native_graph
    process=instantiate_process(get_manager().get_runner(),QCLPlanWorkChain,
        code=f["code"],plan=f["plan"],resources=orm.Dict(dict={}))
    try:
        assert process.setup() is None
        assert process.inputs.max_attempts.value==3
        assert process.ctx.pending==[f["execution"]["execution_id"]]
    finally:process.close()


@pytest.mark.integration
@pytest.mark.parametrize("selector",[None,"relative","/","/srv//jobs","/srv/jobs/","/srv/../jobs","/srv\\jobs"])
def test_native_output_selector_unavailable_has_no_legacy_fallback(native_graph,monkeypatch,selector):
    f=native_graph
    monkeypatch.setattr(orm.Computer,"get_scheduler",lambda self:SlurmScheduler())
    if selector is None:monkeypatch.delenv("QCL_NEGF_NATIVE_JOBS_ROOT",raising=False)
    else:monkeypatch.setenv("QCL_NEGF_NATIVE_JOBS_ROOT",selector)
    process=instantiate_process(get_manager().get_runner(),QCLExecutionCalculation,
        code=f["code"],plan=f["plan"],source_graph=f["graph"],native_execution=f["native"],
        execution_id=orm.Str(f["execution"]["execution_id"]),
        release_id=orm.Str(f["execution"]["code"]["release_id"]),metadata={"options":scheduler_options({})})
    try:
        with SandboxFolder() as folder,pytest.raises(actor().NativeActorUnavailable,match="native_output_mapping_unavailable"):
            process.prepare_for_submission(folder)
        assert process.node.get_job_id() is None
    finally:process.close()


@pytest.mark.integration
@pytest.mark.parametrize("fault",["foreign_node","nonfile","length","hash","candidate_bytes"])
def test_explicit_original_plan_full_reference_negatives(native_graph,fault):
    module=actor();f=native_graph;declaration=deepcopy(f["execution"])
    candidate=f["plan"]
    if fault=="foreign_node":declaration["input_plan"]["node_uuid"]=blob(b"{}").store().uuid
    if fault=="nonfile":declaration["input_plan"]["node_uuid"]=orm.Str("untrusted").store().uuid
    if fault=="length":declaration["input_plan"]["bytes"]=str(len(f["raw_plan"])+1)
    if fault=="hash":declaration["input_plan"]["sha256"]="0"*64
    if fault=="candidate_bytes":candidate=blob(f["raw_plan"]+b" ").store()
    declared=blob(encode(declaration)).store()
    with pytest.raises(module.NativeActorUnavailable):
        module._original_plan(candidate,{declaration["execution_id"]:declared},module._Reads(module.MAX_METADATA))


@pytest.mark.integration
@pytest.mark.parametrize("fault",["unbound_graph","unbound_execution","missing_pair","release","environment","resources","output_name","legacy_recovery"])
def test_native_prepare_rejects_direct_input_policy_drift(native_graph,monkeypatch,fault):
    f=native_graph
    monkeypatch.setattr(orm.Computer,"get_scheduler",lambda self:SlurmScheduler())
    monkeypatch.setenv("QCL_NEGF_NATIVE_JOBS_ROOT","/srv/qcl-negf/jobs")
    inputs=dict(code=f["code"],plan=f["plan"],source_graph=f["graph"],native_execution=f["native"],
        execution_id=orm.Str(f["execution"]["execution_id"]),
        release_id=orm.Str(f["execution"]["code"]["release_id"]),metadata={"options":scheduler_options({})})
    if fault=="unbound_graph":inputs["source_graph"]=blob(f["raw_graph"]).store()
    if fault=="unbound_execution":inputs["native_execution"]=blob(f["raw_native"]).store()
    if fault=="missing_pair":del inputs["native_execution"]
    if fault=="release":inputs["release_id"]=orm.Str("foreign-release")
    if fault=="environment":inputs["metadata"]["options"]["environment_variables"]["LD_PRELOAD"]="/tmp/foreign.so"
    if fault=="resources":inputs["metadata"]["options"]["resources"]["num_machines"]=2
    if fault=="output_name":inputs["metadata"]["options"]["output_filename"]="foreign.stdout"
    if fault=="legacy_recovery":inputs["recovery_bundle"]=orm.FolderData().store()
    process=instantiate_process(get_manager().get_runner(),QCLExecutionCalculation,**inputs)
    try:
        with SandboxFolder() as folder,pytest.raises(actor().NativeActorUnavailable):
            process.prepare_for_submission(folder)
        assert process.node.get_job_id() is None
    finally:process.close()


@pytest.mark.integration
def test_actual_upload_wraps_primary_presubmit_failure_without_remote_effects(native_graph,monkeypatch):
    import asyncio
    from contextlib import asynccontextmanager
    from aiida.engine.processes.calcjobs import tasks
    f=native_graph
    monkeypatch.setattr(orm.Computer,"get_scheduler",lambda self:SlurmScheduler())
    monkeypatch.setenv("QCL_NEGF_NATIVE_JOBS_ROOT","/srv/qcl-negf/jobs")
    process=instantiate_process(get_manager().get_runner(),QCLExecutionCalculation,
        code=f["code"],plan=f["plan"],source_graph=f["graph"],native_execution=f["native"],
        execution_id=orm.Str(f["execution"]["execution_id"]),
        release_id=orm.Str(f["execution"]["code"]["release_id"]),metadata={"options":scheduler_options({})})
    primary=OSError("original bounded synthetic store failure");effects=[]
    monkeypatch.setattr(process,"update_outputs",lambda:(_ for _ in ()).throw(primary))
    async def forbidden_upload(*args,**kwargs):effects.append("upload");raise AssertionError("remote upload")
    monkeypatch.setattr(tasks.execmanager,"upload_calculation",forbidden_upload)
    class Queue:
        @asynccontextmanager
        async def request_transport(self,authinfo):
            future=asyncio.get_running_loop().create_future();future.set_result(object())
            yield future
    class Cancellable:
        async def with_interrupt(self,future):return await future
    try:
        with pytest.raises(tasks.PreSubmitException) as caught:
            asyncio.run(tasks.task_upload_job(process,Queue(),Cancellable()))
        assert caught.value.__cause__ is primary
        assert effects==[] and process.node.get_job_id() is None
    finally:process.close()


@pytest.mark.integration
def test_native_ingress_requires_stored_original_execution_leaf(native_graph):
    module=actor();f=native_graph
    transient=blob(b"{}")
    declaration=deepcopy(f["execution"])
    declaration["storage"]["execution_binding_uuid"]=transient.uuid
    transient.set_file(io.BytesIO(encode(declaration)),filename="native-execution.json")
    with pytest.raises(module.NativeActorUnavailable,match="stored_original_inputs_required"):
        module._ingress(f["plan"],f["graph"],{declaration["execution_id"]:transient},f["code"],1)


def test_native_ingress_rejects_oversized_mapping_before_key_materialization():
    module=actor()
    # Exact dict only is already required; a large actual dict is bounded by
    # cardinality before schema/ORM/file reads, not by a fresh callback window.
    with pytest.raises(module.NativeActorUnavailable,match="native_execution_coverage"):
        module._ingress(None,object(),{str(i):None for i in range(257)},None,1)


@pytest.mark.integration
def test_native_presubmit_conflicting_created_script_is_never_replaced(native_graph,monkeypatch):
    f=native_graph
    monkeypatch.setattr(orm.Computer,"get_scheduler",lambda self:SlurmScheduler())
    monkeypatch.setenv("QCL_NEGF_NATIVE_JOBS_ROOT","/srv/qcl-negf/jobs")
    process=instantiate_process(get_manager().get_runner(),QCLExecutionCalculation,
        code=f["code"],plan=f["plan"],source_graph=f["graph"],native_execution=f["native"],
        execution_id=orm.Str(f["execution"]["execution_id"]),
        release_id=orm.Str(f["execution"]["code"]["release_id"]),metadata={"options":scheduler_options({})})
    original=blob(b"#!/bin/bash\n# conflicting genuine CREATE\n","_aiidasubmit.sh")
    original.base.links.add_incoming(process.node,LinkType.CREATE,"native_submit_script")
    original.store()
    try:
        with SandboxFolder() as folder,pytest.raises(actor().NativeActorUnavailable,match="submit_script_conflict"):
            process.presubmit(folder)
        assert process.node.outputs.native_submit_script.uuid==original.uuid
        with original.open(mode="rb") as stream:assert stream.read()==b"#!/bin/bash\n# conflicting genuine CREATE\n"
        assert process.node.get_job_id() is None
    finally:process.close()


def metadata_graph(f, raw_plan, *, raw_graph=None, script_raw=b'#!/bin/bash\n# original metadata script\n'):
    """Build fresh actual typed originals; never rebind stored fixture INPUTs."""
    from qcl_negf_contracts.messages import decode
    raw_graph=f['raw_graph'] if raw_graph is None else raw_graph
    plan,graph=blob(raw_plan).store(),blob(raw_graph).store()
    native=blob(b'{}');execution=deepcopy(f['execution'])
    execution['execution_id']=decode(raw_plan,maximum=1048576)['executions'][0]['id']
    execution['input_plan']=digest(plan,raw_plan);execution['source_graph']=digest(graph,raw_graph)
    execution['storage']['execution_binding_uuid']=native.uuid
    raw_native=encode(execution);native.set_file(io.BytesIO(raw_native),filename='native-execution.json');native.store()
    release=orm.Str(execution['code']['release_id']).store()
    run=orm.WorkChainNode(process_type=association._PLAN_PROCESS)
    for key,node in (('plan',plan),('source_graph',graph),('code',f['code']),('native_executions__execution_0',native),('release_id',release)):
        run.base.links.add_incoming(node,LinkType.INPUT_WORK,key)
    run.store()
    restart=orm.WorkChainNode(process_type=association._RESTART_PROCESS)
    restart.base.links.add_incoming(run,LinkType.CALL_WORK,'metadata')
    inputs=dict(plan=plan,source_graph=graph,code=f['code'],native_execution=native,
        attempt=orm.Int(7).store(),execution_id=orm.Str(execution['execution_id']).store(),release_id=release)
    for key,node in inputs.items():restart.base.links.add_incoming(node,LinkType.INPUT_WORK,key)
    restart.store()
    calc=orm.CalcJobNode(computer=f['calc'].computer,process_type=association._CALC_PROCESS)
    calc.base.links.add_incoming(restart,LinkType.CALL_CALC,'metadata')
    for key,node in inputs.items():calc.base.links.add_incoming(node,LinkType.INPUT_CALC,key)
    calc.set_option('submit_script_filename','_aiidasubmit.sh')
    for key,value in scheduler_options({}).items():calc.set_option(key,value)
    calc.store();calc.set_remote_workdir('/srv/qcl-negf/jobs/upload/metadata')
    script=blob(script_raw,'_aiidasubmit.sh');script.base.links.add_incoming(calc,LinkType.CREATE,'native_submit_script');script.store()
    return dict(calc=calc,run=run,restart=restart,plan=plan,graph=graph,native=native,code=f['code'],script=script,
        execution=execution,raw_native=raw_native,raw_plan=raw_plan,raw_graph=raw_graph,raw_script=script_raw)


@pytest.mark.integration
@pytest.mark.parametrize('role,size,reason',[
    ('plan',113111,None),('plan',1048575,'metadata_budget'),('plan',1048576,'metadata_budget'),('plan',1048577,'metadata_budget'),
    *[(role,size,None if size<=65536 else 'input_file_bound') for role in
      ('source_graph','native_execution','submit_script') for size in (65535,65536,65537)]])
def test_original_role_ceiling_and_aggregate_are_distinct(aiida_profile_clean,role,size,reason):
    module=actor();raw=b'{}'+b' '*(size-2);node=blob(raw).store();budget=module._Reads(8388608)
    if reason:
        with pytest.raises(module.NativeActorUnavailable,match=reason):budget.file(node,role=role)
        assert node.uuid not in budget.cache
        if reason=='metadata_budget':assert budget.remaining<0
    else:
        assert budget.file(node,role=role)==raw
        assert budget.remaining>=0
    with node.open(mode='rb') as stream:assert stream.read()==raw


@pytest.mark.integration
@pytest.mark.parametrize('role',['metadata','source_graph','native_execution','submit_script'])
def test_cached_plan_cannot_bypass_stricter_original_role(aiida_profile_clean,monkeypatch,role):
    module=actor();raw=b'{}'+b' '*113109;node=blob(raw).store();budget=module._Reads(8388608)
    assert budget.file(node,role='plan')==raw
    after=budget.remaining
    def forbidden(*args,**kw):raise AssertionError('cached immutable original reopened')
    monkeypatch.setattr(orm.SinglefileData,'open',forbidden)
    with pytest.raises(module.NativeActorUnavailable,match='input_file_bound'):budget.file(node,role=role)
    assert budget.file(node,role='plan')==raw and budget.remaining==after


@pytest.mark.integration
@pytest.mark.parametrize('fault',['budget','open_failure','cancellation'])
def test_original_read_workspace_prepaid_before_open_and_never_refunded(aiida_profile_clean,monkeypatch,fault):
    module=actor();node=blob(b'{}'+b' '*10000).store();opened=[];real_open=orm.SinglefileData.open
    primary=OSError('original open failure')
    def opening(self,*args,**kw):
        opened.append(budget.remaining)
        if fault=='open_failure':raise primary
        from contextlib import contextmanager
        @contextmanager
        def tracked():
            with real_open(self,*args,**kw) as stream:
                streams.append(stream)
                yield stream
        return tracked()
    class Cancelled(BaseException):pass
    count=[0];streams=[]
    def before():
        count[0]+=1
        if fault=='cancellation' and count[0]==2:raise Cancelled()
    budget=module._Reads(32767 if fault=='budget' else 8388608,before=before)
    monkeypatch.setattr(orm.SinglefileData,'open',opening)
    if fault=='budget':
        with pytest.raises(module.NativeActorUnavailable,match='metadata_budget'):budget.file(node,role='plan')
        assert opened==[] and budget.remaining==-1
    elif fault=='open_failure':
        with pytest.raises(OSError) as caught:budget.file(node,role='plan')
        assert caught.value is primary and opened==[8355840] and budget.remaining==8355840
    else:
        with pytest.raises(Cancelled):budget.file(node,role='plan')
        assert opened==[8355840] and budget.remaining==8355840
        assert streams and all(stream.closed for stream in streams)
    assert node.uuid not in budget.cache


@pytest.mark.integration
def test_large_plan_ingress_explicit_original_and_public_ports_keep_full_bytes(native_graph):
    module=actor();raw=native_graph['raw_plan']+b' '*(113111-len(native_graph['raw_plan']))
    f=metadata_graph(native_graph,raw);execution=f['execution']['execution_id']
    budget=module._Reads(8388608)
    assert module._original_plan(f['plan'],{execution:f['native']},budget).uuid==f['plan'].uuid
    assert module._ingress(f['plan'],f['graph'],{execution:f['native']},f['code'],1,budget=budget)=={'execution_0':f['native']}
    launch=module.load_native_launch(f['calc'].uuid,maximum_metadata_bytes=8388608)
    assert launch['files']['plan']==dict(bytes=113111,sha256=hashlib.sha256(raw).hexdigest())
    assert module.read_native_execution(f['calc'].uuid,maximum_metadata_bytes=8388608)==f['raw_native']
    assert module.read_native_origin_inputs(f['calc'].uuid,source_epoch=origin_epoch(f),maximum_metadata_bytes=8388608)==(f['raw_native'],f['raw_script'])
    with f['plan'].open(mode='rb') as stream:assert stream.read()==raw


def test_unknown_input_role_refuses_before_orm(monkeypatch):
    module=actor()
    monkeypatch.setattr(module,"_orm",lambda:pytest.fail("unknown role opened ORM"))
    with pytest.raises(module.NativeActorUnavailable,match="input_file_role"):
        module._Reads(8388608).file(None,role="unknown")


@pytest.mark.integration
@pytest.mark.parametrize('primary_type',[KeyboardInterrupt,SystemExit])
def test_T4_original_four_file_cancel_survives_secondary_context_close(native_graph,monkeypatch,primary_type):
    from contextlib import contextmanager
    module=actor();primary=primary_type('original four cancellation')
    @contextmanager
    def failed_open(self,*a,**k):
        class Stream:
            def read(self,size):raise primary
        try:yield Stream()
        finally:raise OSError('original input secondary close')
    monkeypatch.setattr(orm.SinglefileData,'open',failed_open)
    with pytest.raises(primary_type) as caught:module._Reads(16<<20).file(native_graph['plan'],role='plan')
    assert caught.value is primary and any('original input repository close' in note for note in primary.__notes__)


@pytest.mark.integration
@pytest.mark.parametrize('signal_name',['KillInterruption','PauseInterruption'])
@pytest.mark.parametrize('port',['load_native_launch','read_native_execution','read_native_origin_inputs','read_native_performance_inputs','native_calcjob_uuid'])
def test_T4_repair_public_actor_preserves_true_process_signal(native_graph,monkeypatch,signal_name,port):
    from contextlib import contextmanager
    from plumpy import process_states
    module=actor();f=native_graph;primary=getattr(process_states,signal_name)('true actor signal');hits=[]
    @contextmanager
    def failed_open(node,*a,**k):
        class Stream:
            def read(self,size):hits.append('read');raise primary
        try:yield Stream()
        finally:hits.append('secondary_close');raise OSError('actor secondary close')
    monkeypatch.setattr(orm.SinglefileData,'open',failed_open)
    if port=='native_calcjob_uuid':
        def lookup(*a,**k):hits.append('lookup');raise primary
        monkeypatch.setattr(module,'_lookup',lookup)
        call=lambda:getattr(module,port)(computer_uuid=f['calc'].computer.uuid,job_id='321',maximum_metadata_bytes=16<<20)
    else:
        args=dict(maximum_metadata_bytes=16<<20)
        if port=='read_native_origin_inputs':args['source_epoch']='c1-'+f['calc'].uuid.replace('-','')+'-'+f['native'].uuid.replace('-','')+'-1'
        call=lambda:getattr(module,port)(f['calc'].uuid,**args)
    with pytest.raises(type(primary)) as caught:call()
    assert caught.value is primary
    assert hits==(['lookup'] if port=='native_calcjob_uuid' else ['read','secondary_close'])
