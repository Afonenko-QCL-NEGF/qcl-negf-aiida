"""A-Q1 SOURCE/L; isolated ORM/protocol/RO observation fixtures, never native qualification."""
import inspect
from aiida_qcl_negf import native_backend, native_actor

def test_A01_public_selected_access_port():
    function = getattr(native_backend, "open_native_selected_sources", None)
    assert callable(function), "missing actual selected-source context manager"
    assert tuple(inspect.signature(function).parameters) == ("calcjob_uuid", "maximum_metadata_bytes", "maximum_io_bytes")

def test_A01_exact_five_controls():
    assert native_actor.CONTROL_FILES == ("series_result.json", "native-selection.json", "native-close.json", "_scheduler-stdout.txt", "_scheduler-stderr.txt")

import asyncio
from copy import deepcopy
import hashlib
import io
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiida import orm
from aiida.common.links import LinkType
from aiida.engine.daemon.execmanager import retrieve_calculation
from plumpy.process_states import PauseInterruption
from qcl_negf_contracts.messages import encode, decode
from test_native_actor import native_graph
from test_native_backend import reply
from test_native_binding import fixture


def canonical(value):
    """Independent fixed Platform encoder oracle, not the adapter encoder."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def claim(name, raw):
    return dict(path=name, bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def _execution_graph(f, identifier):
    """Fresh actual typed graph; never rewrite stored original input links."""
    def retarget(value):
        if isinstance(value,dict):return {k:retarget(v) for k,v in value.items()}
        if isinstance(value,list):return [retarget(v) for v in value]
        return identifier if value=="execution" else value
    def blob(raw):return orm.SinglefileData(file=io.BytesIO(raw),filename="original.json")
    raw_plan=encode(retarget(decode(f["raw_plan"])));plan=blob(raw_plan).store()
    native=blob(b"{}");declaration=deepcopy(f["execution"])
    declaration["execution_id"]=identifier
    declaration["input_plan"]=dict(node_uuid=plan.uuid,bytes=str(len(raw_plan)),sha256=hashlib.sha256(raw_plan).hexdigest())
    declaration["storage"]["execution_binding_uuid"]=native.uuid
    raw_native=encode(declaration);native.set_file(io.BytesIO(raw_native),filename="native-execution.json");native.store()
    release=f["calc"].inputs.release_id;attempt=orm.Int(7).store();execution=orm.Str(identifier).store()
    run=orm.WorkChainNode(process_type=f["run"].process_type)
    for name,node in (("plan",plan),("source_graph",f["graph"]),("code",f["code"]),("native_executions__execution_0",native),("release_id",release)):
        run.base.links.add_incoming(node,LinkType.INPUT_WORK,name)
    run.store()
    calc=orm.CalcJobNode(computer=f["calc"].computer,process_type=f["calc"].process_type)
    calc.base.links.add_incoming(run,LinkType.CALL_CALC,"selected")
    for name,node in (("plan",plan),("source_graph",f["graph"]),("code",f["code"]),("native_execution",native),("attempt",attempt),("execution_id",execution),("release_id",release)):
        calc.base.links.add_incoming(node,LinkType.INPUT_CALC,name)
    for key,value in f["calc"].get_options().items():calc.set_option(key,value)
    calc.store();script=blob(b"#!/bin/bash\n# synthetic original script\n")
    script.base.links.add_incoming(calc,LinkType.CREATE,"native_submit_script");script.store()
    return dict(f,calc=calc,run=run,plan=plan,native=native,raw_plan=raw_plan,raw_native=raw_native,execution=declaration)


@pytest.fixture
def packet(native_graph, tmp_path, monkeypatch, request):
    f=_execution_graph(native_graph,"exec") if getattr(request,"param",None)=="native5" else native_graph
    calc=f["calc"];calc.set_job_id("321")
    jobs=tmp_path/"jobs";root=jobs/"native/new-domain";work=jobs/"upload/unique"
    root.mkdir(parents=True);work.mkdir(parents=True)
    calc.set_remote_workdir(str(work))
    monkeypatch.setenv("QCL_NEGF_NATIVE_JOBS_ROOT",str(jobs))
    # Explicit local RO observation simulation; actual W1/NFS/unit/caller are N.
    monkeypatch.setattr(native_backend,"_namespace",lambda root,budget:("SIMULATED_RO_NOT_NATIVE",))
    raw={"series_result.json":b" \n"+fixture("absent")["raw_result"]+b"\n",
         "native-selection.json":b' {"version":1,"selected_inputs":[]}\n',
         "_scheduler-stdout.txt":b"stock output\n", "_scheduler-stderr.txt":b""}
    calls=[];transports=[]
    def expected():
        value=reply(calc.uuid,"closed")
        value.update(receipt_ref=calc.uuid+"/close.json",controls=[claim(k,raw[k]) for k in ("series_result.json","native-selection.json","_scheduler-stdout.txt","_scheduler-stderr.txt")])
        value["job"].update(node_name="worker",start_time="2026-10-29T00:00:00",worker_machine_id="c"*32,worker_boot_id="00000000-0000-4000-8000-000000000004")
        return canonical(value)
    class Protocol(native_backend.NativeLocalTransport):
        def exec_command_wait_bytes(self,command,stdin=None,workdir=None,**kwargs):
            request=decode(stdin);calls.append(request)
            assert command=="/run/current-system/sw/bin/qcl-negf-native-attempt-client close"
            assert request["action"]=="close" and request["calcjob_uuid"]==calc.uuid
            assert request["controls"]==decode(expected())["controls"]
            # Retain the production adapter command gross debit in this protocol
            # fixture; no real privileged client, Slurm or shell is executed.
            self._native_operation.before(2*native_backend._OUTPUT_BYTES)
            return 0,expected()+b"\n",b""
    def transport():
        t=Protocol(machine="localhost",native_computer_uuid=calc.computer.uuid);transports.append(t);return t
    monkeypatch.setattr(orm.AuthInfo,"get_transport",lambda self:transport())
    def write():
        for name,data in raw.items():(root if name in ("series_result.json","native-selection.json") else work).joinpath(name).write_bytes(data)
    write()
    def stored(extra=None, kind=LinkType.CREATE, creator=None):
        node=orm.FolderData()
        for name,data in dict(raw,**{"native-close.json":expected()}).items():
            node.base.repository.put_object_from_filelike(io.BytesIO(data),name)
        if extra:node.base.repository.put_object_from_filelike(io.BytesIO(b"unexpected"),extra)
        node.base.links.add_incoming(creator or calc,kind,"retrieved");node.store();return node
    return SimpleNamespace(f=f,calc=calc,root=root,work=work,jobs=jobs,raw=raw,calls=calls,
        transports=transports,transport=transport,stored=stored,expected=expected,write=write)


def test_A02_closed_reply_literal_claims_and_strict_job():
    identifier="00000000-0000-4000-8000-000000000003"
    claims=[claim(name,b"") for name in ("series_result.json","native-selection.json","_scheduler-stdout.txt","_scheduler-stderr.txt")]
    value=reply(identifier,"closed");value.update(receipt_ref=identifier+"/close.json",controls=claims)
    value["job"].update(node_name="worker",start_time="2026-10-29T00:00:00",worker_machine_id="c"*32,worker_boot_id="00000000-0000-4000-8000-000000000004")
    assert native_backend._reply(canonical(value),identifier,state="closed",controls=claims,job_id="321")==value["job"]
    for field,bad in (("receipt_ref",identifier+"/claim.json"),("state","started"),("version",True)):
        changed=deepcopy(value);changed[field]=bad
        with pytest.raises(PauseInterruption):native_backend._reply(canonical(changed),identifier,state="closed",controls=claims,job_id="321")


@pytest.mark.parametrize("fault",["bool","string","order","extra","duplicate","selfhash"])
def test_A02_four_claim_operational_types(fault):
    claims=[claim(name,b"") for name in ("series_result.json","native-selection.json","_scheduler-stdout.txt","_scheduler-stderr.txt")]
    if fault=="bool":claims[0]["bytes"]=False
    elif fault=="string":claims[0]["bytes"]="0"
    elif fault=="order":claims.reverse()
    elif fault=="extra":claims[0]["verified"]=True
    elif fault=="duplicate":claims[1]=deepcopy(claims[0])
    else:claims.append(claim("native-close.json",b"x"))
    with pytest.raises(PauseInterruption):native_backend._claims(claims)


@pytest.mark.integration
def test_A03_actual_create_retrieved_and_raw_whitespace(packet):
    p=packet;p.stored()
    with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024) as (raw,sources,telemetry):
        assert raw==p.raw["series_result.json"] and raw.startswith(b" \n") and raw.endswith(b"\n")
        assert sources==() and telemetry is None
    assert len(p.calls)==1 and all(x["action"]=="close" for x in p.calls)
    assert not p.transports[-1].is_open


@pytest.mark.integration
def test_A04_stock_async_retrieve_exact_five_and_one_session(packet,tmp_path):
    p=packet;p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES));t=p.transport()
    result=asyncio.run(retrieve_calculation(p.calc,t,str(tmp_path/"temporary")))
    assert set(result.base.repository.list_object_names())==set(native_actor.CONTROL_FILES)
    with result.base.repository.open("native-close.json",mode="rb") as stream:assert stream.read()==p.expected()
    with result.base.repository.open("series_result.json",mode="rb") as stream:assert stream.read()==p.raw["series_result.json"]
    assert len(p.calls)==1 and not t.is_open and t._q1_budget is None
    assert not (p.work/"series_result.json").exists() and not (p.work/"native-close.json").exists()


@pytest.mark.integration
@pytest.mark.parametrize("fault",["missing_retrieved","extra_repository","wrong_creator","stale_raw"])
def test_A03_actual_retrieved_failure(packet,fault):
    p=packet
    if fault=="extra_repository":p.stored(extra="solver.stdout")
    elif fault=="wrong_creator":p.stored(creator=orm.CalcJobNode(computer=p.calc.computer).store())
    elif fault=="stale_raw":
        p.stored();p.raw["_scheduler-stdout.txt"]=b"changed\n";p.write()
    with pytest.raises((PauseInterruption,native_actor.NativeActorUnavailable)):
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024):pass


@pytest.mark.integration
@pytest.mark.parametrize("framing",[b"",b"\n\n",b"\nextra",b"\r\n"])
def test_A02_client_framing_never_materializes(packet,monkeypatch,tmp_path,framing):
    p=packet;t=p.transport();destination=tmp_path/"copy"
    monkeypatch.setattr(t,"exec_command_wait_bytes",lambda *a,**k:(0,p.expected()+framing,b""))
    with t,pytest.raises(PauseInterruption):t.get(str(p.work/"native-close.json"),str(destination),ignore_nonexisting=True)
    assert not destination.exists()


@pytest.mark.integration
@pytest.mark.parametrize("fault",["missing","symlink","hardlink","owner_mode","fifo","no_ro"])
def test_A06_whole_packet_before_any_transfer(packet,monkeypatch,tmp_path,fault):
    p=packet;path=p.root/"native-selection.json"
    if fault=="missing":path.unlink()
    elif fault=="symlink":path.unlink();path.symlink_to(p.root/"series_result.json")
    elif fault=="hardlink":os.link(path,p.root/"alias")
    elif fault=="owner_mode":path.chmod(0o666)
    elif fault=="fifo":path.unlink();os.mkfifo(path)
    else:monkeypatch.setattr(native_backend,"_namespace",lambda *a:(_ for _ in ()).throw(PauseInterruption("RO unavailable")))
    destination=tmp_path/"copy";t=p.transport()
    with t,pytest.raises((PauseInterruption,OSError)):t.get(str(p.work/"series_result.json"),str(destination),ignore_nonexisting=True)
    assert not destination.exists() and p.calls==[]


@pytest.mark.integration
def test_A09_get_session_repeats_share_debit_and_conflict_preserves(packet,tmp_path):
    p=packet;t=p.transport();destination=tmp_path/"copy"
    with t:
        t.get(str(p.work/"series_result.json"),str(destination))
        budget=t._q1_budget;before=budget.reads.remaining
        t.getfile(str(p.work/"series_result.json"),str(destination),overwrite=True)
        assert t._q1_budget is budget and budget.reads.remaining<before and len(p.calls)==1
        destination.write_bytes(b"foreign original")
        with pytest.raises(PauseInterruption):t.get(str(p.work/"series_result.json"),str(destination),overwrite=True)
        assert destination.read_bytes()==b"foreign original"
        with pytest.raises(PauseInterruption):t.get(str(p.work/"series_result.json"),str(tmp_path/"other"))
    assert not (tmp_path/"other").exists()


@pytest.mark.integration
def test_A09_budget_before_effect_and_cancel_primary(packet,monkeypatch):
    p=packet;p.stored()
    with pytest.raises(PauseInterruption):
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=1,maximum_io_bytes=1):pass
    assert not p.calls
    cancellation=KeyboardInterrupt("original cancellation")
    with pytest.raises(KeyboardInterrupt) as caught:
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024):
            monkeypatch.setattr(native_actor,"load_native_launch",lambda *a,**k:(_ for _ in ()).throw(OSError("readback cleanup failed")))
            raise cancellation
    assert caught.value is cancellation and any("close failed" in n for n in cancellation.__notes__)


@pytest.mark.parametrize("identifier,metadata,io_bytes",[("bad",1,1),("00000000-0000-4000-8000-000000000001",True,1),("00000000-0000-4000-8000-000000000001",1,False)])
def test_A01_bad_selectors_before_profile(identifier,metadata,io_bytes,monkeypatch):
    monkeypatch.setattr(native_actor,"_orm",lambda:(_ for _ in ()).throw(AssertionError("profile touched")))
    with pytest.raises((ValueError,PauseInterruption)):
        with native_backend.open_native_selected_sources(identifier,maximum_metadata_bytes=metadata,maximum_io_bytes=io_bytes):pass


def _literal_selection(root):
    value=decode(fixture()["raw_result"]);old=deepcopy(value["points"][0]);current=deepcopy(old)
    current["attempt"]=4;current["data"]["native_source"]["relation"]="retained_source"
    current["data"]["native_source"]["selected_state"]["state_id"]="distinct-same-sequence"
    current["data"]["native_source"]["selected_state"]["manifest"]["sha256"]="f"*64
    value["points"]=[current];value["attempt_history"]=[old,deepcopy(current)]
    inputs=[]
    for row in (current,old):
        source=row["data"]["native_source"];path=root/source["artifact_root"];path.mkdir(parents=True,exist_ok=True)
        plan=dict(path="inputs/original-plan.json",bytes="7",sha256="a"*64,schema="qcl-negf-scientific-plan-v2",media_type="application/json")
        model=dict(path="inputs/original-model.json",bytes="9",sha256="b"*64,schema="qcl-negf-resolved-configuration-v3",media_type="application/json")
        inputs.append(dict(artifact_root=source["artifact_root"],selected_state=deepcopy(source["selected_state"]),plan=plan,model=model))
    return value,dict(version=1,selected_inputs=inputs)


def _selection_packet(tmp_path,result,selection,monkeypatch):
    # Wire-order tests supply one explicit same-mount observation. Real mount
    # refusal and lifetime changes use the production observer in F1 below.
    namespace=("SIMULATED_RO_NOT_NATIVE",)
    monkeypatch.setattr(native_backend,"_namespace",lambda root,budget:namespace)
    return SimpleNamespace(root=str(tmp_path),namespace=namespace,namespaces={str(tmp_path):namespace},budget=native_backend._Operation(),trees=[],launch={"execution_id":"execution"},raw={"series_result.json":encode(result),"native-selection.json":encode(selection)})


def test_A05_literal_full_state_order_and_original_closed8(tmp_path,monkeypatch):
    result,selection=_literal_selection(tmp_path)
    packet=_selection_packet(tmp_path,result,selection,monkeypatch)
    try:
        sources=native_backend._selected(packet)
        assert [s[1]["selected_state"]["state_id"] for s in sources]==["distinct-same-sequence",result["attempt_history"][0]["data"]["native_source"]["selected_state"]["state_id"]]
        assert sources[0][1]==result["points"][0]["data"]["native_source"]
        assert sources[1][1]==result["attempt_history"][0]["data"]["native_source"]
        assert all(set(s[1])=={"schema","kind","artifact_root","relation","selected_state","diagnostics","observations","reason"} for s in sources)
    finally:
        for tree in packet.trees:tree.close()


@pytest.mark.parametrize("fault",["order","missing","extra","duplicate","sequence_only","escape","bool_version","foreign_execution"])
def test_A05_selection_join_refuses(tmp_path,fault,monkeypatch):
    result,selection=_literal_selection(tmp_path)
    rows=selection["selected_inputs"]
    if fault=="order":rows.reverse()
    elif fault=="missing":rows.pop()
    elif fault=="extra":rows.append(deepcopy(rows[0]));rows[-1]["artifact_root"]="foreign"
    elif fault=="duplicate":rows.append(deepcopy(rows[0]))
    elif fault=="sequence_only":rows[0]["selected_state"]["state_id"]="not-the-state"
    elif fault=="escape":rows[0]["artifact_root"]="../foreign"
    elif fault=="bool_version":selection["version"]=True
    else:
        def foreign(value):
            if isinstance(value,dict):return {k:foreign(v) for k,v in value.items()}
            if isinstance(value,list):return [foreign(v) for v in value]
            return "other" if value=="execution" else value
        result=foreign(result)  # valid outward format, wrong actual CalcJob scope
    packet=_selection_packet(tmp_path,result,selection,monkeypatch)
    try:
        with pytest.raises(PauseInterruption):native_backend._selected(packet)
    finally:
        for tree in packet.trees:tree.close()


@pytest.mark.integration
@pytest.mark.parametrize("packet",["native5"],indirect=True)
@pytest.mark.parametrize("retained",[False,True])
def test_A07_public_provider_actual_selected_checkpoint_and_final_bytes(packet,retained,record_property):
    """Actual tiny engineering bytes + ORM; simulated native owner/RO, no creator/science acceptance."""
    import shutil
    from qcl_negf_results.state import StateReader,ResultReadLimits
    from qcl_negf_results.final_observations import describe_checkpoint_inputs,FinalObservationsReader
    from qcl_negf_results.final_diagnostics import FinalDiagnosticsReader
    p=packet;location=os.environ.get("QCL_NEGF_NATIVE_FIXTURE_ROOT")
    if not location:pytest.skip("explicit prepared Root native5 engineering fixture required")
    original=Path(location);result=json.loads((original/"outward-v4.json").read_text())
    source=result["points"][0]["data"]["native_source"]
    manifest=json.loads((original/source["selected_state"]["manifest"]["path"]).read_text())
    refs={a["role"]:{k:a[k] for k in ("path","bytes","sha256","schema","media_type")} for a in manifest["artifacts"] if a["role"] in ("plan","model")}
    shutil.copytree(original/"transaction",p.root/"transaction")
    if retained:
        # Same actual byte-access guard regardless of relation. Original creator
        # is deliberately NOT certified by A (Root creator addendum377b4880).
        for row in result["points"]+result["attempt_history"]:
            row["attempt"]=2;row["data"]["native_source"].update(relation="retained_source",reason="last_committed_before_pause")
    p.raw["series_result.json"]=encode(result) if retained else (original/"outward-v4.json").read_bytes()
    p.raw["native-selection.json"]=encode(dict(version=1,selected_inputs=[dict(artifact_root=".",selected_state=source["selected_state"],plan=refs["plan"],model=refs["model"])]))
    p.write();p.stored()
    with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024) as (raw,sources,telemetry):
        assert raw==p.raw["series_result.json"] and telemetry is None and len(sources)==1
        root,closed,plan,model=sources[0];assert root==p.root and closed==source and "creator" not in closed
        record_property("A_metadata_remaining_before_consumer",p.transports[-1]._native_operation.reads.remaining)
        # B owns a separately prepaid outer Results allowance. No reader private
        # metadata/budget API is accessed and provider does not claim origin.
        with StateReader(root,state_ref=closed["selected_state"],expected_identity=closed["selected_state"]["identity"],limits=ResultReadLimits(maximum_metadata_bytes=16*1024*1024,maximum_io_bytes=2*1024*1024)) as reader:
            cp=describe_checkpoint_inputs(reader,plan_ref=plan,model_ref=model)
            assert hashlib.sha256(cp.plan_raw).hexdigest()==plan["sha256"]
            assert hashlib.sha256(cp.model_raw).hexdigest()==model["sha256"]
            assert FinalDiagnosticsReader(reader,artifact_ref=closed["diagnostics"]).describe().artifact==closed["diagnostics"]
            assert FinalObservationsReader(reader,artifact_ref=closed["observations"]).describe().artifact==closed["observations"]


@pytest.mark.integration
def test_A09_current_readback_generation_change_and_consumer_error(packet,monkeypatch):
    p=packet;p.stored();original=ValueError("consumer error must remain primary")
    with pytest.raises(ValueError) as caught:
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024):raise original
    assert caught.value is original
    with pytest.raises(PauseInterruption):
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024):
            monkeypatch.setattr(native_backend,"_namespace",lambda *a:("DIFFERENT_GENERATION",))


def test_A06_real_namespace_rw_source_holds(tmp_path):
    # Exercise the actual kernel observer: this fixture has no qualified RO NFS.
    with pytest.raises(PauseInterruption,match="namespace"):
        native_backend._namespace(str(tmp_path),native_backend._Operation())


def test_A05_original_final_representative_replaces_checkpoint_without_new_owner(tmp_path,monkeypatch):
    result,selection=_literal_selection(tmp_path)
    current=result["points"][0];previous=result["attempt_history"][0]
    current["data"]["native_source"]["selected_state"]=deepcopy(previous["data"]["native_source"]["selected_state"])
    final=previous["data"]["native_source"];final.update(kind="final",reason=None)
    final["diagnostics"]=dict(path="analysis/d.h5",bytes="1",sha256="d"*64,schema="qcl-negf-physics-analysis-v5",media_type="application/x-hdf5")
    final["observations"]=dict(path="analysis/o.h5",bytes="1",sha256="e"*64,schema="qcl-negf-physics-analysis-v5",media_type="application/x-hdf5")
    result["attempt_history"][1]=deepcopy(current)
    selection["selected_inputs"]=[selection["selected_inputs"][1]]
    packet=_selection_packet(tmp_path,result,selection,monkeypatch)
    try:
        sources=native_backend._selected(packet)
        assert len(sources)==1 and sources[0][1]==final
        assert "creator" not in sources[0][1] and sources[0][1]["relation"]=="current_point_attempt"
        assert result["points"][0]["data"]["native_source"]["relation"]=="retained_source"
    finally:
        for tree in packet.trees:tree.close()


def test_A08_missing_creator_proof_remains_required_after_access():
    from aiida_qcl_negf.science_association import bind_native_result
    from qcl_negf_contracts.messages import ContractError
    arguments=fixture(native=1,row_attempt=2)
    arguments["creator_proofs"]=()
    with pytest.raises(ContractError):bind_native_result(**arguments)


@pytest.mark.integration
def test_A09_all_public_actor_caps_are_prepaid_without_refund(packet,monkeypatch):
    p=packet;p.stored();observed=[]
    for name in ("load_native_launch","read_native_execution"):
        original=getattr(native_actor,name)
        def observed_port(*args,_original=original,_name=name,**kwargs):
            observed.append((_name,kwargs["maximum_metadata_bytes"]))
            return _original(*args,**kwargs)
        monkeypatch.setattr(native_actor,name,observed_port)
    maximum=8*1024*1024
    with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=maximum,maximum_io_bytes=2*1024*1024):
        budget=p.transports[-1]._native_operation
        assert len(observed)==2 and maximum-budget.reads.remaining>=sum(cap for _,cap in observed)
    assert [name for name,_ in observed]==["load_native_launch","read_native_execution","load_native_launch"]
    assert all(type(cap) is int and cap>0 for _,cap in observed)
    assert maximum-budget.reads.remaining>=sum(cap for _,cap in observed)
    assert budget.reads.remaining>0


# F1 literal OS observations: real observer, synthetic mountinfo only. No mounts
# are created and no RO/native/foreign-writer qualification is asserted.
_F1_REAL_NAMESPACE = native_backend._namespace


def _f1_mount_observer(packet, monkeypatch, extra=""):
    p=packet
    rows={"raw":(f"1 0 0:1 / {p.root} ro - nfs4 storage:/jobs rw\n"+extra).encode()}
    original=native_backend._proc_bytes
    def proc(path,budget,cap=65536):
        if path=="/proc/self/mountinfo":
            raw=rows["raw"];budget.before(8*len(raw));budget.io(len(raw))
            return raw
        return original(path,budget,cap)
    monkeypatch.setattr(native_backend,"_proc_bytes",proc)
    monkeypatch.setattr(native_backend,"_namespace",_F1_REAL_NAMESPACE)
    return rows


def _f1_nested_selection(packet):
    p=packet;result,selection=_literal_selection(p.root)
    for row in result["points"]+result["attempt_history"]:
        row["data"]["native_source"]["artifact_root"]="selected"
    for row in selection["selected_inputs"]:row["artifact_root"]="selected"
    (p.root/"selected/science").mkdir(parents=True)
    p.raw["series_result.json"]=encode(result)
    p.raw["native-selection.json"]=encode(selection)
    p.write();p.stored()


@pytest.mark.integration
@pytest.mark.parametrize("boundary,mode,source",[
    ("selected","rw","storage:/jobs"),
    ("selected","ro","foreign:/export"),
    ("selected/science","rw","storage:/jobs"),
    ("selected/science","ro","foreign:/export"),
    ("selected/science","ro","storage:/jobs"),
])
def test_F1_nested_mount_boundaries_never_inherit_publisher_access(packet,monkeypatch,boundary,mode,source):
    p=packet;_f1_nested_selection(p)
    _f1_mount_observer(p,monkeypatch,f"2 1 0:2 / {p.root/boundary} {mode} - nfs4 {source} rw\n")
    with pytest.raises(PauseInterruption,match="namespace"):
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024):pass


@pytest.mark.integration
def test_F1_same_domain_selected_subdirectory_has_its_own_lifetime_readback(packet,monkeypatch):
    p=packet;_f1_nested_selection(p);_f1_mount_observer(p,monkeypatch)
    observed=[]
    def namespace(root,budget):
        observed.append(root)
        return _F1_REAL_NAMESPACE(root,budget)
    monkeypatch.setattr(native_backend,"_namespace",namespace)
    with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024) as (_,sources,telemetry):
        assert len(sources)==2 and all(row[0]==p.root/"selected" for row in sources)
        assert telemetry is None
        before=observed.count(str(p.root/"selected"))
        assert before>=2
    assert observed.count(str(p.root/"selected"))>before


@pytest.mark.integration
def test_F1_original_job_selector_survives_until_context_exit(packet):
    p=packet;p.stored()
    with pytest.raises(PauseInterruption,match="job"):
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024):
            assert p.calc.get_job_id()=="321"
            p.calc.set_job_id("999")


@pytest.mark.integration
def test_F1_job_change_during_owner_close_prevents_first_delivery(packet,monkeypatch,tmp_path):
    p=packet;t=p.transport();original=t.exec_command_wait_bytes;destination=tmp_path/"first-control"
    def owner_close(*args,**kwargs):
        value=original(*args,**kwargs);p.calc.set_job_id("999");return value
    monkeypatch.setattr(t,"exec_command_wait_bytes",owner_close)
    with pytest.raises(PauseInterruption,match="job"):
        with t:t.get(str(p.work/"series_result.json"),str(destination))
    assert not destination.exists()


@pytest.mark.integration
def test_F1_job_change_during_transfer_never_returns_success(packet,monkeypatch,tmp_path):
    p=packet;t=p.transport();original=native_backend._deliver;destination=tmp_path/"partial-control";returned=[]
    def transfer(*args,**kwargs):
        original(*args,**kwargs);p.calc.set_job_id("999")
    monkeypatch.setattr(native_backend,"_deliver",transfer)
    with pytest.raises(PauseInterruption,match="job"):
        with t:
            t.get(str(p.work/"series_result.json"),str(destination))
            returned.append(True)
    assert not returned and destination.read_bytes()==p.raw["series_result.json"]


@pytest.mark.integration
@pytest.mark.parametrize("change",["job","mount"])
def test_F1_change_during_final_actor_readback_is_not_success(packet,monkeypatch,change):
    p=packet;p.stored();rows=_f1_mount_observer(p,monkeypatch)
    with pytest.raises(PauseInterruption,match="job|namespace"):
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024):
            original=native_actor.load_native_launch
            def closing_actor(*args,**kwargs):
                value=original(*args,**kwargs)
                if change=="job":p.calc.set_job_id("999")
                else:rows["raw"]=f"9 0 0:1 / {p.root} ro - nfs4 storage:/jobs rw\n".encode()
                return value
            monkeypatch.setattr(native_actor,"load_native_launch",closing_actor)


@pytest.mark.integration
@pytest.mark.parametrize("change",["job","mount"])
@pytest.mark.parametrize("primary_type",[ValueError,KeyboardInterrupt])
def test_F1_original_consumer_failure_survives_secondary_selector_mismatch(packet,monkeypatch,change,primary_type):
    p=packet;p.stored();rows=_f1_mount_observer(p,monkeypatch);original=primary_type("original F1 consumer failure")
    with pytest.raises(primary_type) as caught:
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=2*1024*1024):
            if change=="job":p.calc.set_job_id("999")
            else:rows["raw"]+=f"2 1 0:2 / {p.root}/deeper rw - nfs4 foreign:/export rw\n".encode()
            raise original
    assert caught.value is original
    assert any("close failed" in note for note in getattr(original,"__notes__",())),"secondary job/mount mismatch was not observed"
