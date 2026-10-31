"""Source/L ordinary adapters. No real Slurm/client/profile qualification."""
import importlib
import importlib.util
import sys
import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy

import pytest
from plumpy.process_states import PauseInterruption
from aiida import orm
from aiida.engine.processes.calcjobs import tasks
from qcl_negf_contracts.messages import decode,encode
from test_native_actor import native_graph


def backend():
    assert importlib.util.find_spec("aiida_qcl_negf.native_backend"), "missing ordinary native adapters"
    return importlib.import_module("aiida_qcl_negf.native_backend")


def test_default_control_get_and_cancel_hold_before_effect(tmp_path):
    module=backend()
    transport=module.NativeLocalTransport(machine="localhost",native_computer_uuid="00000000-0000-4000-8000-000000000001")
    source=tmp_path/"series_result.json";source.write_bytes(b"never copy")
    with pytest.raises(PauseInterruption):transport.get(str(source),str(tmp_path/"copy"),ignore_nonexisting=True)
    assert not (tmp_path/"copy").exists()
    with pytest.raises(PauseInterruption):module.NativeSlurmScheduler().kill_job("321")


def test_bounded_local_public_exec_positive_and_output_failure(tmp_path):
    module=backend()
    transport=module.NativeLocalTransport(machine="localhost",native_computer_uuid="00000000-0000-4000-8000-000000000001")
    with transport:
        status,out,err=transport.exec_command_wait_bytes(f"{sys.executable} -c 'print(123)'",workdir=str(tmp_path))
        assert (status,out,err)==(0,b"123\n",b"")
        with pytest.raises(PauseInterruption):
            transport.exec_command_wait_bytes(f"{sys.executable} -c 'print(\"x\"*300000)'",workdir=str(tmp_path))


def test_submit_unknown_protocol_never_returns_jobid(monkeypatch):
    module=backend();scheduler=module.NativeSlurmScheduler()
    with pytest.raises(PauseInterruption):scheduler.submit_job("/unqualified/workdir","_aiidasubmit.sh")


def reply(calcjob_uuid, state="bound"):
    return dict(version=1,state=state,calcjob_uuid=calcjob_uuid,receipt_ref=calcjob_uuid+"/claim.json",
        controls=None,reason=None,job=dict(job_id="321",cluster="fixture",submit_time="2026-10-29T00:00:00",
        controller_machine_id="a"*32,controller_boot_id="00000000-0000-4000-8000-000000000001",
        dispatch_id="00000000-0000-4000-8000-000000000002",generation="b"*64,
        node_name=None,start_time=None,worker_machine_id=None,worker_boot_id=None))


@pytest.mark.integration
def test_actual_task_hold_bypasses_backoff_then_same_calcjob_sets_id(native_graph,monkeypatch):
    module=backend();calc=native_graph["calc"];requests=[]
    class ProtocolTransport(module.NativeLocalTransport):
        def exec_command_wait_bytes(self,command,stdin=None,workdir=None,**kwargs):
            request=decode(stdin);requests.append(request)
            assert request["calcjob_uuid"]==calc.uuid and request["action"]=="submit"
            if len(requests)==1:return 75,encode(dict(reply(calc.uuid),state="hold",reason="dispatch_outcome_unknown")),b""
            return 0,encode(reply(calc.uuid)),b""
    transport=ProtocolTransport(machine="localhost",native_computer_uuid=calc.computer.uuid)
    scheduler=module.NativeSlurmScheduler();scheduler.set_transport(transport)
    monkeypatch.setattr(orm.Computer,"get_scheduler",lambda self:scheduler)
    assert calc.get_authinfo().computer.uuid==calc.computer.uuid
    class Queue:
        @asynccontextmanager
        async def request_transport(self,authinfo):
            with transport:
                future=asyncio.get_running_loop().create_future();future.set_result(transport)
                yield future
    class Cancellable:
        async def with_interrupt(self,future):return await future
    async def exercise():
        with pytest.raises(PauseInterruption,match="dispatch_outcome_unknown"):
            await tasks.task_submit_job(calc,Queue(),Cancellable())
        assert calc.get_job_id() is None and not calc.is_terminated and len(requests)==1
        assert await tasks.task_submit_job(calc,Queue(),Cancellable())=="321"
    asyncio.run(exercise())
    assert calc.get_job_id()=="321" and len(requests)==2
    assert requests[0]["launch"]==requests[1]["launch"]
    # Protocol source fixture proves the AiiDA seam, not native dispatch counts,
    # persisted play transport, installed authentication or a Slurm job.


@pytest.mark.parametrize("fault",["uuid","extra","job","generation","allocation","state"])
def test_reply_shape_and_retained_hold_are_not_current_bound(fault):
    module=backend();identifier="00000000-0000-4000-8000-000000000003";value=reply(identifier)
    if fault=="uuid":value["calcjob_uuid"]="00000000-0000-4000-8000-000000000004"
    elif fault=="extra":value["verified"]=True
    elif fault=="job":value["job"]["job_id"]=321
    elif fault=="generation":value["job"]["generation"]="fake"
    elif fault=="allocation":value["job"]["node_name"]="worker"
    else:value.update(state="hold",reason="native_qualification_missing")
    with pytest.raises(PauseInterruption):module._reply(encode(value),identifier,state="bound")


def test_command_timeout_preserves_original_and_no_fresh_batch_budget(tmp_path,monkeypatch):
    module=backend();monkeypatch.setattr(module,"_COMMAND_SECONDS",.05)
    transport=module.NativeLocalTransport(machine="localhost",native_computer_uuid="00000000-0000-4000-8000-000000000001")
    with transport,pytest.raises(PauseInterruption) as caught:
        transport.exec_command_wait_bytes(f"{sys.executable} -c 'import time;time.sleep(30)'",workdir=str(tmp_path))
    assert isinstance(caught.value.__cause__,TimeoutError)
    with transport._operation() as budget:
        budget.before(budget.reads.remaining-1)
        with pytest.raises((PauseInterruption,ValueError)):
            with transport._operation() as nested:
                assert nested is budget
                nested.before(2)


def test_empty_or_invalid_job_batch_never_invokes_command(monkeypatch):
    module=backend()
    transport=module.NativeLocalTransport(machine="localhost",native_computer_uuid="00000000-0000-4000-8000-000000000001")
    scheduler=module.NativeSlurmScheduler();scheduler.set_transport(transport)
    monkeypatch.setattr(transport,"exec_command_wait",lambda *a,**k:(_ for _ in ()).throw(AssertionError("unexpected query")))
    assert scheduler.get_jobs(jobs=[],as_dict=True)=={}
    for invalid in ([True],[123],["123; command"],["0"]):
        with pytest.raises(PauseInterruption):scheduler.get_jobs(jobs=invalid,as_dict=True)


@pytest.mark.integration
@pytest.mark.parametrize("state",["PD","R","missing"])
def test_ordinary_slurm_parse_pending_start_and_missing_do_not_grant_get(native_graph,state,tmp_path):
    module=backend();calc=native_graph["calc"];calc.set_job_id("321");actions=[]
    values=dict(job_id="321",state_raw=state,annotation="None",executing_host="worker",username="qcl-negf",
        number_nodes="1",number_cpus="1",allocated_machines="worker",partition="fixture",
        time_limit="00:01:00",time_used="00:00:01",dispatch_time="2026-10-29T00:00:00",
        job_name="fixture",submission_time="2026-10-29T00:00:00")
    class ProtocolTransport(module.NativeLocalTransport):
        def exec_command_wait_bytes(self,command,stdin=None,workdir=None,**kwargs):
            if stdin is None:
                raw=b"" if state=="missing" else ("^^^".join(values[name] for _,name in module.NativeSlurmScheduler.fields)+"\n").encode()
                return 0,raw,b""
            assert self.is_open
            request=decode(stdin);actions.append(request["action"])
            value=reply(calc.uuid,"started")
            value["job"].update(node_name="worker",start_time="2026-10-29T00:00:00",worker_machine_id="c"*32,
                worker_boot_id="00000000-0000-4000-8000-000000000004")
            return 0,encode(value),b""
    transport=ProtocolTransport(machine="localhost",native_computer_uuid=calc.computer.uuid)
    scheduler=module.NativeSlurmScheduler();scheduler.set_transport(transport)
    result=scheduler.get_jobs(jobs=["321"],as_dict=True)
    assert bool(result)==(state!="missing")
    assert actions==(["start"] if state=="R" else [])
    with pytest.raises(PauseInterruption):transport.get("/any/series_result.json",str(tmp_path/"out"))
    assert not (tmp_path/"out").exists()


def _process_stat(pid):
    """One recorded owned PID only; never scan a foreign process namespace."""
    from pathlib import Path
    try:
        fields=Path(f"/proc/{pid}/stat").read_text().rsplit(")",1)[1].split()
        return dict(state=fields[0],start=fields[19])
    except FileNotFoundError:
        return None


@pytest.mark.parametrize("leader_waits",[False,True])
def test_command_cleanup_retains_generation_before_reap_and_stops_inherited_pipe_child(tmp_path,monkeypatch,leader_waits):
    import json,os,shlex,signal,time
    module=backend();monkeypatch.setattr(module,"_COMMAND_SECONDS",.2);monkeypatch.setattr(module,"_CLEANUP_SECONDS",.3)
    records=[];children=[];real_popen=module.subprocess.Popen;real_killpg=os.killpg
    def spawn(*args,**kwargs):
        proc=real_popen(*args,**kwargs);children.append(proc)
        generation=_process_stat(proc.pid)["start"];wait=proc.wait
        def reap(*args,**kwargs):records.append(("reap",proc.pid));return wait(*args,**kwargs)
        proc.wait=reap;proc._test_generation=generation
        return proc
    def signal_group(pgid,sig):
        records.append(("signal",sig))
        if sig==signal.SIGKILL:
            proc=children[0]
            assert proc.returncode is None,"group signal after leader was reaped"
            assert _process_stat(proc.pid)["start"]==proc._test_generation
            observed=os.waitid(os.P_PID,proc.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT)
            assert observed is None or observed.si_pid==proc.pid
            assert os.getsid(proc.pid)==os.getpgid(proc.pid)==pgid==proc.pid
        return real_killpg(pgid,sig)
    monkeypatch.setattr(module.subprocess,"Popen",spawn);monkeypatch.setattr(module.os,"killpg",signal_group)
    target=tmp_path/"owned.json"
    program='import subprocess,sys,os,json;from pathlib import Path;c=subprocess.Popen([sys.executable,"-c","import time;time.sleep(5)"]);Path('+repr(str(target))+').write_text(json.dumps(dict(child=c.pid,start=Path("/proc/"+str(c.pid)+"/stat").read_text().split()[21])));'+('c.wait()' if leader_waits else '')
    transport=module.NativeLocalTransport(machine="localhost",native_computer_uuid="00000000-0000-4000-8000-000000000001")
    started=time.monotonic();record=None
    try:
        with transport,pytest.raises(PauseInterruption) as caught:
            transport.exec_command_wait_bytes(shlex.join([sys.executable,"-c",program]),workdir=str(tmp_path))
        assert isinstance(caught.value.__cause__,TimeoutError)
        record=json.loads(target.read_text());child=_process_stat(record["child"])
        assert child is None or child["start"]!=record["start"] or child["state"]=="Z","same-generation descendant survived HOLD"
        assert records.index(("signal",signal.SIGKILL)) < records.index(("reap",children[0].pid))
        assert all(s.closed for s in (children[0].stdin,children[0].stdout,children[0].stderr))
        assert time.monotonic()-started<1.5
    finally:
        if record is None and target.exists():record=json.loads(target.read_text())
        if record is not None:
            current=_process_stat(record["child"])
            if current and current["start"]==record["start"] and current["state"]!="Z":
                os.kill(record["child"],signal.SIGKILL)  # own recorded generation only, RED fixture cleanup
        for proc in children:
            try:proc.wait(timeout=.5)
            except module.subprocess.TimeoutExpired:
                current=_process_stat(proc.pid)
                if current and current["start"]==proc._test_generation:os.kill(proc.pid,signal.SIGKILL)
                proc.wait(timeout=.5)


@pytest.mark.parametrize("fault",["reaped","group_changed","unknown_group_readback"])
def test_unknown_cleanup_refuses_healthy_and_never_signals_released_or_changed_group(tmp_path,monkeypatch,fault):
    import os,shlex,signal
    module=backend();monkeypatch.setattr(module,"_CLEANUP_SECONDS",.05)
    real_popen=module.subprocess.Popen;real_waitid=os.waitid;real_killpg=os.killpg;real_getpgid=os.getpgid
    processes=[];signals=[];reaped=[]
    def spawn(*args,**kwargs):
        proc=real_popen(*args,**kwargs);processes.append(proc);return proc
    def observe(*args):
        if fault=="reaped" and not reaped:
            os.waitpid(processes[0].pid,0);reaped.append(True)  # own exited child only
        return real_waitid(*args)
    def group(pid):return real_getpgid(pid)+(1 if fault=="group_changed" else 0)
    primary=PermissionError("synthetic group readback unavailable")
    def signal_group(pgid,sig):
        signals.append(sig)
        if fault in ("reaped","group_changed") and sig==signal.SIGKILL:
            raise AssertionError("unqualified numeric PGID signalled")
        if fault=="unknown_group_readback" and sig==0:raise primary
        return real_killpg(pgid,sig)
    monkeypatch.setattr(module.subprocess,"Popen",spawn);monkeypatch.setattr(module.os,"waitid",observe)
    monkeypatch.setattr(module.os,"getpgid",group);monkeypatch.setattr(module.os,"killpg",signal_group)
    transport=module.NativeLocalTransport(machine="localhost",native_computer_uuid="00000000-0000-4000-8000-000000000001")
    try:
        with transport,pytest.raises(PauseInterruption) as caught:
            transport.exec_command_wait_bytes(shlex.join([sys.executable,"-c","print('healthy')"]),workdir=str(tmp_path))
        if fault=="unknown_group_readback":assert caught.value.__cause__ is primary
        else:assert signal.SIGKILL not in signals
        assert signals.count(signal.SIGKILL)<=1
        assert all(s.closed for s in (processes[0].stdin,processes[0].stdout,processes[0].stderr))
    finally:
        for proc in processes:proc.wait(timeout=.5)  # command is already exited, no group signal after release


@pytest.mark.parametrize("failure_type",[KeyboardInterrupt,SystemExit,OSError,ValueError,PauseInterruption])
def test_baseexception_keeps_original_and_cleans_owned_unreaped_child(tmp_path,monkeypatch,failure_type):
    import signal,shlex
    module=backend();processes=[];real_popen=module.subprocess.Popen;real_selector=module.selectors.DefaultSelector
    primary=failure_type("synthetic caller failure")
    def spawn(*args,**kwargs):
        proc=real_popen(*args,**kwargs);processes.append(proc);return proc
    def selector():
        instance=real_selector()
        def interrupted(*args):raise primary
        instance.select=interrupted;return instance
    monkeypatch.setattr(module.subprocess,"Popen",spawn);monkeypatch.setattr(module.selectors,"DefaultSelector",selector)
    transport=module.NativeLocalTransport(machine="localhost",native_computer_uuid="00000000-0000-4000-8000-000000000001")
    expected=PauseInterruption if failure_type in (OSError,ValueError) else failure_type
    with transport,pytest.raises(expected) as caught:
        transport.exec_command_wait_bytes(shlex.join([sys.executable,"-c","import time;time.sleep(5)"]),workdir=str(tmp_path))
    if failure_type in (OSError,ValueError):assert caught.value.__cause__ is primary
    else:assert caught.value is primary
    assert processes[0].returncode==-signal.SIGKILL
    assert all(s.closed for s in (processes[0].stdin,processes[0].stdout,processes[0].stderr))



def test_cleanup_unknown_keeps_primary_timeout_and_original_shared_deadline(tmp_path,monkeypatch):
    import time,signal,shlex
    module=backend();real_killpg=module.os.killpg;signals=[]
    def group(pgid,sig):
        signals.append(sig)
        if sig==0:return None  # cleanup readback cannot establish absence
        return real_killpg(pgid,sig)
    monkeypatch.setattr(module.os,"killpg",group)
    transport=module.NativeLocalTransport(machine="localhost",native_computer_uuid="00000000-0000-4000-8000-000000000001")
    with transport,transport._operation() as original:
        original.deadline=time.monotonic()+.15
        deadline=original.deadline
        with pytest.raises(PauseInterruption) as caught:
            transport.exec_command_wait_bytes(shlex.join([sys.executable,"-c","import time;time.sleep(5)"]),workdir=str(tmp_path))
        assert transport._native_operation is original and original.deadline==deadline
        assert time.monotonic()<=deadline+.2  # no fresh two-second cleanup/retry window
    assert isinstance(caught.value.__cause__,TimeoutError)
    assert caught.value.__cause__.__notes__==["native cleanup unknown: TimeoutError"]
    assert signals.count(signal.SIGKILL)==1


def test_closed_claim_bool_is_not_integer_even_when_python_equality_matches():
    module=backend();identifier="00000000-0000-4000-8000-000000000003"
    names=("series_result.json","native-selection.json","_scheduler-stdout.txt","_scheduler-stderr.txt")
    controls=[dict(path=name,bytes=0,sha256="a"*64) for name in names]
    technical=dict(status="absent",path="performance-source.json",reason="not_published")
    value=reply(identifier,"closed");value.update(version=3,technical_control=technical,receipt_ref=identifier+"/close.json",controls=deepcopy(controls))
    value["job"].update(node_name="worker",start_time="2026-10-29T00:00:00",worker_machine_id="c"*32,worker_boot_id="00000000-0000-4000-8000-000000000004")
    value["controls"][0]["bytes"]=False
    assert value["controls"]==controls  # Python equality is not this wire contract.
    with pytest.raises(PauseInterruption):module._reply(encode(value),identifier,state="closed",controls=controls,technical_control=technical,job_id="321")


@pytest.mark.parametrize("asynchronous",[False,True])
def test_q1_context_secondary_close_error_preserves_original(monkeypatch,asynchronous):
    module=backend();transport=module.NativeLocalTransport(machine="localhost",native_computer_uuid="00000000-0000-4000-8000-000000000001")
    original=ValueError("original body failure");close=transport.close
    monkeypatch.setattr(transport,"close",lambda:(_ for _ in ()).throw(OSError("secondary close")))
    async def exercise():
        async with transport:raise original
    try:
        with pytest.raises(ValueError) as caught:
            if asynchronous:asyncio.run(exercise())
            else:
                with transport:raise original
        assert caught.value is original and any("close failed" in note for note in original.__notes__)
    finally:
        monkeypatch.setattr(transport,"close",close)
        if transport.is_open:transport.close()


@pytest.mark.parametrize("technical",[
    None,False,{},
    {"status":"absent","path":"performance-source.json","reason":"unknown"},
    {"status":"absent","path":"performance-source.json","reason":"not_published","bytes":"0"},
    {"status":"present","path":"performance-source.json","bytes":False,"sha256":"a"*64},
    {"status":"present","path":"performance-source.json","bytes":0,"sha256":"a"*64},
    {"status":"present","path":"performance-source.json","bytes":"00","sha256":"a"*64},
    {"status":"present","path":"performance-source.json","bytes":"1048577","sha256":"a"*64},
    {"status":"present","path":"performance-source.json","bytes":"18446744073709551616","sha256":"a"*64},
    {"status":"present","path":"../performance-source.json","bytes":"0","sha256":"a"*64},
    {"status":"present","path":"performance-source.json","bytes":"0","sha256":"A"*64},
    {"status":"present","path":"performance-source.json","bytes":"0","sha256":"a"*64,"extra":True},
],ids=["null","bool","empty","reason","absent_extra","bool_bytes","number_bytes","leading_zero","cap","overflow","path","hash","present_extra"])
def test_Q1T_strict_technical_operand_rejects_unobserved_or_malformed(technical):
    module=backend()
    with pytest.raises(module.native_actor.NativeActorUnavailable):module.native_actor._technical_control(technical)


@pytest.mark.parametrize("fault",["v1","v2","bool_version","null_observation","mismatch","extra","hold","wrong_job","noncanonical"])
def test_Q1T_close3_has_no_downgrade_or_projection_substitution(fault):
    module=backend();identifier="00000000-0000-4000-8000-000000000003"
    controls=[dict(path=name,bytes=0,sha256="a"*64) for name in ("series_result.json","native-selection.json","_scheduler-stdout.txt","_scheduler-stderr.txt")]
    technical=dict(status="absent",path="performance-source.json",reason="not_published")
    value=reply(identifier,"closed");value.update(version=3,receipt_ref=identifier+"/close.json",controls=controls,technical_control=technical)
    value["job"].update(node_name="worker",start_time="2026-10-29T00:00:00",worker_machine_id="c"*32,worker_boot_id="00000000-0000-4000-8000-000000000004")
    if fault in ("v1","v2","bool_version"):value["version"]={"v1":1,"v2":2,"bool_version":True}[fault]
    elif fault=="null_observation":value["technical_control"]=None
    elif fault=="mismatch":value["technical_control"]=dict(status="present",path="performance-source.json",bytes="0",sha256="a"*64)
    elif fault=="extra":value["extra"]=None
    elif fault=="hold":value.update(state="hold",controls=None,technical_control=None,reason="not_observed")
    elif fault=="wrong_job":value["job"]["job_id"]="322"
    raw=encode(value)
    if fault=="noncanonical":raw=b" "+raw
    with pytest.raises((PauseInterruption,module.native_actor.NativeActorUnavailable)):
        module._reply(raw,identifier,state="closed",controls=controls,technical_control=technical,job_id="321")


@pytest.mark.parametrize("kind",["metadata","io"])
def test_Q1T_one_byte_short_read_credit_refuses_before_byte_io(tmp_path,monkeypatch,kind):
    module=backend();(tmp_path/"performance-source.json").write_bytes(b"1234")
    budget=module._Operation();tree=module._Tree(str(tmp_path),budget)
    if kind=="metadata":budget.reads.remaining=1024+8*5-1
    else:budget.io_remaining=4  # The complete read also admits its EOF request.
    monkeypatch.setattr(module.os,"read",lambda *args:(_ for _ in ()).throw(AssertionError("unpaid byte IO")))
    try:
        with pytest.raises(PauseInterruption):tree.read("performance-source.json",1048576,optional=True)
        assert tree.technical_fd is None
    finally:tree.close()


@pytest.mark.parametrize("role,bad",[("plan",1048577),("source_graph",65537),("native_execution",65537),("submit_script",65537),("plan",True),("source_graph",-1)])
def test_Q1T_role_bound_refuses_before_second_actor_read(monkeypatch,role,bad):
    module=backend();counts={name:{"bytes":0,"sha256":"a"*64} for name in ("plan","source_graph","native_execution","submit_script")}
    counts[role]["bytes"]=bad
    monkeypatch.setattr(module.native_actor,"load_native_launch",lambda *a,**k:{"files":counts})
    monkeypatch.setattr(module.native_actor,"read_native_execution",lambda *a,**k:(_ for _ in ()).throw(AssertionError("unadmitted second read")))
    with pytest.raises(PauseInterruption,match="q1_actor_input_bound"):
        module._Packet(None,"00000000-0000-4000-8000-000000000001",module._Operation())


def test_Q1T_fixed_leaf_exact_read_workspace_and_eof_credit(tmp_path):
    module=backend();(tmp_path/"performance-source.json").write_bytes(b"1234")
    budget=module._Operation();tree=module._Tree(str(tmp_path),budget)
    # Hand derived: 1024 open + 8*(4 bytes + EOF) + 512 + 2*23 binding.
    budget.reads.remaining=1622;budget.io_remaining=5
    try:
        assert tree.read("performance-source.json",1048576,optional=True)==b"1234"
        assert budget.reads.remaining==0 and budget.io_remaining==0
    finally:tree.close()


def test_Q1T_actor_metadata_ceiling_rejects_one_above_before_profile(monkeypatch):
    module=backend()
    monkeypatch.setattr(module.native_actor,"_orm",lambda:(_ for _ in ()).throw(AssertionError("profile touched")))
    with pytest.raises(module.native_actor.NativeActorUnavailable,match="metadata_budget"):
        module.native_actor._Reads(16777217)


def test_Q1T_optional_read_cancel_keeps_primary_with_secondary_fd_close_failure(tmp_path,monkeypatch):
    module=backend();(tmp_path/"performance-source.json").write_bytes(b"opaque")
    budget=module._Operation();tree=module._Tree(str(tmp_path),budget)
    primary=KeyboardInterrupt("original optional read cancellation");opened=[]
    opening,reading,closing=module.os.open,module.os.read,module.os.close
    def open_leaf(name,*args,**kwargs):
        fd=opening(name,*args,**kwargs)
        if name=="performance-source.json":opened.append(fd)
        return fd
    def read_leaf(fd,*args):
        if fd in opened:raise primary
        return reading(fd,*args)
    def close_leaf(fd):
        closing(fd)
        if fd in opened:raise OSError("secondary file FD cleanup unknown")
    monkeypatch.setattr(module.os,"open",open_leaf);monkeypatch.setattr(module.os,"read",read_leaf);monkeypatch.setattr(module.os,"close",close_leaf)
    try:
        with pytest.raises(KeyboardInterrupt) as caught:tree.read("performance-source.json",1048576,optional=True)
        assert caught.value is primary and any("file FD cleanup failed" in note for note in primary.__notes__)
    finally:
        monkeypatch.setattr(module.os,"close",closing);tree.close()
