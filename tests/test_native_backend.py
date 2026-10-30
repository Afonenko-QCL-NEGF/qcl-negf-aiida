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
        budget.before(8*1024*1024-1)
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
