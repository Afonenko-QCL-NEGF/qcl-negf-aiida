"""Preparation only: real plugin ports, no scheduler dispatch or numerical solve."""
import hashlib
import json
import os
from pathlib import Path

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
def test_native_origin_literal_eleven_args_and_fixed_capture_exports(native_graph,monkeypatch):
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
        assert info.prepend_text.endswith("\nexport QCL_NEGF_CAPTURE_SPEC=capture-spec.json QCL_NEGF_NATIVE_CAPTURE_INPUT=native-execution.json")
        assert info.retrieve_list==["series_result.json","native-selection.json","native-close.json","performance-source.json"]
    finally:
        process.close()


@pytest.fixture
def ingress_process(native_graph,monkeypatch):
    f=native_graph
    monkeypatch.setattr(orm.Computer,'get_scheduler',lambda self:SlurmScheduler())
    monkeypatch.setenv('QCL_NEGF_NATIVE_JOBS_ROOT','/srv/qcl-negf/jobs')
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=f['restart'].pk,inputs={
        **{name:f['calc'].inputs[name] for name in ('code','plan','source_graph','native_execution','execution_id','attempt','release_id')},
        'metadata':{'call_link_label':'native_ingress','options':scheduler_options({})}})
    try:yield process,f
    finally:process.close()


@pytest.mark.integration
def test_native3_stock_after_genuine_CREATE_and_same_candidate(ingress_process,monkeypatch):
    from aiida_qcl_negf import native_actor
    process,f=ingress_process;load=native_actor._load_original;calls=[]
    def qualified(identifier,budget):
        script=process.node.outputs.native_submit_script
        assert script.creator.uuid==identifier
        value=load(identifier,budget);calls.append(value[2]['run_uuid']);return value
    monkeypatch.setattr(native_actor,'_load_original',qualified)
    with SandboxFolder() as folder:
        info=process.presubmit(folder)
        raw=folder.get_abs_path('capture-spec.json')
        from pathlib import Path
        candidate=Path(raw).read_bytes();spec=json.loads(candidate);script=process.node.outputs.native_submit_script
        assert spec['native_performance']=={'run_uuid':f['run'].uuid}
        assert spec['native_performance']['run_uuid']!=f['execution']['request_id']
        assert spec['schema']=='qcl-negf.capture-spec.v3' and len(spec)==9
        from qcl_negf_results.capture import _spec,ObserverIO
        assert _spec(Path(raw),budget=ObserverIO(128*1024*1024))[1]==spec
        assert info.retrieve_list==['series_result.json','native-selection.json','native-close.json','performance-source.json','_scheduler-stdout.txt','_scheduler-stderr.txt']
        with script.open(mode='rb') as stream:original=stream.read()
        assert b'export QCL_NEGF_CAPTURE_SPEC=capture-spec.json QCL_NEGF_NATIVE_CAPTURE_INPUT=native-execution.json\n' in original
        process.presubmit(folder)
        assert Path(raw).read_bytes()==candidate and process.node.outputs.native_submit_script.uuid==script.uuid
        assert calls==[f['run'].uuid,f['run'].uuid]


@pytest.mark.integration
@pytest.mark.parametrize('fault',['foreign','symlink','fifo','hardlink','changed_bytes','residual_temp','reconstructed','credit'])
def test_native3_uncertain_never_returns_to_stock_upload(ingress_process,fault,tmp_path):
    from aiida_qcl_negf.native_actor import NativeActorUnavailable
    process,f=ingress_process
    with SandboxFolder(filepath=tmp_path) as folder:
        target=Path(folder.abspath,'capture-spec.json')
        if fault=='foreign':target.write_bytes(b'unknown')
        elif fault=='fifo':os.mkfifo(target)
        elif fault=='symlink':
            external=tmp_path/'foreign';external.write_bytes(b'untouched');target.symlink_to(external)
        else:
            process.presubmit(folder)
            if fault=='hardlink':os.link(target,tmp_path/'foreign-link')
            elif fault=='changed_bytes':target.write_bytes(target.read_bytes()+b' ')
            elif fault=='residual_temp':Path(folder.abspath,'capture-spec.json.tmp').write_bytes(b'unknown-temp')
            elif fault=='reconstructed':process._native_capture_state=None
            elif fault=='credit':process._native_capture_state['budget'].remaining=1
        with pytest.raises((NativeActorUnavailable,OSError)):
            process.presubmit(folder)
        assert process.node.get_job_id() is None
        assert process.node.outputs.native_submit_script.creator.uuid==process.node.uuid
        if fault=='foreign':assert target.read_bytes()==b'unknown'
        elif fault=='symlink':assert target.is_symlink() and external.read_bytes()==b'untouched'
        elif fault=='residual_temp':assert Path(folder.abspath,'capture-spec.json.tmp').read_bytes()==b'unknown-temp'


@pytest.mark.integration
@pytest.mark.parametrize('fault',['write','partial','fsync','open'])
def test_native3_direct_write_faults_preserve_residual_or_prove_safe_outcome(ingress_process,monkeypatch,fault):
    from aiida_qcl_negf import calculation as C
    process,f=ingress_process;write=os.write;open_file=os.open;sync=os.fsync;fired=[]
    primary=OSError('original direct candidate fault')
    with SandboxFolder() as folder:
        gate=process._native_capture_gate
        def inject(folder,raw):
            def writing(fd,data):
                fired.append('write')
                if fault=='write' or fired.count('write')>1:raise primary
                return write(fd,data[:7])
            def opening(name,flags,*args,**kwargs):
                if name=='capture-spec.json' and flags & os.O_CREAT:
                    fired.append('open');raise primary
                return open_file(name,flags,*args,**kwargs)
            def syncing(fd):
                if os.readlink('/proc/self/fd/'+str(fd)).endswith('/capture-spec.json'):
                    fired.append('fsync');raise primary
                return sync(fd)
            with monkeypatch.context() as patch:
                if fault in ('write','partial'):patch.setattr(C.os,'write',writing)
                elif fault=='open':patch.setattr(C.os,'open',opening)
                else:patch.setattr(C.os,'fsync',syncing)
                return gate(folder,raw)
        monkeypatch.setattr(process,'_native_capture_gate',inject)
        if fault in ('write','partial'):
            with pytest.raises(OSError) as caught:process.presubmit(folder)
            assert caught.value is primary
            assert Path(folder.abspath,'capture-spec.json').read_bytes()==(b'' if fault=='write' else process._native_capture_state['candidate'][:7])
        else:
            process.presubmit(folder)
            target=Path(folder.abspath,'capture-spec.json')
            assert (not target.exists()) if fault=='open' else target.read_bytes()==process._native_capture_state['candidate']
        assert fired and not Path(folder.abspath,'capture-spec.json.tmp').exists()
        assert process.node.outputs.native_submit_script.creator.uuid==process.node.uuid


@pytest.mark.integration
@pytest.mark.parametrize('fault',['foreign_replacement','cancel','system_exit','cancel_close','parent'])
def test_native3_primary_and_namespace_uncertainty_preserved(ingress_process,monkeypatch,fault,tmp_path):
    from aiida_qcl_negf import calculation as module
    process,f=ingress_process;original=module._CaptureSandbox.publish
    primary=(KeyboardInterrupt('original preparation cancel') if fault.startswith('cancel') else
             SystemExit('original preparation exit') if fault=='system_exit' else OSError('original preparation failure'))
    def publish(guard,candidate):
        if fault=='parent':
            raise primary
        original(guard,candidate)
        if fault=='foreign_replacement':
            Path(guard.path,'capture-spec.json').rename(tmp_path/'original-owned');Path(guard.path,'capture-spec.json').write_bytes(b'foreign')
        raise primary
    monkeypatch.setattr(module._CaptureSandbox,'publish',publish)
    if fault=='cancel_close':
        close=module._CaptureSandbox.close
        def secondary(self):close(self);raise OSError('secondary close')
        monkeypatch.setattr(module._CaptureSandbox,'close',secondary)
    with SandboxFolder(filepath=tmp_path) as folder:
        if fault=='parent':
            gate=process._native_capture_gate;original_path=Path(folder.abspath);moved=tmp_path/'moved'
            def changed(folder,raw):
                original_path.rename(moved);original_path.mkdir(mode=0o700)
                try:return gate(folder,raw)
                finally:original_path.rmdir();moved.rename(original_path)
            monkeypatch.setattr(process,'_native_capture_gate',changed)
            with pytest.raises(ValueError,match='capture_sandbox_changed'):process.presubmit(folder)
        else:
            with pytest.raises(type(primary)) as caught:process.presubmit(folder)
            assert caught.value is primary
            if fault in ('foreign_replacement','cancel_close'):assert primary.__notes__
            if fault=='foreign_replacement':assert Path(folder.abspath,'capture-spec.json').read_bytes()==b'foreign'
        assert process.node.get_job_id() is None


@pytest.mark.integration
def test_native3_original_graph_failure_never_becomes_optional(ingress_process,monkeypatch):
    from aiida_qcl_negf import native_actor
    process,f=ingress_process;primary=OSError('original CALL qualification failure')
    monkeypatch.setattr(native_actor,'_load_original',lambda *a:(_ for _ in ()).throw(primary))
    with SandboxFolder() as folder,pytest.raises(OSError) as caught:process.presubmit(folder)
    assert caught.value is primary and process.node.outputs.native_submit_script.creator.uuid==process.node.uuid


@pytest.mark.integration
def test_native3_actual_stock_script_matches_published_Platform_and_B(ingress_process,monkeypatch):
    import importlib.util
    from types import SimpleNamespace
    from aiida_qcl_negf import native_actor,saved_sources
    process,f=ingress_process
    source=Path(__file__).parents[2]/'p07-p1-platform'/'ops'/'native_attempt.py'
    monkeypatch.syspath_prepend(str(source.parent))
    spec=importlib.util.spec_from_file_location('ingress_platform_peer',source);peer=importlib.util.module_from_spec(spec);spec.loader.exec_module(peer)
    assert Path(peer.__file__).resolve()==source.resolve()
    with SandboxFolder() as folder:
        process.presubmit(folder)
        with process.node.outputs.native_submit_script.open(mode='rb') as stream:raw=stream.read()
        launch=native_actor._load_original(process.node.uuid,native_actor._Reads(native_actor.MAX_METADATA))[2]
        sources=SimpleNamespace(policy={'jobs_root':'/srv/qcl-negf/jobs'},origin=lambda path:path)
        for candidate in (raw,raw.replace(b'export QCL_NEGF_CAPTURE_SPEC=capture-spec.json QCL_NEGF_NATIVE_CAPTURE_INPUT=native-execution.json',b'unset QCL_NEGF_CAPTURE_SPEC')):
            peer._review_native_script(candidate,launch,str(f['code'].filepath_executable),'/bin/bash',sources,calcjob_uuid=process.node.uuid)
            saved_sources._native_origin_script(candidate,process.node,dict(execution_binding=f['execution'],selection=dict(execution_id=f['execution']['execution_id'])))


@pytest.mark.integration
def test_native3_missing_optional_candidate_existing_launcher_runs_one_fake_solver(ingress_process,monkeypatch,tmp_path):
    import shlex,subprocess,sys
    from aiida_qcl_negf import calculation
    process,f=ingress_process
    def denied(guard,candidate):raise OSError('optional prewrite failure')
    monkeypatch.setattr(calculation._CaptureSandbox,'publish',denied)
    with SandboxFolder(filepath=tmp_path) as folder:
        info=process.presubmit(folder)
        assert not Path(folder.abspath,'capture-spec.json').exists()
        solver=tmp_path/'fake-solver';count=tmp_path/'solver-count'
        solver.write_text('#!'+sys.executable+'\nfrom pathlib import Path\nimport time\np=Path('+repr(str(count))+')\np.write_text((p.read_text() if p.exists() else "")+"spawn\\n")\ntime.sleep(0.1)\nraise SystemExit(17)\n');solver.chmod(0o700)
        capture=tmp_path/'capture-cli';capture.write_text('#!/bin/bash\nexec '+shlex.join([sys.executable,'-c','from qcl_negf_results.cli import main;main()'])+' "$@"\n');capture.chmod(0o700)
        source=Path(__file__).parents[2]/'p05-whole70-runner'/'nix'/'launcher.sh'
        launcher=tmp_path/'launcher';launcher.write_text(source.read_text().replace('@bash@','/bin/bash').replace('@solver@',str(solver)).replace('@capture@',str(capture)));launcher.chmod(0o700)
        before=set(Path(folder.abspath).iterdir())
        result=subprocess.run([str(launcher),*info.codes_info[0].cmdline_params],cwd=folder.abspath,
            env=dict(os.environ,QCL_NEGF_CAPTURE_SPEC='capture-spec.json',QCL_NEGF_NATIVE_CAPTURE_INPUT='native-execution.json'),capture_output=True,timeout=15)
        assert result.returncode==17 and count.read_text()=='spawn\n'
        assert set(Path(folder.abspath).iterdir())==before
        assert not Path(folder.abspath,'performance').exists()


@pytest.mark.integration
def test_native3_actual_optional_retrieve_declaration_and_genuine_core5(ingress_process):
    from aiida.common.links import LinkType
    from aiida_qcl_negf import native_actor
    import io
    process,f=ingress_process
    with SandboxFolder() as folder:
        info=process.presubmit(folder)
        assert len(info.retrieve_list)==6 and 'performance-source.json' in process.node.get_retrieve_list()
        expected={name:b'original '+name.encode() for name in native_actor.CONTROL_FILES}
        retrieved=orm.FolderData()
        for name,raw in expected.items():retrieved.base.repository.put_object_from_filelike(io.BytesIO(raw),name)
        retrieved.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');retrieved.store()
        observed=native_actor._retrieved_packet(process.node.uuid,expected,native_actor._Reads(native_actor.MAX_METADATA),
            technical_control=dict(status='absent',path='performance-source.json',reason='not_published'))
        assert observed==expected


@pytest.mark.integration
def test_native3_file_close_failure_keeps_original_cancel(ingress_process,monkeypatch):
    from aiida_qcl_negf import calculation
    process,f=ingress_process;close=os.close;primary=KeyboardInterrupt('original file write cancel')
    def write(fd,raw):raise primary
    def secondary(fd):
        target=os.readlink('/proc/self/fd/'+str(fd));close(fd)
        if target.endswith('/capture-spec.json'):raise OSError('secondary file close')
    with SandboxFolder() as folder:
        gate=process._native_capture_gate
        def fault(folder,raw):
            with monkeypatch.context() as patch:
                patch.setattr(calculation.os,'write',write);patch.setattr(calculation.os,'close',secondary)
                return gate(folder,raw)
        monkeypatch.setattr(process,'_native_capture_gate',fault)
        with pytest.raises(KeyboardInterrupt) as caught:process.presubmit(folder)
        assert caught.value is primary and any('capture file close failed' in n for n in primary.__notes__)
        assert Path(folder.abspath,'capture-spec.json').read_bytes()==b'' and not Path(folder.abspath,'capture-spec.json.tmp').exists()


@pytest.mark.integration
def test_native3_large_original_plan_shared_credit_and_fresh_graph(native_graph,monkeypatch,record_property):
    from test_native_selected_sources import _execution_graph
    f=_execution_graph(native_graph,'execution',plan_size=113111)
    monkeypatch.setattr(orm.Computer,'get_scheduler',lambda self:SlurmScheduler())
    monkeypatch.setenv('QCL_NEGF_NATIVE_JOBS_ROOT','/srv/qcl-negf/jobs')
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=f['run'].pk,inputs={
        **{name:f['calc'].inputs[name] for name in ('code','plan','source_graph','native_execution','execution_id','attempt','release_id')},
        'metadata':{'call_link_label':'large_ingress','options':scheduler_options({})}})
    try:
        with SandboxFolder() as folder:
            process.presubmit(folder);state=process._native_capture_state;budget=state['budget']
            first=budget.remaining;candidate=Path(folder.abspath,'capture-spec.json').read_bytes()
            process.presubmit(folder)
            assert process._native_capture_state['budget'] is budget and 0<budget.remaining<first<16*1024*1024
            assert Path(folder.abspath,'capture-spec.json').read_bytes()==candidate
            assert json.loads(candidate)['plan']['bytes']==113111
            record_property('native3_large_shared_credit',json.dumps(dict(first_used=16*1024*1024-first,
                retry_total_used=16*1024*1024-budget.remaining,maximum=16*1024*1024,semantics='gross nonrefundable workspace, not RSS')))
    finally:process.close()


@pytest.mark.integration
def test_native3_file_close_uncertainty_retains_final_and_refuses(ingress_process,monkeypatch):
    from aiida_qcl_negf import calculation
    process,f=ingress_process;close=os.close;primary=OSError('original candidate FD close uncertainty')
    def secondary(fd):
        target=os.readlink('/proc/self/fd/'+str(fd));close(fd)
        if target.endswith('/capture-spec.json'):raise primary
    with SandboxFolder() as folder:
        gate=process._native_capture_gate
        def fault(folder,raw):
            with monkeypatch.context() as patch:
                patch.setattr(calculation.os,'close',secondary)
                return gate(folder,raw)
        monkeypatch.setattr(process,'_native_capture_gate',fault)
        with pytest.raises(OSError) as caught:process.presubmit(folder)
        assert caught.value is primary
        assert Path(folder.abspath,'capture-spec.json').read_bytes()==process._native_capture_state['candidate']
        assert not Path(folder.abspath,'capture-spec.json.tmp').exists()
        assert process._native_capture_state['close_uncertain']


@pytest.mark.integration
def test_F1_direct_exclusive_creation_has_no_named_deletion_or_temporary_write(ingress_process,monkeypatch):
    from aiida_qcl_negf import calculation as C
    process,f=ingress_process;opening=os.open;created=[];forbidden=[]
    assert not hasattr(C._CaptureSandbox,'cleanup')
    with SandboxFolder() as folder:
        gate=process._native_capture_gate
        def inject(folder,raw):
            def open_file(name,flags,*args,**kwargs):
                if flags & os.O_CREAT:
                    created.append((name,flags,args[0]))
                    assert name=='capture-spec.json'
                    assert flags & (os.O_EXCL|os.O_NOFOLLOW|os.O_WRONLY)==os.O_EXCL|os.O_NOFOLLOW|os.O_WRONLY
                    assert not flags & os.O_TRUNC and args[0]==0o600
                return opening(name,flags,*args,**kwargs)
            def rejected(*args,**kwargs):forbidden.append(args);raise AssertionError('named destruction/link attempted')
            with monkeypatch.context() as patch:
                patch.setattr(C.os,'open',open_file)
                for name in ('unlink','link','rename','replace','remove'):patch.setattr(C.os,name,rejected)
                return gate(folder,raw)
        monkeypatch.setattr(process,'_native_capture_gate',inject)
        process.presubmit(folder);candidate=Path(folder.abspath,'capture-spec.json').read_bytes()
        script=process.node.outputs.native_submit_script.uuid
        process.presubmit(folder)
        assert Path(folder.abspath,'capture-spec.json').read_bytes()==candidate
        assert process.node.outputs.native_submit_script.uuid==script
        assert len(created)==1 and forbidden==[]


@pytest.mark.integration
def test_native3_optional_primary_remains_primary_if_later_parent_close_fails(ingress_process,monkeypatch):
    from aiida_qcl_negf import calculation
    process,f=ingress_process;primary=OSError('original optional write failure');close=calculation._CaptureSandbox.close
    def denied(self,candidate):raise primary
    def secondary(self):close(self);raise OSError('secondary parent close')
    monkeypatch.setattr(calculation._CaptureSandbox,'publish',denied)
    monkeypatch.setattr(calculation._CaptureSandbox,'close',secondary)
    with SandboxFolder() as folder,pytest.raises(OSError) as caught:process.presubmit(folder)
    assert caught.value is primary and any('sandbox close failed' in n for n in primary.__notes__)
    assert process._native_capture_state['close_uncertain']


@pytest.mark.integration
@pytest.mark.parametrize('cancel',[False,True])
def test_native3_generation_failure_optional_only_with_proven_absence(ingress_process,monkeypatch,cancel):
    from aiida_qcl_negf import capture_spec
    process,f=ingress_process
    primary=KeyboardInterrupt('original generation cancellation') if cancel else OSError('original generation failure')
    monkeypatch.setattr(capture_spec,'native_capture_spec',lambda *a,**k:(_ for _ in ()).throw(primary))
    with SandboxFolder() as folder:
        if cancel:
            with pytest.raises(KeyboardInterrupt) as caught:process.presubmit(folder)
            assert caught.value is primary
        else:process.presubmit(folder)
        assert not Path(folder.abspath,'capture-spec.json').exists() and not Path(folder.abspath,'capture-spec.json.tmp').exists()
        assert process.node.outputs.native_submit_script.creator.uuid==process.node.uuid


@pytest.mark.integration
@pytest.mark.parametrize('mode',['partial','disappeared'])
def test_F1_direct_partial_residual_and_primary_are_preserved(ingress_process,monkeypatch,mode):
    from aiida_qcl_negf import calculation as C
    process,f=ingress_process;primary=OSError('original direct partial write');write=os.write
    with SandboxFolder() as folder:
        gate=process._native_capture_gate
        def fault(folder,raw):
            def partial(fd,data):
                write(fd,data[:7])
                if mode=='disappeared':
                    # The name may move; ENOENT is not evidence the own inode vanished.
                    path=Path(folder.abspath,'capture-spec.json')
                    if path.exists():path.rename(Path(folder.abspath,'retained-original'))
                raise primary
            with monkeypatch.context() as patch:
                patch.setattr(C.os,'write',partial)
                return gate(folder,raw)
        monkeypatch.setattr(process,'_native_capture_gate',fault)
        with pytest.raises(OSError) as caught:process.presubmit(folder)
        assert caught.value is primary
        path=Path(folder.abspath,'capture-spec.json' if mode=='partial' else 'retained-original')
        assert path.read_bytes()==process._native_capture_state['candidate'][:7]
        assert not Path(folder.abspath,'capture-spec.json.tmp').exists()
        with pytest.raises(ValueError,match='unconfirmed'):process.presubmit(folder)


@pytest.mark.integration
def test_F1_late_ordinary_error_keeps_verified_same_C(ingress_process,monkeypatch):
    from aiida_qcl_negf import calculation as C
    process,f=ingress_process;publish=C._CaptureSandbox.publish;primary=OSError('original late ordinary error')
    def late(self,candidate):publish(self,candidate);raise primary
    monkeypatch.setattr(C._CaptureSandbox,'publish',late)
    with SandboxFolder() as folder:
        process.presubmit(folder);candidate=process._native_capture_state['candidate']
        assert Path(folder.abspath,'capture-spec.json').read_bytes()==candidate
        assert process._native_capture_state['confirmed'] is True


@pytest.mark.integration
def test_F1_unknown_direct_final_replacement_preserved_before_upload(ingress_process,monkeypatch):
    from aiida_qcl_negf import calculation as C
    process,f=ingress_process;write=os.write;fired=[]
    with SandboxFolder() as folder:
        gate=process._native_capture_gate
        def fault(folder,raw):
            def replaced(fd,data):
                result=write(fd,data)
                if os.readlink('/proc/self/fd/'+str(fd)).endswith('/capture-spec.json') and not fired:
                    target=Path(folder.abspath,'capture-spec.json');original=target.stat().st_ino
                    target.rename(Path(folder.abspath,'retained-original'))
                    target.write_bytes(b'unknown replacement');fired.append((original,target.stat().st_ino))
                return result
            with monkeypatch.context() as patch:
                patch.setattr(C.os,'write',replaced)
                return gate(folder,raw)
        monkeypatch.setattr(process,'_native_capture_gate',fault)
        caught=None
        try:process.presubmit(folder)
        except (OSError,ValueError) as error:caught=error
        assert fired and fired[0][0]!=fired[0][1], 'direct final write fault must actually fire'
        assert caught is not None and Path(folder.abspath,'capture-spec.json').read_bytes()==b'unknown replacement'
        assert process.node.get_job_id() is None


@pytest.mark.integration
def test_F1_generation_error_cannot_accept_ABSENT_after_prior_creation(ingress_process,monkeypatch):
    from aiida_qcl_negf import capture_spec
    process,f=ingress_process;primary=OSError('original retry generation failure')
    with SandboxFolder() as folder:
        process.presubmit(folder)
        target=Path(folder.abspath,'capture-spec.json');saved=Path(folder.abspath,'retained-original')
        target.rename(saved)
        monkeypatch.setattr(capture_spec,'native_capture_spec',lambda *a,**k:(_ for _ in ()).throw(primary))
        with pytest.raises(OSError) as caught:process.presubmit(folder)
        assert caught.value is primary and saved.read_bytes()==process._native_capture_state['candidate']
        assert not target.exists() and process.node.get_job_id() is None
