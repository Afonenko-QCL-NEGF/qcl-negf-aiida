"""T4 SOURCE_L: fresh fixtures and genuine ORM, never an installed/native verdict."""
import hashlib
import inspect
import pytest
from aiida_qcl_negf import native_actor,native_backend
from test_native_actor import native_graph


@pytest.mark.integration
def test_T4_original_four_are_actual_nodes_and_whole_raw(native_graph,monkeypatch):
    f=native_graph
    def forbidden(*a,**k):raise AssertionError('offline original4 touched runtime selector')
    monkeypatch.setattr(type(f['calc']),'get_remote_workdir',forbidden)
    monkeypatch.setattr(type(f['calc']),'get_job_id',forbidden)
    selection,pins,raw4=native_actor.read_native_performance_inputs(f['calc'].uuid,maximum_metadata_bytes=16*1024*1024)
    assert selection==dict(run_uuid=f['run'].uuid,execution_id=f['execution']['execution_id'],attempt=7,calcjob_uuid=f['calc'].uuid)
    for name,node,raw,wanted in zip(('plan','source_graph','native_execution','submit_script'),
            (f['plan'],f['graph'],f['native'],f['script']),raw4,
            (f['raw_plan'],f['raw_graph'],f['raw_native'],f['script'].get_content().encode())):
        assert raw==wanted
        assert pins[name]==dict(node_uuid=node.uuid,bytes=str(len(raw)),sha256=hashlib.sha256(raw).hexdigest())
    assert pins['submit_script']['node_uuid'] not in (f['plan'].uuid,f['calc'].uuid)


def test_T4_opt_in_literal_bool_refuses_before_profile_effects():
    assert inspect.signature(native_backend.open_native_selected_sources).parameters['include_performance'].default is False
    for value in (None,0,1,'true',[]):
        with pytest.raises(ValueError):
            native_backend.open_native_selected_sources('not-a-uuid',maximum_metadata_bytes=16<<20,maximum_io_bytes=8<<20,include_performance=value)

from copy import deepcopy
from pathlib import Path
from qcl_negf_contracts.messages import encode
from plumpy.process_states import PauseInterruption
from test_native_selected_sources import packet


def _phase0(p,mutate=None):
    root=dict(storage=deepcopy(p.f['execution']['storage']),area='native_execution',relative_path='performance')
    value=dict(schema='qcl-negf.performance-source-index.v1',contract_set='qcl-negf.results.v2',
        provider_selection=dict(run_uuid=p.f['run'].uuid,execution_id=p.f['execution']['execution_id'],attempt=7,calcjob_uuid=p.calc.uuid),
        generation='1',closed=True,technical=dict(root=root,commit_ref=dict(path='sessions/fixture/commit-1.json',bytes='1',sha256='a'*64,
            media_type='application/json',schema='qcl-negf.telemetry-commit.v2'),phase_context_refs=[],resume_context_refs=[]),entries=[])
    if mutate is not None:mutate(value)
    raw=encode(value);ref=dict(path='sources/'+hashlib.sha256(raw).hexdigest()+'.json',bytes=str(len(raw)),sha256=hashlib.sha256(raw).hexdigest(),
        media_type='application/json',schema=value['schema'])
    p.raw['performance-source.json']=raw;p.write()
    technical=p.root/'performance';(technical/'sources').mkdir(parents=True)
    (technical/ref['path']).write_bytes(raw)
    (technical/'current-source.json').write_bytes(encode(dict(schema='qcl-negf.performance-source-pointer.v1',index=ref)))
    p.calc.set_retrieve_list([*native_actor.CONTROL_FILES,'performance-source.json']);p.stored()
    return value,ref,raw,technical


@pytest.mark.integration
def test_T4_opt_in_phase0_maps_fixed_transaction_in_same_session(packet):
    p=packet;index,ref,raw,technical=_phase0(p)
    with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=16<<20,maximum_io_bytes=8<<20,include_performance=True) as (_,sources,operand):
        assert sources==() and operand==dict(raw_index=raw,root_mappings=((index['technical']['root'],technical),))
        assert all(t.is_open for t in p.transports)
    assert len(p.calls)==1 and not p.transports[-1].is_open


@pytest.mark.integration
@pytest.mark.parametrize('fault',['immutable','pointer','foreign_storage','workdir_technical','open_index','symlink'])
def test_T4_opt_in_rejects_unqualified_transaction_before_delivery(packet,fault):
    p=packet
    def mutate(index):
        if fault=='foreign_storage':index['technical']['root']['storage']['hold_ref']='foreign'
        elif fault=='workdir_technical':index['technical']['root']['area']='native_workdir'
        elif fault=='open_index':index['closed']=False
    index,ref,raw,technical=_phase0(p,mutate)
    if fault=='immutable':(technical/ref['path']).write_bytes(raw+b' ')
    elif fault=='pointer':(technical/'current-source.json').write_bytes(b'{}')
    elif fault=='symlink':
        original=technical/ref['path'];original.unlink();original.symlink_to(p.root/'performance-source.json')
    with pytest.raises((PauseInterruption,ValueError,OSError)):
        with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=16<<20,maximum_io_bytes=8<<20,include_performance=True):
            pytest.fail('unqualified technical transaction yielded')


@pytest.mark.integration
def test_T4_opt_in_verified_absence_distinct_from_empty(packet):
    p=packet;p.stored()
    with native_backend.open_native_selected_sources(p.calc.uuid,maximum_metadata_bytes=16<<20,maximum_io_bytes=8<<20,include_performance=True) as (_,_,operand):
        assert operand==dict(raw_index=None,root_mappings=())

from types import SimpleNamespace
from aiida import orm
from aiida.common.folders import SandboxFolder
from aiida.manage import get_manager
from aiida.schedulers.plugins.slurm import SlurmScheduler
from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.validation import scheduler_options
from qcl_negf_contracts.messages import decode
from aiida_qcl_negf import performance_binding,service


@pytest.fixture
def produced(native_graph,tmp_path,monkeypatch,request):
    """NEW tiny history-only payload, stock original script and genuine graph."""
    from test_native_selected_sources import _execution_graph
    f=(_execution_graph(native_graph,'execution',plan_size=113111)
       if getattr(request,'param',None)=='large_plan' else native_graph)
    monkeypatch.setattr(orm.Computer,'get_scheduler',lambda self:SlurmScheduler())
    monkeypatch.setenv('QCL_NEGF_NATIVE_JOBS_ROOT',str(tmp_path/'jobs'))
    parent=f.get('restart',f['run']) if f is native_graph else f['run']
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=parent.pk,inputs={
        **{name:f['calc'].inputs[name] for name in ('code','plan','source_graph','native_execution','execution_id','attempt','release_id')},
        'metadata':{'call_link_label':'performance_fixture','options':scheduler_options({})}})
    try:
        with SandboxFolder() as folder:process.presubmit(folder)
        f=dict(f,calc=process.node,script=process.node.outputs.native_submit_script)
        p=packet.__wrapped__(f,tmp_path,monkeypatch,SimpleNamespace(param=None))
        process.node.set_retrieve_list([*native_actor.CONTROL_FILES,'performance-source.json'])
        for name,raw in (('plan.json',f['raw_plan']),('source-graph.json',f['raw_graph']),('native-execution.json',f['raw_native'])):
            (p.work/name).write_bytes(raw)
        from result_v2_fixture import source,record,put,reference,encoded
        import h5py,numpy as np
        science=p.root/'science';row=record()
        identity=dict(row['identity'],execution_id=f['execution']['execution_id'],plan_sha256=hashlib.sha256(f['raw_plan']).hexdigest(),
            model_sha256=decode(f['raw_plan'])['points'][0]['effective_inputs']['model_sha256'])
        epoch='c1-'+process.node.uuid.replace('-','')+'-'+f['native'].uuid.replace('-','')+'-1'
        invocation=dict(source_epoch=epoch);row.update(identity=identity,invocation=invocation)
        state=source(science,[row]);manifest=decode((science/'manifest.json').read_bytes())
        with h5py.File(science/'records.h5','r+') as h:
            for name,value in (('identity_json',identity),('invocation_json',invocation)):
                del h['metadata/'+name];h['metadata/'+name]=np.bytes_(encoded(value))
        page=decode((science/'catalog.json').read_bytes());page.update(identity=identity,invocation=invocation)
        page['artifacts'][-1].update(reference(science,'records.h5','qcl-negf-scientific-history-v5','application/x-hdf5'))
        manifest.update(identity=identity,invocation=invocation,provenance=decode(f['raw_graph']))
        manifest['history']['head']=put(science,'catalog.json',page,page['schema'])
        state.update(identity=identity,manifest=put(science,'manifest.json',manifest,manifest['schema']))
        root=lambda area,path:dict(storage=deepcopy(f['execution']['storage']),area=area,relative_path=path)
        origin=dict(calcjob_uuid=process.node.uuid,native_execution_uuid=f['native'].uuid)
        anchor=dict(schema='qcl-negf.performance-anchor.v1',contract_set='qcl-negf.results.v2',phase_identity=identity,
            phase_source=dict(status='unavailable',session_id=None,current_path=None,reason='not_recorded'),origin=origin,invocation=invocation,
            scientific=dict(artifact_root='science',selected_state=state,manifest_utf8=(science/'manifest.json').read_text(),
                invocation=invocation,source_origin=origin,relation='produced'))
        raw=encode(anchor);anchor_ref=dict(path='performance-anchors/'+hashlib.sha256(raw).hexdigest()+'.json',bytes=str(len(raw)),
            sha256=hashlib.sha256(raw).hexdigest(),schema=anchor['schema'],media_type='application/json')
        (p.root/'performance-anchors').mkdir();(p.root/anchor_ref['path']).write_bytes(raw)
        from qcl_negf_results.capture import NativePerformanceInputs,NativePerformanceCapture
        from qcl_negf_results.telemetry import TelemetryWriter
        from qcl_negf_results._capture_io import ObserverIO
        planref=dict(path='plan.json',bytes=str(len(f['raw_plan'])),sha256=hashlib.sha256(f['raw_plan']).hexdigest(),schema=decode(f['raw_plan'])['schema'],media_type='application/json')
        provider=dict(run_uuid=f['run'].uuid,execution_id=f['execution']['execution_id'],attempt=7,calcjob_uuid=process.node.uuid)
        transport=dict(selection={k:provider[k] for k in ('calcjob_uuid','execution_id','attempt')},frozen_plan=planref)
        transport['selection']['plan_fingerprint']=decode(f['raw_plan'])['fingerprint']
        inputs=NativePerformanceInputs(provider_selection=provider,native_execution_uuid=f['native'].uuid,
            runner_anchor_root=root('native_execution','.'),technical_root=root('native_execution','performance'),
            sources=[dict(selected_state=state,artifact_root='science',scientific_root=root('native_execution','science'),scientific_directory=str(science),
                canonical_plan_root=root('native_workdir','.'),canonical_plan_directory=str(p.work),source_transport=transport)])
        budget=ObserverIO(128<<20);writer=TelemetryWriter(p.root/'performance',{}, {},transport=transport,technical_version=2,metadata_budget=budget)
        capture=NativePerformanceCapture(p.work,p.root,writer=writer,transport=transport,inputs=inputs,budget=budget)
        if getattr(request,'param',None)!='empty':capture.remember(anchor,anchor_ref,raw)
        writer.record_resource(dict(wall_seconds=1.));writer.flush()
        capture.publish(close_context=writer.close_context(),capture_status=dict(terminal_tail='unknown',scalar_live_route='unavailable',
            scalar_live_reason='external_root',performance_status='incomplete',reason='tail_unknown'))
        p.raw['performance-source.json']=(p.root/'performance-source.json').read_bytes();p.stored()
        yield SimpleNamespace(p=p,process=process,index=decode(p.raw['performance-source.json']),state=state,manifest=manifest,provider=provider)
    finally:process.close()


@pytest.mark.integration
@pytest.mark.parametrize("produced",[None,"large_plan"],indirect=True)
def test_T4_actual_history_publication_genuine_CREATE_offline_retained(produced,monkeypatch,record_property):
    f=produced
    # Product whole-operation default; file/format limits remain1MiB.
    from qcl_negf_results.state import StateReader
    with StateReader(f.p.root/'science',state_ref=f.state,expected_identity=f.state['identity']) as reader:
        reader.import_history();assert reader.history_batch(maximum_records=1).records
    pin=performance_binding.publish_native_performance(f.process)
    import json
    credit=f.process._t4_last_credit
    record_property('T4_publication_credit',json.dumps(dict(plan_bytes=len(f.p.f['raw_plan']),gross_metadata=credit.spent,prepaid_io=credit.prepaid_io,costs=credit.costs)))
    assert pin['sha256']==hashlib.sha256(f.p.raw['performance-source.json']).hexdigest()
    node=f.process.node.outputs.performance_source
    assert node.creator.uuid==f.process.node.uuid and node.filename=='performance-source.json'
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:pytest.fail('retained opened A/NFS'))
    result=service.get_performance_source(**f.provider)
    assert result['status']=='available' and result['index']['node']==pin
    assert result['origin']['input_nodes']['submit_script']['node_uuid']==f.p.f['script'].uuid
    assert result['retained']['native_result'] is None and result['retained']['native_binding'] is None


@pytest.mark.integration
def test_T4_current_locator_and_snapshot_are_original_metadata_without_NFS(produced,monkeypatch):
    f=produced
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:pytest.fail('getter opened NFS'))
    from qcl_negf_contracts.telemetry import validate_performance_source
    locator=service.get_performance_source(**f.provider,mode='current')
    assert locator['stage']=='current_locator' and locator['locator']['original_inputs']['submit_script']['node_uuid']==f.p.f['script'].uuid
    key=f.index['entries'][0]['anchor_key'];raw=f.p.raw['performance-source.json']
    snapshot=dict(index_ref=performance_binding._reference(raw),index_utf8=raw.decode(),anchor_key=key)
    available=service.get_performance_source(**f.provider,mode='current',current_snapshot=snapshot)
    assert validate_performance_source(available)==available and available['status']=='available'
    assert available['origin']['mode']=='current' and available['index']['node'] is None
    assert available['integrity']==performance_binding._integrity('verified_original_inputs')
    with pytest.raises(ValueError):service.get_performance_source(**f.provider,mode='retained',current_snapshot=snapshot)


@pytest.mark.integration
@pytest.mark.parametrize('fault',['foreign_origin','model','graph','canonical_root','foreign_storage','source_attempt','open','raw_index'])
def test_T4_coherent_snapshot_still_requires_every_original_relation(produced,monkeypatch,fault):
    f=produced
    index=deepcopy(f.index);entry=index['entries'][0];anchor=decode(entry['runner_anchor_utf8'].encode())
    if fault in ('foreign_origin','model','graph'):
        manifest=decode(anchor['scientific']['manifest_utf8'].encode())
        if fault=='foreign_origin':
            fake='00000000-0000-4000-8000-000000000098';epoch='c1-'+fake.replace('-','')+'-'+f.p.f['native'].uuid.replace('-','')+'-1'
            anchor['origin']['calcjob_uuid']=fake;anchor['scientific']['source_origin']['calcjob_uuid']=fake
            for value in (anchor,anchor['scientific'],manifest):value['invocation']['source_epoch']=epoch
        elif fault=='model':
            manifest['identity']['model_sha256']='e'*64;anchor['phase_identity']['model_sha256']='e'*64
        else:manifest['provenance']['revision']='e'*40
        mraw=encode(manifest);anchor['scientific']['manifest_utf8']=mraw.decode()
        state=anchor['scientific']['selected_state'];state['identity']=deepcopy(manifest['identity'])
        state['manifest'].update(bytes=str(len(mraw)),sha256=hashlib.sha256(mraw).hexdigest())
        entry['telemetry']['expected_producer_provenance']=manifest['provenance']
        araw=encode(anchor);entry['runner_anchor_utf8']=araw.decode();entry['runner_anchor_ref'].update(path='performance-anchors/'+hashlib.sha256(araw).hexdigest()+'.json',bytes=str(len(araw)),sha256=hashlib.sha256(araw).hexdigest())
        operands=dict(scientific_root=entry['scientific_root'],selected_state=state,invocation=anchor['scientific']['invocation'],
            source_transport=entry['telemetry']['source_transport'],source_origin=anchor['scientific']['source_origin'])
        entry['anchor_key']=hashlib.sha256(encode(operands)).hexdigest()
    elif fault=='canonical_root':entry['canonical_plan_root']['relative_path']='science'
    elif fault=='foreign_storage':entry['canonical_plan_root']['storage']['hold_ref']='foreign'
    elif fault=='source_attempt':
        entry['telemetry']['source_transport']['selection']['attempt']=8
        entry['telemetry']['expected_transport']['selection']['attempt']=8
    elif fault=='open':index['closed']=False
    raw=encode(index)
    if fault=='raw_index':raw+=b' extra'
    snapshot=dict(index_ref=performance_binding._reference(raw),index_utf8=raw.decode(),anchor_key=entry['anchor_key'])
    refused=service.get_performance_source(**f.provider,mode='current',current_snapshot=snapshot)
    assert refused['status']=='unavailable' and refused['integrity']['origin']=='not_qualified'


@pytest.mark.integration
@pytest.mark.parametrize('fault',['A_close','reader_close','cancel'])
def test_T4_no_output_before_all_closes_and_primary_cancel(produced,monkeypatch,fault):
    f=produced
    from qcl_negf_results.state import StateReader
    if fault=='A_close':
        original=native_backend._Packet.close
        def close(packet):original(packet);raise OSError('A final close unknown')
        monkeypatch.setattr(native_backend._Packet,'close',close)
    else:
        original=StateReader.close
        def close(reader):original(reader);raise OSError('reader close secondary')
        monkeypatch.setattr(StateReader,'close',close)
        if fault=='cancel':
            primary=KeyboardInterrupt('T4 original cancel')
            import qcl_negf_results.performance_views as views
            monkeypatch.setattr(views,'read_performance_context',lambda *a,**k:(_ for _ in ()).throw(primary))
    with pytest.raises(KeyboardInterrupt if fault=='cancel' else (OSError,PauseInterruption)) as caught:
        performance_binding.publish_native_performance(f.process,maximum=512<<20)
    assert 'performance_source' not in f.process.node.outputs
    assert not f.p.transports[-1].is_open
    if fault=='cancel':assert caught.value is primary and any('reader close' in note for note in primary.__notes__)


@pytest.fixture
def t4_pg_profile():
    """Only the Root-created NEW disposable T4 DB, enabled explicitly by L harness."""
    import json,os
    operand_path=os.environ.get('P30_T4_PG_OPERANDS')
    if not operand_path:pytest.skip('explicit private T4 PG operands absent')
    from aiida.manage.configuration import get_config,profile_context
    from aiida.manage.configuration.profile import Profile
    from aiida.storage.psql_dos.backend import PsqlDosBackend
    from aiida.storage.psql_dos.utils import create_sqlalchemy_engine
    receipt=json.loads(Path(operand_path).read_text());storage=dict(receipt['operands'])
    assert storage['database_name']=='p30_t4_native_20261031'
    repository=Path(os.environ['P30_T4_PG_REPOSITORY']).absolute()
    storage['repository_uri']=repository.as_uri()
    profile=Profile('p30-t4-private-source-l',dict(storage=dict(backend='core.psql_dos',config=storage),
        process_control=dict(backend=None,config=None),test_profile=True,default_user_email='t4@localhost'))
    assert profile.storage_config['engine_kwargs']==storage['engine_kwargs']
    engine=create_sqlalchemy_engine(profile.storage_config)
    try:
        with engine.connect() as connection:
            server=connection.exec_driver_sql("SELECT current_setting('server_version_num'),current_setting('server_encoding'),inet_server_addr() IS NULL,current_database() ").all()
            assert server==[('170011','UTF8',True,'p30_t4_native_20261031')]
            tables=connection.exec_driver_sql("SELECT count(*) FROM information_schema.tables WHERE table_schema='public'").scalar()
    finally:engine.dispose()
    if not tables:PsqlDosBackend.initialise(profile,reset=False)
    else:assert repository.exists(),'do not adopt another repository/schema'
    config=get_config(create=True);config.add_profile(profile);config.store()
    with profile_context(profile,allow_switch=True):
        backend=get_manager().get_profile_storage()
        assert type(backend) is PsqlDosBackend and PsqlDosBackend.version_head()=='main_0002'
        if orm.User.collection.get_default() is None:orm.User(email='t4@localhost').store()
        yield profile


def _binary(node):
    with node.open(mode='rb') as stream:return stream.read()


def _pad_new_cp_plan(f,size):
    """Only newly manufactured fixture bytes, before any original nodes/reads."""
    import h5py
    from final_diagnostics_fixture import fixed_json
    from result_v2_fixture import put,reference
    root=f['root'];plan=decode((root/'plan.json').read_bytes())
    plan['nodes']=[dict(estimated_memory_bytes=256,stage_id='standalone',required_evidence=[],comparable_fingerprint=plan['fingerprint'],
        execution_id=plan['executions'][0]['id'],comparison_reference=None,forbidden_evidence=[],priority=0,reserve=False,
        comparison_kind='controlled_axis',controlled_paths=[],depends_on=[])]
    raw=encode(plan)
    assert len(raw)<size
    raw+=b' '*(size-len(raw));(root/'plan.json').write_bytes(raw)
    identity=dict(f['manifest']['identity'],plan_sha256=hashlib.sha256(raw).hexdigest())
    replacements={'plan.json':reference(root,'plan.json','qcl-negf-scientific-plan-v2')}
    def rewrite(value):
        if type(value) is dict:
            if {'path','bytes','sha256','schema','media_type'}<=set(value) and value['path'] in replacements:value.update(replacements[value['path']])
            if set(value)==set(identity):value.update(identity)
            for child in value.values():rewrite(child)
        elif type(value) is list:
            for child in value:rewrite(child)
    with h5py.File(root/'full.h5','r+') as h:
        del h['metadata/identity_json'];fixed_json(h,'metadata/identity_json',identity)
    replacements['full.h5']=reference(root,'full.h5','qcl-negf-physics-v5','application/x-hdf5')
    cp=decode((root/'checkpoint.json').read_bytes());rewrite(cp)
    replacements['checkpoint.json']=put(root,'checkpoint.json',cp,cp['schema'])
    for name,dataset in (('analysis.h5','final_diagnostics/declaration_json'),('observations.h5','final_observations/declaration_json')):
        with h5py.File(root/name,'r+') as h:
            value=decode(bytes(h[dataset][()]));rewrite(value)
            del h['metadata/identity_json'];fixed_json(h,'metadata/identity_json',identity)
            del h[dataset];fixed_json(h,dataset,value)
        replacements[name]=reference(root,name,'qcl-negf-physics-analysis-v5','application/x-hdf5')
    rewrite(f['manifest']);f['source']['selected_state']['identity']=identity
    f['source']['selected_state']['manifest']=put(root,'selected.json',f['manifest'],f['manifest']['schema'])
    f['source']['diagnostics'].update(replacements['analysis.h5']);f['source']['observations'].update(replacements['observations.h5'])
    f['refs']['plan'].update(replacements['plan.json'])


def _cp_current(base,tmp_path,monkeypatch):
    """Fresh CP payload and actual CalcJob owner; fixture transport is explicit."""
    from test_native_parser import scientific_access,mapped_source
    from aiida.common.links import LinkType
    import shutil
    f=scientific_access.__wrapped__(base,tmp_path,monkeypatch,SimpleNamespace(param=False))
    _pad_new_cp_plan(f,113111)
    child=f['child'];run=child.caller
    # Fresh immutable original nodes after the NEW padded scientific fixture is
    # complete; no stored original input or published scientific file is edited.
    import io
    raw_plan=(f['root']/'plan.json').read_bytes()
    plan=orm.SinglefileData(file=io.BytesIO(raw_plan),filename='plan.json').store()
    native=orm.SinglefileData(file=io.BytesIO(b'{}'),filename='native-execution.json')
    with child.inputs.native_execution.open(mode='rb') as stream:declaration=decode(stream.read())
    declaration['input_plan']=dict(node_uuid=plan.uuid,bytes=str(len(raw_plan)),sha256=hashlib.sha256(raw_plan).hexdigest())
    declaration['storage']['execution_binding_uuid']=native.uuid
    native.set_file(io.BytesIO(encode(declaration)),filename='native-execution.json');native.store()
    run=orm.WorkChainNode(process_type=run.process_type)
    for name,node in (('plan',plan),('source_graph',child.inputs.source_graph),('code',child.inputs.code),('release_id',child.inputs.release_id),('native_executions__execution_0',native)):
        run.base.links.add_incoming(node,LinkType.INPUT_WORK,name)
    run.store()
    monkeypatch.setattr(orm.Computer,'get_scheduler',lambda self:SlurmScheduler())
    monkeypatch.setenv('QCL_NEGF_NATIVE_JOBS_ROOT',str(tmp_path/'jobs'))
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=run.pk,inputs={
        **{name:child.inputs[name] for name in ('code','plan','source_graph','native_execution','execution_id','attempt','release_id')},
        'plan':plan,'native_execution':native,
        'metadata':{'call_link_label':'T4_cp_owner','options':scheduler_options({})}})
    try:
        f['child']=process.node;mapped_source(f,script_link='none')
        with SandboxFolder() as folder:process.presubmit(folder)
        original=dict(calc=process.node,run=run,plan=plan,graph=child.inputs.source_graph,native=native,
            code=child.inputs.code,script=process.node.outputs.native_submit_script,
            raw_plan=_binary(plan),raw_graph=_binary(child.inputs.source_graph),
            raw_native=_binary(native),execution=decode(_binary(native)))
        p=packet.__wrapped__(original,tmp_path,monkeypatch,SimpleNamespace(param=None))
        process.node.set_retrieve_list([*native_actor.CONTROL_FILES,'performance-source.json'])
        shutil.copytree(f['root'],p.root/'science')
        f['source'].update(kind='checkpoint',artifact_root='science',diagnostics=None,observations=None,reason='final_not_published')
        f['result']['attempt_history']=deepcopy(f['result']['points'])
        p.raw['series_result.json']=encode(f['result'])
        p.raw['native-selection.json']=encode(dict(version=1,selected_inputs=[dict(artifact_root='science',selected_state=f['source']['selected_state'],plan=f['refs']['plan'],model=f['refs']['model'])]))
        for name,raw in (('plan.json',original['raw_plan']),('source-graph.json',original['raw_graph']),('native-execution.json',original['raw_native'])):(p.work/name).write_bytes(raw)
        root=lambda area,path:dict(storage=deepcopy(original['execution']['storage']),area=area,relative_path=path)
        origin=dict(calcjob_uuid=process.node.uuid,native_execution_uuid=original['native'].uuid)
        state=f['source']['selected_state'];manifest=f['manifest']
        anchor=dict(schema='qcl-negf.performance-anchor.v1',contract_set='qcl-negf.results.v2',phase_identity=state['identity'],
            phase_source=dict(status='unavailable',session_id=None,current_path=None,reason='not_recorded'),origin=origin,invocation=manifest['invocation'],
            scientific=dict(artifact_root='science',selected_state=state,manifest_utf8=(p.root/'science'/state['manifest']['path']).read_text(),
                invocation=manifest['invocation'],source_origin=origin,relation='produced'))
        raw=encode(anchor);ref=dict(path='performance-anchors/'+hashlib.sha256(raw).hexdigest()+'.json',bytes=str(len(raw)),sha256=hashlib.sha256(raw).hexdigest(),schema=anchor['schema'],media_type='application/json')
        (p.root/'performance-anchors').mkdir();(p.root/ref['path']).write_bytes(raw)
        from qcl_negf_results.capture import NativePerformanceInputs,NativePerformanceCapture
        from qcl_negf_results.telemetry import TelemetryWriter
        from qcl_negf_results._capture_io import ObserverIO
        provider=dict(run_uuid=run.uuid,execution_id=child.inputs.execution_id.value,attempt=7,calcjob_uuid=process.node.uuid)
        planref=dict(path='plan.json',bytes=str(len(original['raw_plan'])),sha256=hashlib.sha256(original['raw_plan']).hexdigest(),schema='qcl-negf-scientific-plan-v2',media_type='application/json')
        transport=dict(selection={k:provider[k] for k in ('calcjob_uuid','execution_id','attempt')},frozen_plan=planref)
        transport['selection']['plan_fingerprint']=decode(original['raw_plan'])['fingerprint']
        inputs=NativePerformanceInputs(provider_selection=provider,native_execution_uuid=original['native'].uuid,runner_anchor_root=root('native_execution','.'),technical_root=root('native_execution','performance'),sources=[dict(
            selected_state=state,artifact_root='science',scientific_root=root('native_execution','science'),scientific_directory=str(p.root/'science'),canonical_plan_root=root('native_workdir','.'),canonical_plan_directory=str(p.work),source_transport=transport)])
        budget=ObserverIO(128<<20);writer=TelemetryWriter(p.root/'performance',{}, {},transport=transport,technical_version=2,metadata_budget=budget)
        capture=NativePerformanceCapture(p.work,p.root,writer=writer,transport=transport,inputs=inputs,budget=budget)
        capture.remember(anchor,ref,raw);writer.record_resource(dict(wall_seconds=1.));writer.flush()
        capture.publish(close_context=writer.close_context(),capture_status=dict(terminal_tail='unknown',scalar_live_route='unavailable',scalar_live_reason='external_root',performance_status='incomplete',reason='tail_unknown'))
        p.raw['performance-source.json']=(p.root/'performance-source.json').read_bytes();p.write();p.stored()
        yield SimpleNamespace(p=p,process=process,provider=provider,state=state,index=decode(p.raw['performance-source.json']))
    finally:process.close()


@pytest.mark.integration
def test_T4_PG_actual_CP_owner_B_scalar_source_current_retained(t4_pg_profile,tmp_path,monkeypatch,record_property):
    import json
    def local():
        computer=orm.Computer(label='T4-'+str(len(orm.Computer.collection.all())),hostname='localhost',transport_type='core.local',scheduler_type='core.direct',workdir='/tmp').store()
        computer.configure();return computer
    observed=[];base_credit=performance_binding._T4Credit
    class Credit(base_credit):
        def __init__(self,*a,**k):super().__init__(*a,**k);observed.append(self)
    monkeypatch.setattr(performance_binding,'_T4Credit',Credit)
    base=native_graph.__wrapped__(None,local)
    fixture=_cp_current(base,tmp_path,monkeypatch);f=next(fixture)
    try:
        # Real parser -> genuine B pair/scalar -> optional T4 actual owner hook.
        code=f.process.parse_retrieved_output()
        assert code is not None and 'selection' in f.process.node.outputs and 'performance_source' in f.process.node.outputs
        credit=f.process._t4_last_credit
        record_property('T4_CP_credit',json.dumps(dict(gross_metadata=credit.spent,prepaid_io=credit.prepaid_io,costs=credit.costs)))
        default=service.get_native_result_source(**f.provider,point_id=f.state['identity']['point_id'])
        assert default['status']=='available'
        monkeypatch.setattr(orm.ProcessNode,'called_descendants',property(lambda self:pytest.fail('T4 public B descendant walk')))
        result=service.get_performance_source(**f.provider)
        retained_credit=observed[-1]
        assert result['status']=='available' and result['retained']['native_binding']['node_uuid']==f.process.node.outputs.native_binding.uuid
        assert result['retained']['native_result']['node_uuid']==f.process.node.outputs.result.uuid
        raw=f.p.raw['performance-source.json'];key=f.index['entries'][0]['anchor_key']
        snapshot=dict(index_ref=performance_binding._reference(raw),index_utf8=raw.decode(),anchor_key=key)
        assert service.get_performance_source(**f.provider,mode='current',current_snapshot=snapshot)['status']=='available'
        assert f.process.node.outputs.selection.get_dict()['performance_status']=='unavailable'
        locator=service.get_performance_source(**f.provider,mode='current')
        assert locator['stage']=='current_locator'
        record_property('T4_complete_credits',json.dumps([dict(gross_metadata=c.spent,prepaid_io=c.prepaid_io,costs=c.costs) for c in observed]))
        # Exact and one-short full offline operation: the last repository read
        # is denied before its open; no existing node or scalar is modified.
        output=f.process.node.outputs.performance_source;opened=[];original_open=orm.SinglefileData.open
        def opened_output(node,*a,**k):
            if node.uuid==output.uuid:opened.append(node.uuid)
            return original_open(node,*a,**k)
        monkeypatch.setattr(orm.SinglefileData,'open',opened_output)
        exact=performance_binding.native_source(**f.provider,maximum=retained_credit.spent)
        assert exact['status']=='available' and len(opened)==2
        opened.clear()
        short=performance_binding.native_source(**f.provider,maximum=retained_credit.spent-1)
        assert short['status']=='unavailable' and short['reason']=='budget_exceeded' and len(opened)==1
        assert service._T4_NATIVE_RESULT_PROVIDER.get() is None
        assert performance_binding.retained_source(f.process.node)['status']=='available'

        # Admission of the complete finite selector is separate from the full
        # B allowance. One byte short refuses before the public B call; exact
        # admission reaches the real B proof, then has no later row-copy credit.
        credit=base_credit(performance_binding.NATIVE_METADATA)
        original=performance_binding._original(f.process.node.uuid,credit)
        items,_=performance_binding._entry_origins(f.index,original,credit)
        from aiida_qcl_negf.saved_sources import _NATIVE_METADATA
        selector_boundary=4096+_NATIVE_METADATA+512+service._T4_SELECTOR_ALLOWANCE+512
        called=[];actual_B=service.get_native_result_source
        def public_B(*a,**k):called.append(True);return actual_B(*a,**k)
        with monkeypatch.context() as patch:
            patch.setattr(service,'get_native_result_source',public_B)
            with pytest.raises(ValueError,match='selector full allowance'):
                performance_binding._association(items[0],base_credit(selector_boundary-1),{})
            assert called==[]
            with pytest.raises(ValueError,match='relation workspace'):
                performance_binding._association(items[0],base_credit(selector_boundary),{})
            assert called==[True]
        assert service._T4_NATIVE_RESULT_PROVIDER.get() is None

        # Real finite ORM selection has already run before this repository
        # cancellation fault. The private scope must unwind the exact primary.
        from aiida_qcl_negf import saved_sources
        primary=KeyboardInterrupt('T4 actual B repository cancellation')
        with monkeypatch.context() as patch:
            patch.setattr(saved_sources,'_retained_native_result_source',lambda *a,**k:(_ for _ in ()).throw(primary))
            with pytest.raises(KeyboardInterrupt) as caught:
                performance_binding._association(items[0],base_credit(performance_binding.NATIVE_METADATA),{})
            assert caught.value is primary
        assert service._T4_NATIVE_RESULT_PROVIDER.get() is None
        _borrowed_metadata_current(f,monkeypatch,record_property)

    finally:
        try:next(fixture)
        except StopIteration:pass


@pytest.mark.integration
@pytest.mark.parametrize('signal_name',['KillInterruption','PauseInterruption','KeyboardInterrupt','SystemExit',None,'enter'])
def test_T4_pending_output_F1_public_parse_keeps_local_primary(native_graph,tmp_path,monkeypatch,signal_name):
    from contextlib import contextmanager
    from plumpy import process_states
    fixture=_cp_current(native_graph,tmp_path,monkeypatch);f=next(fixture)
    signals=dict(KillInterruption=process_states.KillInterruption,PauseInterruption=process_states.PauseInterruption,
        KeyboardInterrupt=KeyboardInterrupt,SystemExit=SystemExit,enter=process_states.KillInterruption)
    primary=signals[signal_name]('pending output own primary') if signal_name else None
    secondary=OSError('pending output secondary close');real=orm.SinglefileData.open;hits=[];streams=[];updates=[]
    original_update=f.process.update_outputs
    def updated():updates.append(True);return original_update()
    @contextmanager
    def opening(node,*a,**k):
        with real(node,*a,**k) as stream:
            if node.filename!='series_result.json' or node.is_stored:yield stream;return
            streams.append(stream)
            if signal_name=='enter':hits.append('enter');raise primary
            class Proxy:
                def __getattr__(self,name):return getattr(stream,name)
                def read(self,*args):
                    hits.append('read')
                    if primary is not None:raise primary
                    return stream.read(*args)
            try:yield Proxy()
            finally:hits.append('secondary_close');raise secondary
    monkeypatch.setattr(orm.SinglefileData,'open',opening);monkeypatch.setattr(f.process,'update_outputs',updated)
    try:
        try:raise RuntimeError('unrelated caller exception')
        except RuntimeError as ambient:
            if primary is not None:
                with pytest.raises(type(primary)) as caught:f.process.parse_retrieved_output()
                assert caught.value is primary and caught.value is not ambient
            else:
                assert f.process.parse_retrieved_output().status==302
        assert hits==(['enter'] if signal_name=='enter' else ['read','secondary_close'])
        assert streams and all(stream.closed for stream in streams) and updates==[]
        assert not any(name in f.process.node.outputs for name in ('result','native_binding','selection','performance_source'))
        assert service._T4_NATIVE_RESULT_PROVIDER.get() is None
    finally:
        try:next(fixture)
        except StopIteration:pass


@pytest.mark.integration
def test_T4_pending_output_F1_normal_negative_keeps_CREATE_order_and_public_APIs(native_graph,tmp_path,monkeypatch):
    fixture=_cp_current(native_graph,tmp_path,monkeypatch);f=next(fixture)
    trace=[];original=f.process.update_outputs
    def updated():
        result=original();trace.append(set(f.process.node.outputs));return result
    monkeypatch.setattr(f.process,'update_outputs',updated)
    try:
        assert f.process.parse_retrieved_output().status==302
        first={name:next(i for i,names in enumerate(trace) if name in names) for name in ('result','native_binding','selection','performance_source')}
        assert first['result']==first['native_binding']<first['selection']<first['performance_source']
        for name in first:assert f.process.node.outputs[name].creator.uuid==f.process.node.uuid
        for point in (f.state['identity']['point_id'],None):
            assert service.get_native_result_source(**f.provider,point_id=point)['status']=='available'
        assert service.get_performance_source(**f.provider)['status']=='available'
        raw=f.p.raw['performance-source.json'];key=f.index['entries'][0]['anchor_key']
        snapshot=dict(index_ref=performance_binding._reference(raw),index_utf8=raw.decode(),anchor_key=key)
        assert service.get_performance_source(**f.provider,mode='current',current_snapshot=snapshot)['status']=='available'
        assert f.process.node.outputs.selection.get_dict()['performance_status']=='unavailable'
    finally:
        try:next(fixture)
        except StopIteration:pass


def _borrowed_metadata_current(f,monkeypatch,record_property):
    """Actual parent CREATE and stock child script; snapshot is metadata only."""
    from aiida.common.links import LinkType
    from qcl_negf_contracts.telemetry import validate_performance_source_index
    binding=f.process.node.outputs.native_binding
    setup=QCLExecutionCalculation._setup_inputs
    def parent_input(owner):
        setup(owner)
        owner.node.base.links.add_incoming(binding,LinkType.INPUT_CALC,'parent_binding')
    # Manufacture the INPUT during engine setup, before the node is stored.
    # This is an explicit fixture graph, not a new public restart producer.
    with monkeypatch.context() as patch:
        patch.setattr(QCLExecutionCalculation,'_setup_inputs',parent_input)
        process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=f.process.node.caller.pk,inputs={
            **{name:f.process.inputs[name] for name in ('code','plan','source_graph','native_execution','execution_id','attempt','release_id')},
            'metadata':{'call_link_label':'T4_borrowed_metadata','options':scheduler_options({})}})
    try:
        with SandboxFolder() as folder:process.presubmit(folder)
        provider=dict(f.provider,calcjob_uuid=process.node.uuid)
        index=deepcopy(f.index);index['provider_selection']=provider
        entry=index['entries'][0];anchor=decode(entry['runner_anchor_utf8'].encode())
        anchor['origin']['calcjob_uuid']=process.node.uuid
        anchor['invocation']['source_epoch']='c1-'+process.node.uuid.replace('-','')+'-'+process.inputs.native_execution.uuid.replace('-','')+'-1'
        anchor['scientific']['relation']='borrowed'
        raw=encode(anchor);entry['runner_anchor_utf8']=raw.decode()
        entry['runner_anchor_ref'].update(path='performance-anchors/'+hashlib.sha256(raw).hexdigest()+'.json',bytes=str(len(raw)),sha256=hashlib.sha256(raw).hexdigest())
        entry['telemetry']['expected_transport']['selection']['calcjob_uuid']=process.node.uuid
        validate_performance_source_index(index)
        raw=encode(index);key=entry['anchor_key']
        snapshot=dict(index_ref=performance_binding._reference(raw),index_utf8=raw.decode(),anchor_key=None)
        pending=service.get_performance_source(**provider,mode='current',current_snapshot=snapshot)
        assert pending['reason']=='anchor_selector_required'
        snapshot['anchor_key']=key
        available=service.get_performance_source(**provider,mode='current',current_snapshot=snapshot)
        assert available['status']=='available'
        assert available['origin']['source_calcjob_uuid']==f.process.node.uuid
        assert available['origin']['hops']==[dict(child_calcjob_uuid=process.node.uuid,parent_binding_uuid=binding.uuid)]
        assert available['retained']['native_binding']['node_uuid']==binding.uuid
        record_property('T4_borrowed_scope','Actual parent INPUT/CREATE + original4 stock script + public B; metadata snapshot only, current payload access not checked')
    finally:process.close()


@pytest.mark.integration
@pytest.mark.parametrize('direct',[False,True])
def test_T4_actual_finite_CALL_selection_and_foreign_operands(native_graph,monkeypatch,direct):
    from aiida.common.links import LinkType
    f=native_graph;child=f['calc']
    if direct:
        child=orm.CalcJobNode(computer=child.computer,process_type=child.process_type)
        child.base.links.add_incoming(f['run'],LinkType.CALL_CALC,'T4_direct')
        for link in f['calc'].base.links.get_incoming(link_type=LinkType.INPUT_CALC).all():child.base.links.add_incoming(link.node,LinkType.INPUT_CALC,link.link_label)
        child.store()
    sibling=orm.CalcJobNode(computer=child.computer,process_type=child.process_type)
    sibling.base.links.add_incoming(f['run'],LinkType.CALL_CALC,'T4_unrelated');sibling.store()
    provider=dict(run_uuid=f['run'].uuid,execution_id=f['execution']['execution_id'],attempt=7,calcjob_uuid=child.uuid)
    monkeypatch.setattr(orm.ProcessNode,'called_descendants',property(lambda self:pytest.fail('T4 descendant walk')))
    with service._performance_result_scope(provider):
        assert service._performance_result_child(**provider)[0].uuid==child.uuid
    for field,value in (('run_uuid',sibling.uuid),('execution_id','foreign'),('attempt',8),('calcjob_uuid',sibling.uuid)):
        changed=dict(provider,**{field:value})
        with service._performance_result_scope(changed),pytest.raises(ValueError):service._performance_result_child(**changed)
    assert service._T4_NATIVE_RESULT_PROVIDER.get() is None


@pytest.mark.integration
@pytest.mark.parametrize('produced',['empty'],indirect=True)
def test_T4_phase_zero_retained_is_staging_without_scientific_B_pair(produced,monkeypatch):
    f=produced
    performance_binding.publish_native_performance(f.process,maximum=512<<20)
    result=service.get_performance_source(**f.provider)
    assert result['status']=='staging' and result['reason']=='scientific_anchor_pending'
    assert not {'result','native_binding','selection'}&set(f.process.node.outputs)


@pytest.mark.integration
def test_T4_missing_retained_CREATE_is_explicit_and_offline(produced,monkeypatch):
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:pytest.fail('missing retained touched NFS'))
    result=service.get_performance_source(**produced.provider)
    assert result['status']=='unavailable' and result['reason']=='retained_performance_source_missing'
    assert result['stage']=='origin_qualification'


@pytest.mark.integration
def test_T4_late_technical_change_after_public_reader_blocks_CREATE(produced,monkeypatch):
    f=produced
    from qcl_negf_results import performance_views
    original=performance_views.read_performance_context
    def changed(*a,**k):
        result=original(*a,**k)
        file=f.p.root/'performance'/f.index['entries'][0]['telemetry']['binding_ref']['path']
        file.write_bytes(file.read_bytes()+b' ')
        return result
    monkeypatch.setattr(performance_views,'read_performance_context',changed)
    with pytest.raises((ValueError,PauseInterruption,OSError)):
        performance_binding.publish_native_performance(f.process,maximum=640<<20)
    assert 'performance_source' not in f.process.node.outputs and not f.p.transports[-1].is_open


def _second_snapshot_entry(first):
    """NEW metadata-only assertion; no claim of a second physical payload."""
    entry=deepcopy(first);anchor=decode(entry['runner_anchor_utf8'].encode())
    manifest=decode(anchor['scientific']['manifest_utf8'].encode())
    manifest.update(state_id='second-metadata-state',state_sequence='2',generation='2')
    raw=encode(manifest);state=anchor['scientific']['selected_state']
    state.update(state_id=manifest['state_id'],state_sequence='2')
    state['manifest'].update(path='second-manifest.json',bytes=str(len(raw)),sha256=hashlib.sha256(raw).hexdigest())
    anchor['scientific'].update(artifact_root='second-science',manifest_utf8=raw.decode())
    entry['scientific_root']['relative_path']='second-science'
    raw=encode(anchor);entry['runner_anchor_utf8']=raw.decode()
    entry['runner_anchor_ref'].update(path='performance-anchors/'+hashlib.sha256(raw).hexdigest()+'.json',bytes=str(len(raw)),sha256=hashlib.sha256(raw).hexdigest())
    operands=dict(scientific_root=entry['scientific_root'],selected_state=state,invocation=anchor['scientific']['invocation'],
        source_transport=entry['telemetry']['source_transport'],source_origin=anchor['scientific']['source_origin'])
    entry['anchor_key']=hashlib.sha256(encode(operands)).hexdigest()
    return entry


@pytest.mark.integration
def test_T4_multiple_metadata_entries_require_selector_and_unselected_roots_are_checked(produced):
    from qcl_negf_contracts.telemetry import validate_performance_source_index,validate_performance_source
    f=produced;index=deepcopy(f.index);index['entries'].append(_second_snapshot_entry(index['entries'][0]))
    validate_performance_source_index(index)
    def snapshot(key=None):
        raw=encode(index);return dict(index_ref=performance_binding._reference(raw),index_utf8=raw.decode(),anchor_key=key)
    pending=service.get_performance_source(**f.provider,mode='current',current_snapshot=snapshot())
    assert validate_performance_source(pending)==pending and pending['reason']=='anchor_selector_required' and len(pending['candidates'])==2
    key=index['entries'][0]['anchor_key']
    selected=service.get_performance_source(**f.provider,mode='current',current_snapshot=snapshot(key))
    assert selected['status']=='available' and selected['anchor_key']==key and selected['integrity']['current_access']=='not_checked'
    index['entries'][1]['canonical_plan_root']['relative_path']='unselected-foreign-root'
    validate_performance_source_index(index)
    rejected=service.get_performance_source(**f.provider,mode='current',current_snapshot=snapshot(key))
    assert rejected['status']=='unavailable' and rejected['reason']=='origin_mismatch'


from test_native_parser import native_process as native_process


@pytest.mark.integration
def test_T4_optional_owner_preserves_engine_kill(native_process,monkeypatch):
    from plumpy.process_states import KillInterruption
    process,_,_=native_process;primary=KillInterruption('original T4 engine cancellation')
    monkeypatch.setattr(performance_binding,'publish_native_performance',lambda *a,**k:(_ for _ in ()).throw(primary))
    with pytest.raises(KillInterruption) as caught:process.parse_retrieved_output()
    assert caught.value is primary and 'performance_source' not in process.node.outputs


@pytest.mark.integration
def test_T4_partial_CREATE_preserves_genuine_residual_primary_and_reuses_it(produced,monkeypatch):
    f=produced;created=[];primary=OSError('T4 failure after actual output store')
    original_store=orm.SinglefileData.store
    def stored(node,*a,**k):
        result=original_store(node,*a,**k)
        if node.filename=='performance-source.json':created.append(node);raise primary
        return result
    with monkeypatch.context() as patch:
        patch.setattr(orm.SinglefileData,'store',stored)
        with pytest.raises(OSError) as caught:performance_binding.publish_native_performance(f.process)
        assert caught.value is primary
    assert len(created)==1 and created[0].is_stored
    assert created[0].creator.uuid==f.process.node.uuid
    assert _binary(created[0])==f.p.raw['performance-source.json']
    again=performance_binding.publish_native_performance(f.process)
    assert again['node_uuid']==created[0].uuid


def test_T4_held_metadata_FD_rejects_identical_named_replacement_and_closes(tmp_path):
    import os
    path=tmp_path/'metadata.json';path.write_bytes(b'{}')
    tree=native_backend._Tree(str(tmp_path),performance_binding._T4Credit(performance_binding.NATIVE_METADATA))
    try:
        assert tree.read('metadata.json',2,hold=True)==b'{}'
        fd=tree.held['metadata.json']
        path.rename(tmp_path/'original.json');path.write_bytes(b'{}')
        with pytest.raises(PauseInterruption):tree.check()
    finally:tree.close()
    with pytest.raises(OSError):os.fstat(fd)


@pytest.mark.integration
@pytest.mark.parametrize('boundary',['singlefile','retrieved','packet'])
@pytest.mark.parametrize('signal_name',['KillInterruption','KeyboardInterrupt','SystemExit',None,'enter'])
def test_T4_repair_repository_primary_and_success_close_failure(produced,monkeypatch,boundary,signal_name):
    from contextlib import contextmanager
    from plumpy.process_states import KillInterruption
    from aiida_qcl_negf import saved_sources
    f=produced;hits=[];streams=[]
    signals=dict(KillInterruption=KillInterruption,KeyboardInterrupt=KeyboardInterrupt,SystemExit=SystemExit,enter=KillInterruption)
    primary=signals[signal_name]('repository primary') if signal_name else None;secondary=OSError('repository secondary close')
    kind=orm.SinglefileData if boundary=='singlefile' else type(f.process.node.outputs.retrieved.base.repository)
    real=kind.open
    @contextmanager
    def opening(owner,*a,**k):
        with real(owner,*a,**k) as stream:
            if (boundary=='singlefile' and owner.uuid!=f.process.inputs.plan.uuid) or (boundary!='singlefile' and a[0]!='series_result.json'):
                yield stream;return
            streams.append(stream)
            if signal_name=='enter':hits.append('enter');raise primary
            class Proxy:
                def __getattr__(self,name):return getattr(stream,name)
                def read(self,*args):
                    hits.append('read')
                    if primary is not None:raise primary
                    return stream.read(*args)
            try:yield Proxy()
            finally:hits.append('secondary_close');raise secondary
    monkeypatch.setattr(kind,'open',opening)
    expected=type(primary) if primary is not None else PauseInterruption if boundary=='packet' else OSError
    with pytest.raises(expected) as caught:
        if boundary=='singlefile':saved_sources._native_file(f.process.inputs.plan,saved_sources._NativeLedger(320<<20,256<<20),maximum=1<<20)
        elif boundary=='retrieved':saved_sources._native_result(f.process.node,saved_sources._NativeLedger(320<<20,256<<20))
        else:performance_binding.publish_native_performance(f.process)
    if primary is not None:assert caught.value is primary
    else:assert caught.value is secondary if boundary!='packet' else caught.value.__cause__ is secondary
    assert hits==['enter'] if signal_name=='enter' else hits[-1]=='secondary_close' and hits[:-1] and set(hits[:-1])=={'read'}
    assert all(stream.closed for stream in streams) and 'performance_source' not in f.process.node.outputs
    assert not f.p.transports or not f.p.transports[-1].is_open
    assert service._T4_NATIVE_RESULT_PROVIDER.get() is None


@pytest.mark.integration
def test_T4_repair_actual_B_and_T4_public_getters_keep_primary(native_graph,tmp_path,monkeypatch):
    from contextlib import contextmanager
    from plumpy.process_states import KillInterruption
    fixture=_cp_current(native_graph,tmp_path,monkeypatch);f=next(fixture)
    try:
        assert f.process.parse_retrieved_output().status==302
        for point in (f.state['identity']['point_id'],None):
            assert service.get_native_result_source(**f.provider,point_id=point)['status']=='available'
        assert service.get_performance_source(**f.provider)['status']=='available'
        for boundary in ('singlefile','retrieved'):
            kind=orm.SinglefileData if boundary=='singlefile' else type(f.process.node.outputs.retrieved.base.repository)
            real=kind.open;hits=[];primary=KillInterruption('public B/T4 primary '+boundary)
            @contextmanager
            def opening(owner,*a,**k):
                with real(owner,*a,**k) as stream:
                    target=owner.uuid==f.process.node.outputs.result.uuid if boundary=='singlefile' else a[0]=='series_result.json'
                    if not target:yield stream;return
                    class Proxy:
                        def __getattr__(self,name):return getattr(stream,name)
                        def read(self,*args):hits.append('read');raise primary
                    try:yield Proxy()
                    finally:hits.append('secondary_close');raise OSError('public B/T4 secondary close')
            with monkeypatch.context() as patch:
                patch.setattr(kind,'open',opening)
                for call in (lambda:service.get_native_result_source(**f.provider,point_id=None),lambda:service.get_performance_source(**f.provider)):
                    with pytest.raises(KillInterruption) as caught:call()
                    assert caught.value is primary and hits[-2:]==['read','secondary_close']
                    assert service._T4_NATIVE_RESULT_PROVIDER.get() is None
    finally:
        try:next(fixture)
        except StopIteration:pass
