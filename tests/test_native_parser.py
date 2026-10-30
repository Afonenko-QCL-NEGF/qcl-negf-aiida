"""B SOURCE/L: genuine isolated ORM links; no installed storage/creator claim.

Mapped c1 checkpoint sources exercise actual immutable ORM/Results joins.
Closed metadata API and prepaid whole-operation credit cover synthetic CP/final tuples.
Actual installed/native producer qualification remains an explicit OPEN gate.
"""
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import io
import json
import struct
from pathlib import Path

import pytest
from aiida import orm
from aiida.common.links import LinkType
from aiida.engine.utils import instantiate_process
from aiida.manage import get_manager
from aiida.schedulers.plugins.slurm import SlurmScheduler
from qcl_negf_contracts.messages import encode, ContractError
from qcl_negf_results import final_observations as native_metadata

from aiida_qcl_negf import saved_sources, native_backend, service
from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.parser import QCLExecutionParser
from test_native_actor import native_graph
from test_native_binding import SEED

M = 1024 * 1024


def raw_pin(node, raw):
    return dict(node_uuid=node.uuid, bytes=str(len(raw)), sha256=hashlib.sha256(raw).hexdigest())


def absent_raw():
    value = deepcopy(SEED['series'])
    value['points'][0]['data']['phase_source'] = dict(status='unavailable',session_id=None,current_path=None,reason='not_recorded')
    value['points'][0]['observables']['current_density_A_per_m2'] = -0.0
    value['attempt_history'] = deepcopy(value['points'])
    return b' \n' + encode(value) + b'\n'


def retrieved(calc, raw):
    folder = orm.FolderData()
    folder.base.repository.put_object_from_filelike(io.BytesIO(raw),'series_result.json')
    folder.base.links.add_incoming(calc,LinkType.CREATE,'retrieved')
    return folder.store()


@contextmanager
def access(raw, sources=(), *, cleanup=None):
    try:
        yield raw, sources, None
    except BaseException:
        if cleanup is not None:
            try: raise cleanup
            except BaseException: pass
        raise
    else:
        if cleanup is not None: raise cleanup


@pytest.fixture
def native_process(native_graph,monkeypatch):
    f=native_graph
    monkeypatch.setattr(orm.Computer,'get_scheduler',lambda self:SlurmScheduler())
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=f['restart'].pk,inputs={
        **{name:f['calc'].inputs[name] for name in ('code','plan','source_graph','native_execution','execution_id','attempt','release_id')},
        'metadata':{'call_link_label':'b_consumer','options':dict(resources={'num_machines':1,'num_mpiprocs_per_machine':1})}})
    raw=absent_raw();retrieved(process.node,raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw))
    try: yield process,raw,f
    finally: process.close()


@pytest.mark.integration
def test_absent_only_genuine_calcjob_pair_scalar_selection_and_offline_getter(native_process,monkeypatch):
    process,raw,f=native_process
    code=process.parse_retrieved_output()
    assert code.status==302
    process.update_outputs()
    outputs={row.link_label:row.node for row in process.node.base.links.get_outgoing(link_type=LinkType.CREATE).all()}
    assert set(outputs)=={'retrieved','result','native_binding','selection'}
    with outputs['result'].open(mode='rb') as stream:assert stream.read()==raw
    scalar=outputs['selection'].get_dict()
    assert scalar['commits']=={} and scalar['recovery_commit_sha256'] is None
    assert scalar['native_result']==raw_pin(outputs['result'],raw)
    assert scalar['performance_status']=='unavailable'
    assert scalar['performance_reason']=='native_technical_context_unavailable'
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline getter touched A')))
    view=service.get_native_result_source(f['run'].uuid,'execution',attempt=7,calcjob_uuid=process.node.uuid,point_id='point',binding_uuid=outputs['native_binding'].uuid)
    assert view['status']=='available' and view['result_utf8']==raw.decode()
    assert [r['point_id'] for r in view['binding']['points']]==['point']
    assert [r['point_id'] for r in view['binding']['attempt_history']]==['point']
    assert struct.pack('>d',json.loads(view['result_utf8'])['points'][0]['observables']['current_density_A_per_m2']).hex()=='8000000000000000'
    assert view['integrity']==dict(native_contexts='not_reverified',current_access='not_checked',scientific_acceptance='not_assessed')
    assert 'inventory' not in outputs
    with pytest.raises(ValueError):
        service.get_native_result_source(f['run'].uuid,'execution',attempt=7,calcjob_uuid=process.node.uuid,point_id='point',binding_uuid=f['plan'].uuid)


@pytest.mark.integration
def test_native_does_not_fall_through_legacy_inventory_or_publish_before_A_exit(native_process,monkeypatch):
    process,raw,f=native_process
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,cleanup=ValueError('exit source changed')))
    parser=QCLExecutionParser(process.node)
    assert parser.parse().status==301
    assert parser.outputs=={}


@pytest.mark.integration
def test_budget_prepaid_before_provider_and_no_output_constructors(native_process,monkeypatch):
    process,raw,f=native_process;calls=[]
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:calls.append(True))
    with pytest.raises(ValueError,match='budget'):
        saved_sources._prepare_native_outputs(process.node,raw,maximum_metadata_bytes=8*M-1,maximum_io_bytes=64*M)
    assert calls==[]
    assert set(process.outputs)==set()


@pytest.mark.integration
def test_repeat_parse_never_replaces_exact_existing_pair(native_process):
    process,raw,f=native_process
    assert process.parse_retrieved_output().status==302
    process.update_outputs()
    original={row.link_label:row.node.uuid for row in process.node.base.links.get_outgoing(link_type=LinkType.CREATE).all()}
    assert process.parse_retrieved_output().status==301
    assert {row.link_label:row.node.uuid for row in process.node.base.links.get_outgoing(link_type=LinkType.CREATE).all()}==original


@pytest.mark.integration
@pytest.mark.parametrize('field',['result','native_binding'])
def test_pair_readback_requires_exact_node_not_caller_pins(native_process,field):
    process,raw,f=native_process
    assert process.parse_retrieved_output().status==302
    process.update_outputs()
    result=process.node.outputs.result;binding=process.node.outputs.native_binding
    with binding.open(mode='rb') as stream:binding_raw=stream.read()
    pins={'result':raw_pin(result,raw),'native_binding':raw_pin(binding,binding_raw)}
    pins[field]['node_uuid']=f['plan'].uuid
    with pytest.raises(ValueError):
        saved_sources._readback_native_outputs(process.node,expected_result=pins['result'],expected_binding=pins['native_binding'],maximum_metadata_bytes=saved_sources._NATIVE_METADATA)


def test_one_nonrefundable_ledger_reserves_atomic_boundaries():
    ledger=saved_sources._NativeLedger(10,20)
    ledger.reserve('first',metadata=7,io=11)
    with pytest.raises(ValueError):ledger.reserve('too large',metadata=4,io=1)
    assert (ledger.metadata,ledger.io)==(3,9) and ledger.failed
    with pytest.raises(ValueError):ledger.reserve('remaining after failure',metadata=3,io=9)
    assert (ledger.metadata,ledger.io)==(3,9)


@pytest.fixture
def scientific_access(native_graph,tmp_path,monkeypatch,request):
    """Original CP/model/final bytes and ORM plan use the SAME input, no solve."""
    import sys
    monkeypatch.syspath_prepend(str(Path(__file__).parents[2]/'qcl-negf-results'/'tests'))
    from consumer_context_fixture import fixture as source_fixture
    root=tmp_path/'selected';root.mkdir()
    state,observations,observation,checkpoint,_=source_fixture(root,plan_kind='scientific_plan_v2')
    actual_inputs=getattr(request,'param',False)
    if actual_inputs:rebind_actual_source_inputs(root,state,observations)
    manifest=json.loads((root/state['manifest']['path']).read_bytes())
    refs={a['role']:{k:a[k] for k in ('path','bytes','sha256','schema','media_type')} for a in manifest['artifacts'] if a['role'] in ('plan','model')}
    def blob(raw):return orm.SinglefileData(file=io.BytesIO(raw),filename='input.json')
    original_plan=(root/refs['plan']['path']).read_bytes()
    plan=blob(original_plan).store();raw_graph=encode(manifest['provenance']);graph=blob(raw_graph).store()
    f=native_graph;native=blob(b'{}');declaration=deepcopy(f['execution'])
    declaration.update(execution_id=state['identity']['execution_id'],input_plan=raw_pin(plan,original_plan),source_graph=raw_pin(graph,raw_graph))
    declaration['storage']['execution_binding_uuid']=native.uuid
    native.set_file(io.BytesIO(encode(declaration)),filename='native.json');native.store()
    code=f['code'];release=f['calc'].inputs.release_id;attempt=orm.Int(7).store();execution=orm.Str(state['identity']['execution_id']).store()
    run=orm.WorkChainNode(process_type='aiida.workflows:qcl_negf.plan')
    for name,node in (('plan',plan),('source_graph',graph),('code',code),('release_id',release),('native_executions__execution_0',native)):
        run.base.links.add_incoming(node,LinkType.INPUT_WORK,name)
    run.store()
    child=orm.CalcJobNode(computer=code.computer,process_type='aiida.calculations:qcl_negf.execution')
    child.base.links.add_incoming(run,LinkType.CALL_CALC,'selected')
    for name,node in (('plan',plan),('source_graph',graph),('code',code),('release_id',release),('native_execution',native),('attempt',attempt),('execution_id',execution)):
        child.base.links.add_incoming(node,LinkType.INPUT_CALC,name)
    child.store()
    result=deepcopy(SEED['series']);frozen=json.loads(original_plan)
    result.update(plan_fingerprint=frozen['fingerprint'],plan_scientific_fingerprint=frozen['scientific_fingerprint'],selected_execution_id=state['identity']['execution_id'],
        root_definition_id=frozen['root_definition_id'],name=frozen['name'])
    result['executions']=[{k:frozen['executions'][0][k] for k in ('id','definition_id','variant_id','method_id','purpose','label','operation','point_ids','repetition')}]
    row=result['points'][0];row.update(id=state['identity']['point_id'],execution_id=state['identity']['execution_id'],attempt=int(state['identity']['attempt']))
    point=frozen['points'][0]
    row['coordinates'].update({key:point[key] for key in ('temperature_K','voltage_per_period_V','branch','order')})
    row['data'].pop('progress_events',None)
    row['data']['phase_source']=dict(status='unavailable',session_id=None,current_path=None,reason='not_recorded')
    source=dict(schema='qcl-negf.native-point-source.v1',kind='final',artifact_root='.',relation='current_point_attempt',selected_state=state,
        diagnostics=next({k:a[k] for k in ('path','bytes','sha256','schema','media_type')} for a in manifest['artifacts'] if a['path']=='analysis.h5'),
        observations=observations,reason=None)
    row['data']['native_source']=source
    # Independent literal identity and row order, then align BOTH raw inventories.
    assert {k:state['identity'][k] for k in ('run_id','execution_id','point_id','attempt')}==dict(run_id='run',execution_id='execution-1' if actual_inputs else 'exec',point_id='point-1' if actual_inputs else 'point',attempt='1')
    result['attempt_history']=deepcopy(result['points'])
    return dict(child=child,result=result,source=source,root=root,refs=refs,manifest=manifest)


@pytest.mark.integration
@pytest.mark.parametrize('kind',['checkpoint','final'])
@pytest.mark.parametrize('relation',['current_point_attempt','retained_source'])
def test_coherent_public_readers_cannot_turn_access_or_relation_into_creator(scientific_access,monkeypatch,kind,relation):
    f=scientific_access;result=f['result'];source=f['source'];row=result['points'][0]
    if kind=='checkpoint':source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    if relation=='retained_source':
        row['attempt']+=1;source.update(relation=relation,reason='last_committed_before_pause')
    result['attempt_history']=deepcopy(result['points']);raw=encode(result);retrieved(f['child'],raw)
    sources=((f['root'],source,f['refs']['plan'],f['refs']['model']),)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,sources))
    with pytest.raises(saved_sources.SourceError) as caught:
        saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    assert caught.value.code=='creator_proof_unavailable'
    assert [r['point_attempt'] for r in caught.value.detail]==[row['attempt']]
    assert caught.value.detail[0]['operand']=='original_invocation_calcjob_origin'
    assert set(f['child'].outputs)=={'retrieved'}


@pytest.mark.integration
def test_actual_public_source_rejects_full_manifest_ref_before_constructor(scientific_access,monkeypatch):
    f=scientific_access;source=f['source'];source['selected_state']['manifest']['sha256']='f'*64
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(f['child'],raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    with pytest.raises(ContractError) as caught:
        saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    assert caught.value.code=='corrupt_result'
    assert set(f['child'].outputs)=={'retrieved'}


@pytest.mark.integration
def test_primary_cancellation_survives_provider_cleanup_without_outputs(native_process,monkeypatch):
    process,raw,f=native_process;primary=KeyboardInterrupt('B primary cancel')
    @contextmanager
    def failed(*a,**k):
        with access(raw,cleanup=ValueError('cleanup secondary')):
            raise primary
            yield
    monkeypatch.setattr(native_backend,'open_native_selected_sources',failed)
    with pytest.raises(KeyboardInterrupt) as caught:
        saved_sources._prepare_native_outputs(process.node,raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    assert caught.value is primary and set(process.outputs)==set()


@pytest.mark.integration
def test_parse_and_output_readback_share_one_original_nonrefundable_credit(native_process):
    process,raw,f=native_process
    with saved_sources._native_operation(saved_sources._NATIVE_METADATA,256*M) as ledger:
        assert process.parse_retrieved_output().status==302
        assert len([r for r in ledger.reservations if r[0]=='A selected access full lifetime'])==1
        # Actual parser, output byte copies, genuine CREATE/readback and scalar
        # construction consumed this SAME ledger; no helper reset/refund.
        names=[r[0] for r in ledger.reservations]
        assert 'pure binder argument copy/workspace' in names and 'offline pure binder argument copy/workspace' in names
        assert 'scalar selection constructor/readback' in names
        assert sum(r[1] for r in ledger.reservations)==saved_sources._NATIVE_METADATA-ledger.metadata
        assert sum(r[2] for r in ledger.reservations)==256*M-ledger.io
        remaining=ledger.metadata
        with pytest.raises(saved_sources.SourceError):ledger.reserve('one byte beyond remaining',metadata=remaining+1)
        assert ledger.metadata==remaining


@pytest.mark.integration
def test_creator_hold_does_not_refund_credit_for_retry(scientific_access,monkeypatch):
    f=scientific_access;source=f['source'];raw=encode(f['result']);retrieved(f['child'],raw)
    calls=[]
    def provider(*args,**kwargs):
        calls.append(True)
        return access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),))
    monkeypatch.setattr(native_backend,'open_native_selected_sources',provider)
    with saved_sources._native_operation(saved_sources._NATIVE_METADATA,256*M) as ledger:
        with pytest.raises(saved_sources.SourceError) as first:
            saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
        assert first.value.code=='creator_proof_unavailable'
        after=ledger.metadata
        with pytest.raises(saved_sources.SourceError) as second:
            saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
        assert second.value.code=='budget_exceeded'
        assert ledger.metadata==after and ledger.failed and calls==[True]


@pytest.mark.integration
@pytest.mark.parametrize('primary',[False,True])
def test_real_reader_cleanup_preserves_primary_and_never_constructs_output(scientific_access,monkeypatch,primary):
    from qcl_negf_results import final_observations
    from qcl_negf_results.state import StateReader
    f=scientific_access;raw=encode(f['result']);retrieved(f['child'],raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],f['source'],f['refs']['plan'],f['refs']['model']),)))
    original=StateReader.close;cancel=KeyboardInterrupt('original reader consumer cancellation')
    def close(reader):
        original(reader)
        raise ValueError('reader close secondary')
    monkeypatch.setattr(StateReader,'close',close)
    if primary:
        monkeypatch.setattr(final_observations,'describe_checkpoint_inputs',lambda *a,**k:(_ for _ in ()).throw(cancel))
    with pytest.raises(KeyboardInterrupt if primary else ValueError) as caught:
        saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    if primary:assert caught.value is cancel
    else:assert str(caught.value)=='reader close secondary'
    assert set(f['child'].outputs)=={'retrieved'}


@pytest.mark.integration
@pytest.mark.parametrize('mutation',['plan_uuid','plan_newline','code_uuid','release','missing_call'])
def test_actual_input_graph_refuses_equivalent_or_foreign_operands_before_A(native_graph,monkeypatch,mutation):
    f=native_graph;original=f['calc'];inputs={k:original.inputs[k] for k in ('plan','source_graph','native_execution','code','release_id','execution_id','attempt')}
    if mutation in ('plan_uuid','plan_newline'):
        inputs['plan']=orm.SinglefileData(file=io.BytesIO(f['raw_plan']+(b'\n' if mutation=='plan_newline' else b'')),filename='copy.json').store()
    elif mutation=='code_uuid':
        inputs['code']=orm.InstalledCode(computer=f['code'].computer,filepath_executable=str(f['code'].filepath_executable),default_calc_job_plugin='qcl_negf.execution').store()
    elif mutation=='release':inputs['release_id']=orm.Str('foreign').store()
    child=orm.CalcJobNode(computer=f['code'].computer,process_type='aiida.calculations:qcl_negf.execution')
    if mutation!='missing_call':child.base.links.add_incoming(f['restart'],LinkType.CALL_CALC,'negative_input')
    for name,node in inputs.items():child.base.links.add_incoming(node,LinkType.INPUT_CALC,name)
    child.store();raw=absent_raw();retrieved(child,raw);calls=[]
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:calls.append(True))
    with pytest.raises(saved_sources.SourceError) as caught:
        saved_sources._prepare_native_outputs(child,raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    assert caught.value.code=='source_identity_mismatch' and calls==[]
    assert set(child.outputs)=={'retrieved'}


@pytest.mark.integration
def test_native_missing_result_preserves_existing_missing_exit(native_graph):
    child=native_graph['calc'];folder=orm.FolderData()
    folder.base.links.add_incoming(child,LinkType.CREATE,'retrieved');folder.store()
    parser=QCLExecutionParser(child)
    assert parser.parse().status==300 and parser.outputs=={}


@pytest.mark.integration
@pytest.mark.parametrize('cap',['metadata','io'])
def test_nested_consumer_cannot_ignore_explicit_tighter_caller_cap(native_process,monkeypatch,cap):
    process,raw,f=native_process;calls=[]
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:calls.append(True))
    with saved_sources._native_operation(saved_sources._NATIVE_METADATA,256*M) as ledger:
        with pytest.raises(saved_sources.SourceError) as caught:
            saved_sources._prepare_native_outputs(process.node,raw,maximum_metadata_bytes=8*M-1 if cap=='metadata' else saved_sources._NATIVE_METADATA,
                maximum_io_bytes=64*M-1 if cap=='io' else 256*M)
        assert caught.value.code=='budget_exceeded' and calls==[]
        assert ledger.metadata<8*M if cap=='metadata' else ledger.io<64*M


@pytest.mark.integration
@pytest.mark.parametrize('target',['diagnostics','observations'])
@pytest.mark.parametrize('primary_kind',['regular','cancellation','healthy_body'])
def test_final_reader_cleanup_preserves_primary_or_blocks_output(scientific_access,monkeypatch,target,primary_kind):
    """Catch loss of either primary object, duplicate close or post-cleanup output."""
    from qcl_negf_results.final_diagnostics import FinalDiagnosticsReader
    from qcl_negf_results.final_observations import FinalObservationsReader
    from qcl_negf_results.state import StateReader
    f=scientific_access;raw=encode(f['result']);retrieved(f['child'],raw)
    lifetime=[]
    @contextmanager
    def provider(*args,**kwargs):
        lifetime.append('enter')
        try:yield raw,((f['root'],f['source'],f['refs']['plan'],f['refs']['model']),),None
        finally:lifetime.append('exit')
    monkeypatch.setattr(native_backend,'open_native_selected_sources',provider)
    classes={'diagnostics':FinalDiagnosticsReader,'observations':FinalObservationsReader}
    closes={'diagnostics':0,'observations':0,'state':0};described=[];constructors=[]
    primary=(KeyboardInterrupt('original final reader cancellation') if primary_kind=='cancellation' else
             ValueError('original final reader error') if primary_kind=='regular' else None)
    secondary=ValueError('secondary final reader close failure')
    original_describe=classes[target].describe
    def describe(reader):
        described.append(target)
        if primary is not None:raise primary
        return original_describe(reader)
    monkeypatch.setattr(classes[target],'describe',describe)
    for name,klass in classes.items():
        original_close=klass.close
        def close(reader,*,name=name,original=original_close):
            closes[name]+=1;original(reader)
            if name==target:raise secondary
        monkeypatch.setattr(klass,'close',close)
    original_state_close=StateReader.close
    def close_state(reader):
        closes['state']+=1
        return original_state_close(reader)
    monkeypatch.setattr(StateReader,'close',close_state)
    original_constructor=orm.SinglefileData.__init__
    def construct(node,*args,**kwargs):
        constructors.append(True)
        return original_constructor(node,*args,**kwargs)
    monkeypatch.setattr(orm.SinglefileData,'__init__',construct)
    with saved_sources._native_operation(saved_sources._NATIVE_METADATA,256*M) as ledger:
        with pytest.raises(type(primary) if primary is not None else ValueError) as caught:
            saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
        assert caught.value is (primary if primary is not None else secondary)
        operations=[entry[0] for entry in ledger.reservations]
        assert operations.count('Results detached retained slot')==1
        assert operations.count('closed Results metadata context artifact allowance')==1
        assert ledger.failed
        assert sum(entry[1] for entry in ledger.reservations)==saved_sources._NATIVE_METADATA-ledger.metadata
        assert sum(entry[2] for entry in ledger.reservations)==256*M-ledger.io
    assert described==[target]
    assert closes=={'diagnostics':1,'observations':int(target=='observations'),'state':1}
    assert lifetime==['enter','exit'] and constructors==[]
    assert set(f['child'].outputs)=={'retrieved'}


def mapped_source(f, *, creator=None, native_uuid=None, script_transform=None, script_link="create"):
    """Manufacture c1 BEFORE reads, rebinding every original file in DAG order.

    IDs come from the actual stored fixture inputs, never the B proof builder.
    This is a wire/ORM oracle; it does not qualify a native writer.
    """
    import h5py
    from final_diagnostics_fixture import fixed_json
    from result_v2_fixture import reference, put
    creator=f['child'] if creator is None else creator
    if script_link=='input':
        previous=creator;creator=orm.CalcJobNode(computer=previous.computer,process_type=previous.process_type)
        creator.base.links.add_incoming(previous.caller,LinkType.CALL_CALC,'input_script_negative')
        for name in ('plan','source_graph','native_execution','code','release_id','execution_id','attempt'):
            creator.base.links.add_incoming(previous.inputs[name],LinkType.INPUT_CALC,name)
        f['child']=creator
    epoch='c1-'+creator.uuid.replace('-','')+'-'+(native_uuid or creator.inputs.native_execution.uuid).replace('-','')+'-17'
    root=f['root'];manifest=f['manifest'];replacements={}
    def rewrite(v):
        if isinstance(v,dict):
            if set(('path','bytes','sha256','schema','media_type'))<=set(v) and v['path'] in replacements:
                v.update(replacements[v['path']])
            if 'source_epoch' in v:v['source_epoch']=epoch
            for child in v.values():rewrite(child)
        elif isinstance(v,list):
            for child in v:rewrite(child)
    for filename,declaration in [('full.h5',None),('checkpoint.json',None),('analysis.h5','final_diagnostics/declaration_json'),('observations.h5','final_observations/declaration_json')]:
        old=next(a for a in manifest['artifacts'] if a['path']==filename)
        if filename.endswith('.json'):
            value=json.loads((root/filename).read_bytes());rewrite(value)
            new=put(root,filename,value,old['schema'])
        else:
            with h5py.File(root/filename,'r+') as handle:
                del handle['metadata/invocation_json'];fixed_json(handle,'metadata/invocation_json',dict(source_epoch=epoch))
                if declaration:
                    value=json.loads(bytes(handle[declaration][()]));rewrite(value)
                    del handle[declaration];fixed_json(handle,declaration,value)
            new=reference(root,filename,old['schema'],old['media_type'])
        replacements[filename]=new
    rewrite(manifest)
    f['source']['selected_state']['manifest']=put(root,'selected.json',manifest,manifest['schema'])
    for key in ('diagnostics','observations'):
        f['source'][key].update(replacements[f['source'][key]['path']])
    f['result']['attempt_history']=deepcopy(f['result']['points'])
    solver=str(creator.inputs.code.filepath_executable)
    output='/srv/qcl-negf/jobs/'+json.loads(creator.inputs.native_execution.get_content())['storage']['relative_execution_root']
    words=[solver,'run-plan','plan.json',output,'--execution-id',creator.inputs.execution_id.value,'--source-graph','source-graph.json',
        '--native-execution','native-execution.json','--native-calcjob-uuid',creator.uuid]
    literal=lambda s:"'"+s.replace("'", "'\"'\"'")+"'"
    script=("#!/bin/bash\n/run/current-system/sw/bin/qcl-negf-release guard --release-id "+creator.inputs.release_id.value+
        ' --solver-executable '+solver+' || exit 78\nunset QCL_NEGF_CAPTURE_SPEC\n'+
        ' '.join(map(literal,words))+"  > 'solver.stdout' 2> 'solver.stderr'\n").encode()
    if script_transform is not None:script=script_transform(script)
    node=orm.SinglefileData(file=io.BytesIO(script),filename='_aiidasubmit.sh')
    if script_link=='create':node.base.links.add_incoming(creator,LinkType.CREATE,'native_submit_script')
    elif script_link=='input':creator.base.links.add_incoming(node.store(),LinkType.INPUT_CALC,'native_submit_script')
    elif script_link=='foreign':node.base.links.add_incoming(orm.CalcFunctionNode(process_type='foreign.creator').store(),LinkType.CREATE,'native_submit_script')
    elif script_link=='return':
        node.store();node.base.links.add_incoming(creator.caller,LinkType.RETURN,'native_submit_script')
    if not node.is_stored:node.store()
    if not creator.is_stored:creator.store()
    f.update(epoch=epoch,script=node,script_raw=script)
    return f


@pytest.mark.integration
@pytest.mark.parametrize('kind',['checkpoint','final'])
@pytest.mark.parametrize('relation',['current_point_attempt','retained_source'])
def test_actual_c1_current_and_samechild_retained_have_complete_rows_and_offline_projection(scientific_access,monkeypatch,kind,relation):
    f=mapped_source(scientific_access);source=f['source'];row=f['result']['points'][0]
    if kind=='checkpoint':source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    if relation=='retained_source':
        row['attempt']+=1;source.update(relation=relation,reason='last_committed_before_pause')
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=b' \n'+encode(f['result'])+b'\n';retrieved(f['child'],raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    result,binding=saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    for node,label in ((result,'result'),(binding,'native_binding')):
        node.base.links.add_incoming(f['child'],LinkType.CREATE,label);node.store()
    with binding.open(mode='rb') as stream:binding_raw=stream.read()
    scalar=saved_sources._readback_native_outputs(f['child'],expected_result=raw_pin(result,raw),expected_binding=raw_pin(binding,binding_raw),maximum_metadata_bytes=saved_sources._NATIVE_METADATA)
    pin=orm.Dict(dict=scalar);pin.base.links.add_incoming(f['child'],LinkType.CREATE,'selection');pin.store()
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline touched A')))
    monkeypatch.setattr(native_metadata,'describe_native_metadata_context',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline touched native root')))
    view=saved_sources._retained_native_result_source(f['child'],point_id='point',maximum_metadata_bytes=saved_sources._NATIVE_METADATA)
    assert view['result_utf8']==raw.decode() and view['status']=='available'
    for name in ('points','attempt_history'):
        assert len(view['binding'][name])==1
        projected=view['binding'][name][0]
        assert projected['source']==source and projected['point_attempt']==row['attempt']
        assert projected['source_invocation']==dict(source_epoch=f['epoch'])
        assert projected['creator']==dict(calcjob_uuid=f['child'].uuid,binding_uuid=None)
    assert view['integrity']==dict(native_contexts='not_reverified',current_access='not_checked',scientific_acceptance='not_assessed')


def store_pair(child,raw,*,maximum=saved_sources._NATIVE_METADATA):
    result,binding=saved_sources._prepare_native_outputs(child,raw,maximum_metadata_bytes=maximum,maximum_io_bytes=256*M)
    for node,label in ((result,'result'),(binding,'native_binding')):
        node.base.links.add_incoming(child,LinkType.CREATE,label);node.store()
    with binding.open(mode='rb') as stream:binding_raw=stream.read()
    scalar=saved_sources._readback_native_outputs(child,expected_result=raw_pin(result,raw),expected_binding=raw_pin(binding,binding_raw),maximum_metadata_bytes=maximum)
    pin=orm.Dict(dict=scalar);pin.base.links.add_incoming(child,LinkType.CREATE,'selection');pin.store()
    return result,binding,pin


def next_child(origin,*,parent_binding=None):
    child=orm.CalcJobNode(computer=origin.computer,process_type='aiida.calculations:qcl_negf.execution')
    child.base.links.add_incoming(origin.caller,LinkType.CALL_CALC,'next_child')
    for name in ('plan','source_graph','native_execution','code','release_id','execution_id','attempt'):
        child.base.links.add_incoming(origin.inputs[name],LinkType.INPUT_CALC,name)
    if parent_binding is not None:child.base.links.add_incoming(parent_binding,LinkType.INPUT_CALC,'parent_binding')
    return child.store()


@pytest.mark.integration
@pytest.mark.parametrize('chain',[True,False])
def test_foreign_c1_requires_actual_parent_binding_chain_even_current_relation(scientific_access,monkeypatch,chain):
    f=mapped_source(scientific_access);source=f['source'];source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(f['child'],raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    _,original_binding,_=store_pair(f['child'],raw)
    child=next_child(f['child'],parent_binding=original_binding if chain else None);retrieved(child,raw)
    if not chain:
        with pytest.raises(saved_sources.SourceError) as caught:
            saved_sources._prepare_native_outputs(child,raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
        assert caught.value.code=='creator_proof_unavailable'
        assert caught.value.detail[0]['operand']=='actual_parent_binding_chain'
        assert set(child.outputs)=={'retrieved'}
        return
    with saved_sources._native_operation(saved_sources._NATIVE_METADATA,saved_sources._NATIVE_IO) as ledger:
        _,binding,pin=store_pair(child,raw)
        value=json.loads(binding.get_content())
        assert value['points'][0]['creator']==dict(calcjob_uuid=f['child'].uuid,binding_uuid=original_binding.uuid)
        assert pin.get_dict()['native_binding']['node_uuid']==binding.uuid
        assert [r[0] for r in ledger.reservations].count('Results detached retained slot')==1
        print('PARENT_COMPLETE_LEDGER',json.dumps(dict(remaining_metadata=ledger.metadata,remaining_io=ledger.io,reservations=ledger.reservations)))
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline A')))
    view=saved_sources._retained_native_result_source(child,point_id='point',maximum_metadata_bytes=saved_sources._NATIVE_METADATA)
    assert view['point']['creator']==dict(calcjob_uuid=f['child'].uuid,binding_uuid=original_binding.uuid)


@pytest.mark.integration
def test_former_32mib_capacity_refuses_before_foreign_provider(scientific_access,monkeypatch):
    f=mapped_source(scientific_access);raw=encode(f['result']);retrieved(f['child'],raw);calls=[]
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:calls.append(True))
    with saved_sources._native_operation(32*M,256*M) as ledger:
        with pytest.raises(saved_sources.SourceError) as caught:
            saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=32*M,maximum_io_bytes=256*M)
        assert caught.value.detail=='Results reusable whole-call workspace' and ledger.failed
    assert not calls and set(f['child'].outputs)=={'native_submit_script','retrieved'}


@pytest.mark.integration
@pytest.mark.parametrize('fault',['input_uuid','script_uuid','script_alias','script_shell','missing_script','input_script','foreign_CREATE','RETURN_script','raw_native','raw_script'])
def test_c1_actual_original_uuid_script_create_and_raw_negative_controls(scientific_access,monkeypatch,fault):
    from aiida_qcl_negf import native_actor
    change=None
    if fault=='script_uuid':change=lambda raw:raw.replace(scientific_access['child'].uuid.encode(),b'00000000-0000-4000-8000-000000000001')
    if fault=='script_alias':change=lambda raw:raw.replace(b"'native-execution.json'",b"'./native-execution.json'")
    if fault=='script_shell':change=lambda raw:raw.rstrip()+b'; true\n'
    kwargs=dict(script_transform=change,script_link={'missing_script':'none','input_script':'input','foreign_CREATE':'foreign','RETURN_script':'return'}.get(fault,'create'))
    if fault=='input_uuid':kwargs['native_uuid']=scientific_access['child'].inputs.plan.uuid
    f=mapped_source(scientific_access,**kwargs);source=f['source'];source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(f['child'],raw)
    if fault in ('raw_native','raw_script'):
        actual=native_actor.read_native_origin_inputs
        def swapped(*a,**k):
            native,script=actual(*a,**k)
            return (native+b'\n',script) if fault=='raw_native' else (native,script+b'\n')
        monkeypatch.setattr(native_actor,'read_native_origin_inputs',swapped)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    constructors=[];original=orm.SinglefileData.__init__
    def constructor(node,*a,**k):constructors.append(True);return original(node,*a,**k)
    monkeypatch.setattr(orm.SinglefileData,'__init__',constructor)
    with pytest.raises(saved_sources.SourceError) as caught:
        saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    assert caught.value.code in ('source_identity_mismatch','creator_proof_unavailable')
    assert constructors==[] and 'result' not in f['child'].outputs and 'native_binding' not in f['child'].outputs


@pytest.mark.integration
def test_restart_process_type_string_on_workfunction_refuses_before_A(native_graph,monkeypatch):
    f=native_graph;restart=orm.WorkFunctionNode(process_type='aiida.workflows:qcl_negf.execution_restart')
    restart.base.links.add_incoming(f['run'],LinkType.CALL_WORK,'wrong_restart_class')
    for name in ('plan','source_graph','native_execution','code','release_id','execution_id','attempt'):
        restart.base.links.add_incoming(f['calc'].inputs[name],LinkType.INPUT_WORK,name)
    restart.store()
    child=orm.CalcJobNode(computer=f['code'].computer,process_type='aiida.calculations:qcl_negf.execution')
    child.base.links.add_incoming(restart,LinkType.CALL_CALC,'wrong_restart_child')
    for name in ('plan','source_graph','native_execution','code','release_id','execution_id','attempt'):
        child.base.links.add_incoming(f['calc'].inputs[name],LinkType.INPUT_CALC,name)
    child.store();raw=absent_raw();retrieved(child,raw);calls=[]
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:calls.append(True))
    with pytest.raises(saved_sources.SourceError) as caught:
        saved_sources._prepare_native_outputs(child,raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    assert caught.value.detail=='native_restart_call_required' and calls==[]


@pytest.mark.integration
def test_c1_distinct_history_row_relation_is_covered_without_counter_creator_guess(scientific_access,monkeypatch):
    f=mapped_source(scientific_access);source=f['source'];source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    history=deepcopy(f['result']['points'][0]);f['result']['points'][0]['attempt']=2
    source.update(relation='retained_source',reason='last_committed_before_pause')
    f['result']['attempt_history']=[history];raw=encode(f['result']);retrieved(f['child'],raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    args=saved_sources._load_native_binding_inputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    assert [c['row_key']['point_attempt'] for c in args['source_contexts']]==[2,1]
    assert [c['row_key']['point_attempt'] for c in args['creator_proofs']]==[2,1]
    assert [c['creator_calcjob_uuid'] for c in args['creator_proofs']]==[f['child'].uuid,f['child'].uuid]


@pytest.mark.integration
def test_c1_checkpoint_actual_parser_aggregate_credit_feasibility(scientific_access,monkeypatch):
    f=scientific_access;previous=f['child']
    monkeypatch.setattr(orm.Computer,'get_scheduler',lambda self:SlurmScheduler())
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=previous.caller.pk,inputs={
        **{name:previous.inputs[name] for name in ('code','plan','source_graph','native_execution','execution_id','attempt','release_id')},
        'metadata':{'call_link_label':'b_origin_actual_parser','options':dict(resources={'num_machines':1,'num_mpiprocs_per_machine':1})}})
    f['child']=process.node;f=mapped_source(f);source=f['source'];source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(process.node,raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    try:
        with saved_sources._native_operation(saved_sources._NATIVE_METADATA,256*M) as ledger:
            code=process.parse_retrieved_output();process.update_outputs()
            outputs={r.link_label:r.node for r in process.node.base.links.get_outgoing(link_type=LinkType.CREATE).all()}
            print('CHECKPOINT_PARSER_LEDGER',json.dumps(dict(exit=code.status,outputs=list(outputs),remaining_metadata=ledger.metadata,remaining_io=ledger.io,reservations=ledger.reservations)))
            # Public orchestration keeps the original scientific exit and its
            # genuine pair when readback credit fails; no selection is exposed.
            assert code.status==302
            assert set(outputs)=={'native_submit_script','retrieved','result','native_binding','selection'}
            assert ledger.metadata>0
            assert [r[0] for r in ledger.reservations].count('origin raw port full allowance')==2
            assert any(r[0]=='scalar selection constructor/readback' for r in ledger.reservations)
    finally:process.close()


@pytest.mark.integration
def test_origin_actor_exact_restart_query_inventory_fits_prepaid_finite_allowance(native_graph,monkeypatch):
    from aiida_qcl_negf import native_actor
    f=native_graph;original=native_actor._Reads;reads=[]
    class MeasuredReads(original):
        def __init__(self,*a,**k):super().__init__(*a,**k);reads.append(self)
    monkeypatch.setattr(native_actor,'_Reads',MeasuredReads)
    epoch='c1-'+f['calc'].uuid.replace('-','')+'-'+f['native'].uuid.replace('-','')+'-0'
    assert native_actor.read_native_origin_inputs(f['calc'].uuid,source_epoch=epoch,
        maximum_metadata_bytes=saved_sources._NATIVE_ORIGIN_METADATA)==(f['raw_native'],f['script'].get_content().encode())
    measured=reads[0]
    assert measured.calls==30 and len(measured.cache)==4 and measured.remaining>=0
    assert 30*16384+4*8*65537<=saved_sources._NATIVE_ORIGIN_METADATA==2621440
    assert saved_sources._NATIVE_ORIGIN_IO==327680
    print('ORIGIN_PORT_INVENTORY',json.dumps(dict(queries=measured.calls,files=len(measured.cache),remaining=measured.remaining,
        allowance=saved_sources._NATIVE_ORIGIN_METADATA,worst_case=30*16384+4*8*65537)))


@pytest.mark.integration
def test_c1_origin_port_cancellation_keeps_primary_and_A_cleanup_without_outputs(scientific_access,monkeypatch):
    from aiida_qcl_negf import native_actor
    f=mapped_source(scientific_access);source=f['source'];source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(f['child'],raw)
    primary=KeyboardInterrupt('original origin port cancellation');lifetime=[]
    def origin(*a,**k):raise primary
    @contextmanager
    def provider(*a,**k):
        lifetime.append('enter')
        try:
            with access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),),cleanup=ValueError('secondary A close')) as packet:yield packet
        finally:lifetime.append('exit')
    monkeypatch.setattr(native_actor,'read_native_origin_inputs',origin)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',provider)
    with saved_sources._native_operation(saved_sources._NATIVE_METADATA,256*M) as ledger:
        with pytest.raises(KeyboardInterrupt) as caught:
            saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
        assert caught.value is primary
        assert [r[0] for r in ledger.reservations].count('origin raw port full allowance')==1
    assert lifetime==['enter','exit'] and set(f['child'].outputs)=={'native_submit_script','retrieved'}


@pytest.mark.integration
@pytest.mark.parametrize('fault',['invocation_input','creator'])
def test_offline_genuine_create_projection_rejects_wrong_c1_or_creator_without_A(scientific_access,monkeypatch,fault):
    f=mapped_source(scientific_access);source=f['source'];source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(f['child'],raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    result,binding=saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    value=json.loads(binding.get_content())
    for name in ('points','attempt_history'):
        if fault=='invocation_input':value[name][0]['source_invocation']['source_epoch']='c1-'+f['child'].uuid.replace('-','')+'-'+f['child'].inputs.plan.uuid.replace('-','')+'-17'
        else:value[name][0]['creator']=dict(calcjob_uuid=f['child'].caller.uuid,binding_uuid=f['child'].inputs.plan.uuid)
    binding=orm.SinglefileData(file=io.BytesIO(encode(value)),filename='native-binding.json')
    for node,label in ((result,'result'),(binding,'native_binding')):
        node.base.links.add_incoming(f['child'],LinkType.CREATE,label);node.store()
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline A')))
    monkeypatch.setattr(native_metadata,'describe_native_metadata_context',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline native root')))
    with pytest.raises(saved_sources.SourceError) as caught:
        saved_sources._readback_native_outputs(f['child'],expected_result=raw_pin(result,raw),expected_binding=raw_pin(binding,encode(value)),maximum_metadata_bytes=saved_sources._NATIVE_METADATA)
    assert caught.value.code=='source_identity_mismatch'
    assert 'selection' not in f['child'].outputs


@pytest.mark.integration
@pytest.mark.parametrize('scientific_access',[False,True],indirect=True,ids=['literal-inputs','actual-inputs'])
def test_two_actual_manifest_state_refs_do_not_lose_history_to_fit_budget(scientific_access,monkeypatch):
    f=mapped_source(scientific_access);source=f['source'];source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    history=deepcopy(f['result']['points'][0]);other=history['data']['native_source']
    # A second immutable manifest selector is actually present, with its exact
    # original bytes/hash. The source is manufactured, not a new solver state.
    (f['root']/'selected-second.json').write_bytes((f['root']/source['selected_state']['manifest']['path']).read_bytes())
    other['selected_state']['manifest']['path']='selected-second.json'
    f['result']['points'][0]['attempt']=2;source.update(relation='retained_source',reason='last_committed_before_pause')
    f['result']['attempt_history']=[history];raw=encode(f['result']);retrieved(f['child'],raw)
    packet=((f['root'],source,f['refs']['plan'],f['refs']['model']),(f['root'],other,f['refs']['plan'],f['refs']['model']))
    calls=[]
    def provider(*a,**k):calls.append(True);return access(raw,packet)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',provider)
    with saved_sources._native_operation(saved_sources._NATIVE_METADATA,256*M) as ledger:
        _,binding,_=store_pair(f['child'],raw)
        assert [r[0] for r in ledger.reservations].count('Results detached retained slot')==2
        assert [r[0] for r in ledger.reservations].count('Results reusable whole-call workspace')==1
        value=json.loads(binding.get_content())
        assert value['points'][0]['point_attempt']==2 and value['attempt_history'][0]['point_attempt']==1
        assert value['attempt_history'][0]['source']['selected_state']['manifest']['path']=='selected-second.json'
        print('TWO_MANIFESTS_COMPLETE_LEDGER',json.dumps(dict(remaining_metadata=ledger.metadata,remaining_io=ledger.io,reservations=ledger.reservations)))
    assert calls==[True]


@pytest.mark.integration
def test_original_completed_restore_requires_public_immediate_parent_context(scientific_access,monkeypatch,tmp_path):
    import consumer_context_fixture as fixtures
    f=scientific_access;original_fixture=fixtures.fixture;root=tmp_path/'completed';root.mkdir()
    monkeypatch.setattr(fixtures,'fixture',lambda root:original_fixture(root,plan_kind='scientific_plan_v2'))
    state,observations,value,_,_,_=fixtures.completed_fixture(root)
    source=dict(schema='qcl-negf.native-point-source.v1',kind='final',artifact_root='.',relation='current_point_attempt',selected_state=state,
        diagnostics=value['diagnostics'],observations=observations,reason=None)
    row=f['result']['points'][0];row['attempt']=2;row['data']['native_source']=source
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(f['child'],raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((root,source,f['refs']['plan'],f['refs']['model']),)))
    with pytest.raises(saved_sources.SourceError) as caught:
        saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=256*M)
    assert caught.value.code=='source_unavailable' and caught.value.detail=='qualified_immediate_parent_context'
    assert set(f['child'].outputs)=={'retrieved'}


def b_metadata_graph(f,raw_plan,*,raw_graph=None,graph_alias=False,native_padding=0,claim_mutation=None):
    """Fresh real ORM metadata operands; no StateRef/CP context or authority."""
    def blob(raw):return orm.SinglefileData(file=io.BytesIO(raw),filename='input.json')
    plan=blob(raw_plan).store();raw_graph=f['raw_graph'] if raw_graph is None else raw_graph
    graph=plan if graph_alias else blob(raw_graph).store()
    if graph_alias:raw_graph=raw_plan
    native=blob(b'{}');declaration=deepcopy(f['execution'])
    declaration.update(execution_id=json.loads(raw_plan)['executions'][0]['id'],input_plan=raw_pin(plan,raw_plan),source_graph=raw_pin(graph,raw_graph))
    declaration['storage']['execution_binding_uuid']=native.uuid
    if claim_mutation:claim_mutation(declaration)
    raw_native=encode(declaration)
    if native_padding:raw_native+=b' '*(native_padding-len(raw_native))
    native.set_file(io.BytesIO(raw_native),filename='native-execution.json');native.store()
    code=f['code'];release=orm.Str(declaration['code']['release_id']).store()
    run=orm.WorkChainNode(process_type='aiida.workflows:qcl_negf.plan')
    for name,node in (('plan',plan),('source_graph',graph),('code',code),('release_id',release),('native_executions__execution_0',native)):
        run.base.links.add_incoming(node,LinkType.INPUT_WORK,name)
    run.store();restart=orm.WorkChainNode(process_type='aiida.workflows:qcl_negf.execution_restart')
    restart.base.links.add_incoming(run,LinkType.CALL_WORK,'metadata')
    inputs=dict(plan=plan,source_graph=graph,code=code,release_id=release,native_execution=native,
        execution_id=orm.Str(declaration['execution_id']).store(),attempt=orm.Int(7).store())
    for name,node in inputs.items():restart.base.links.add_incoming(node,LinkType.INPUT_WORK,name)
    restart.store();child=orm.CalcJobNode(computer=code.computer,process_type='aiida.calculations:qcl_negf.execution')
    child.base.links.add_incoming(restart,LinkType.CALL_CALC,'metadata')
    for name,node in inputs.items():child.base.links.add_incoming(node,LinkType.INPUT_CALC,name)
    child.store()
    solver=str(code.filepath_executable);output='/srv/qcl-negf/jobs/'+declaration['storage']['relative_execution_root']
    argv=[solver,'run-plan','plan.json',output,'--execution-id',declaration['execution_id'],'--source-graph','source-graph.json',
        '--native-execution','native-execution.json','--native-calcjob-uuid',child.uuid]
    q=lambda word:"'"+word.replace("'","'\"'\"'")+"'"
    script_raw=("#!/bin/bash\n/run/current-system/sw/bin/qcl-negf-release guard --release-id "+release.value+
        ' --solver-executable '+solver+" || exit 78\nunset QCL_NEGF_CAPTURE_SPEC\n"+' '.join(map(q,argv))+"  > 'solver.stdout' 2> 'solver.stderr'\n").encode()
    script=blob(script_raw);script.base.links.add_incoming(child,LinkType.CREATE,'native_submit_script');script.store()
    return dict(child=child,run=run,restart=restart,plan=plan,graph=graph,native=native,raw_plan=raw_plan,raw_graph=raw_graph,
        raw_native=raw_native,script=script,script_raw=script_raw,declaration=declaration)


@pytest.mark.integration
@pytest.mark.parametrize('size',[1048575,1048576,1048577])
def test_b_plan_role_1mib_boundary_independent_of_outer_32mib(native_graph,size):
    raw=native_graph['raw_plan']+b' '*(size-len(native_graph['raw_plan']))
    f=b_metadata_graph(native_graph,raw);ledger=saved_sources._NativeLedger(saved_sources._NATIVE_METADATA,256*M)
    if size>1048576:
        with pytest.raises(saved_sources.SourceError) as caught:saved_sources._native_inputs(f['child'],ledger)
        assert caught.value.detail=='native_file_bound'
    else:
        args=saved_sources._native_inputs(f['child'],ledger,snapshot={})
        assert args['frozen_plan']==raw and 0<ledger.metadata<saved_sources._NATIVE_METADATA
        assert json.loads(args['frozen_plan'])==json.loads(native_graph['raw_plan'])


@pytest.mark.integration
@pytest.mark.parametrize('role',['source_graph','native_execution','submit_script'])
@pytest.mark.parametrize('size',[65535,65536,65537])
def test_b_other_original_roles_keep_64kib(native_graph,role,size):
    raw=b'{}'+b' '*(size-2)
    node=orm.SinglefileData(file=io.BytesIO(raw),filename='original.json').store()
    ledger=saved_sources._NativeLedger(saved_sources._NATIVE_METADATA,256*M)
    if size>65536:
        with pytest.raises(saved_sources.SourceError) as caught:saved_sources._native_file(node,ledger)
        assert caught.value.detail=='native_file_bound'
    else:assert saved_sources._native_file(node,ledger)==raw


@pytest.mark.integration
def test_b_same_original_plan_alias_cannot_bypass_strict_graph_role(native_graph):
    raw=native_graph['raw_plan']+b' '*(113111-len(native_graph['raw_plan']))
    f=b_metadata_graph(native_graph,raw,graph_alias=True);ledger=saved_sources._NativeLedger(saved_sources._NATIVE_METADATA,256*M)
    assert saved_sources._native_file(f['plan'],ledger,maximum=1048576)==raw
    with pytest.raises(saved_sources.SourceError) as caught:saved_sources._native_inputs(f['child'],ledger)
    assert caught.value.detail=='native_file_bound'


@pytest.mark.integration
@pytest.mark.parametrize('value',[True,-1,'-1','01',None])
def test_b_large_original_plan_requires_whole_exact_claim(native_graph,value):
    raw=native_graph['raw_plan']+b' '*(113111-len(native_graph['raw_plan']))
    f=b_metadata_graph(native_graph,raw,claim_mutation=lambda d:d['input_plan'].update(bytes=value))
    with pytest.raises(saved_sources.SourceError) as caught:
        saved_sources._native_inputs(f['child'],saved_sources._NativeLedger(saved_sources._NATIVE_METADATA,256*M))
    assert caught.value.code=='source_identity_mismatch' and caught.value.detail=='native_original_inputs'


@pytest.mark.integration
@pytest.mark.parametrize('fault',['budget','failure','cancel'])
def test_b_original_file_prepay_no_read_before_credit_and_no_refund(native_graph,monkeypatch,fault):
    raw=native_graph['raw_plan']+b' '*(113111-len(native_graph['raw_plan']));node=orm.SinglefileData(file=io.BytesIO(raw),filename='plan.json').store()
    real_open=orm.SinglefileData.open;reads=[];closed=[]
    class Cancelled(BaseException):pass
    primary=OSError('original metadata read failure') if fault=='failure' else Cancelled()
    @contextmanager
    def opening(self,*a,**kw):
        with real_open(self,*a,**kw) as stream:
            class View:
                def seek(self,*args):return stream.seek(*args)
                def tell(self):return stream.tell()
                def read(self,size):reads.append(size);raise primary
            try:yield View()
            finally:closed.append(True)
    monkeypatch.setattr(orm.SinglefileData,'open',opening)
    ledger=saved_sources._NativeLedger(8192 if fault=='budget' else saved_sources._NATIVE_METADATA,256*M)
    if fault=='budget':
        with pytest.raises(saved_sources.SourceError):saved_sources._native_file(node,ledger,maximum=1048576)
        assert reads==[] and ledger.metadata==0 and ledger.io==256*M
    else:
        with pytest.raises(OSError if fault=='failure' else Cancelled) as caught:
            saved_sources._native_file(node,ledger,maximum=1048576)
        assert caught.value is primary and reads==[113112]
        assert ledger.metadata==saved_sources._NATIVE_METADATA-8192-904896 and ledger.io==256*M-113112
    assert closed==[True]


@pytest.mark.integration
@pytest.mark.parametrize('projection',['offline','foreign_parent'])
def test_b_large_plan_offline_and_foreign_decode_reach_downstream_only(native_graph,monkeypatch,projection):
    """Real ORM/raw joins up to plan decode; sentinel grants no stored authority."""
    from aiida_qcl_negf import science_association
    raw=native_graph['raw_plan']+b' '*(113111-len(native_graph['raw_plan']));f=b_metadata_graph(native_graph,raw)
    parent=f['child'];result_raw=absent_raw();retrieved(parent,result_raw)
    result=orm.SinglefileData(file=io.BytesIO(result_raw),filename='series_result.json');result.base.links.add_incoming(parent,LinkType.CREATE,'result');result.store()
    binding=orm.SinglefileData(file=io.BytesIO(b'{}'),filename='native-binding.json');binding.base.links.add_incoming(parent,LinkType.CREATE,'native_binding');binding.store()
    class Downstream(BaseException):pass
    def stop(value,raw_result,selection,plan,execution):
        assert plan==json.loads(raw) and raw_result==result_raw
        raise Downstream()
    monkeypatch.setattr(science_association,'_binding_stored',stop)
    ledger=saved_sources._NativeLedger(saved_sources._NATIVE_METADATA,256*M)
    with pytest.raises(Downstream):
        if projection=='offline':
            saved_sources._native_readback(parent,raw_pin(result,result_raw),raw_pin(binding,b'{}'),ledger)
        else:
            child=orm.CalcJobNode(computer=parent.computer,process_type=parent.process_type)
            child.base.links.add_incoming(f['restart'],LinkType.CALL_CALC,'retained_metadata')
            for link in parent.base.links.get_incoming(link_type=LinkType.INPUT_CALC).all():child.base.links.add_incoming(link.node,LinkType.INPUT_CALC,link.link_label)
            child.base.links.add_incoming(binding,LinkType.INPUT_CALC,'parent_binding');child.store()
            snapshot={};args=saved_sources._native_inputs(child,ledger,snapshot=snapshot)
            saved_sources._native_parent_proof(dict(execution_id=args['selection']['execution_id'],id='point',attempt=1),{},
                {},child,args,snapshot,parent,ledger,{})


def b_origin_route(seed, size, restart):
    """Stored direct/Restart originals, with a valid unchanged scientific plan."""
    raw=seed['raw_plan']+b' '*(size-len(seed['raw_plan']))
    f=b_metadata_graph(seed,raw)
    if not restart:
        previous=f['child'];child=orm.CalcJobNode(computer=previous.computer,process_type=previous.process_type)
        child.base.links.add_incoming(f['run'],LinkType.CALL_CALC,'direct_origin')
        for edge in previous.base.links.get_incoming(link_type=LinkType.INPUT_CALC).all():
            child.base.links.add_incoming(edge.node,LinkType.INPUT_CALC,edge.link_label)
        child.store();script_raw=f['script_raw'].replace(previous.uuid.encode(),child.uuid.encode())
        script=orm.SinglefileData(file=io.BytesIO(script_raw),filename='_aiidasubmit.sh')
        script.base.links.add_incoming(child,LinkType.CREATE,'native_submit_script');script.store()
        f.update(child=child,script=script,script_raw=script_raw)
    ledger=saved_sources._NativeLedger(saved_sources._NATIVE_METADATA,256*M);snapshot={}
    args=saved_sources._native_inputs(f['child'],ledger,snapshot=snapshot)
    epoch=dict(source_epoch=f"c1-{f['child'].uuid.replace('-','')}-{f['native'].uuid.replace('-','')}-19")
    return f,ledger,snapshot,args,epoch


@pytest.mark.integration
@pytest.mark.parametrize('restart',[False,True])
def test_b_origin_medium_plan_payload_is_prepaid_even_on_metadata_refusal(native_graph,monkeypatch,restart):
    """A24 refusal must not conceal bytes read beyond B's prior I/O credit."""
    from aiida_qcl_negf import native_actor
    f,ledger,snapshot,args,epoch=b_origin_route(native_graph,262144,restart)
    real_port=native_actor.read_native_origin_inputs;real_open=orm.SinglefileData.open
    delivered=[];requests=[];actors=[];initial=(ledger.metadata,ledger.io)
    class Observed(native_actor._Reads):
        def __init__(self,*a,**kw):super().__init__(*a,**kw);actors.append(self)
    monkeypatch.setattr(native_actor,'_Reads',Observed)
    def port(*a,**kw):
        @contextmanager
        def opening(node,*aa,**kk):
            with real_open(node,*aa,**kk) as stream:
                class View:
                    def read(self,size):
                        requests.append(size);value=stream.read(size);delivered.append(len(value));return value
                yield View()
        with monkeypatch.context() as local:
            local.setattr(orm.SinglefileData,'open',opening)
            return real_port(*a,**kw)
    monkeypatch.setattr(native_actor,'read_native_origin_inputs',port)
    with pytest.raises(saved_sources.SourceError) as caught:
        saved_sources._native_origin(f['child'],args,snapshot,epoch,ledger,{})
    assert caught.value.code=='creator_proof_unavailable'
    reservation=[r for r in ledger.reservations if r[0]=='origin raw port full allowance']
    assert len(reservation)==1
    prepaid=reservation[0][2]
    print('B_ORIGIN_MEDIUM',json.dumps(dict(restart=restart,plan_bytes=len(f['raw_plan']),payload=sum(delivered),
        requested=sum(requests),prepaid=prepaid,actor_debit=2621440-actors[0].remaining,
        charged_metadata=initial[0]-ledger.metadata,charged_io=initial[1]-ledger.io,status=caught.value.code)))
    assert sum(delivered)>262148  # The independently reproduced former underpayment.
    assert sum(delivered)<=prepaid
    assert sum(requests)<=prepaid
    assert initial[1]-ledger.io==prepaid and initial[0]-ledger.metadata==2621440
    assert len(actors)==1 and actors[0].remaining<0


@pytest.mark.integration
@pytest.mark.parametrize('fault',['insufficient_io','failure','cancel'])
def test_b_origin_port_credit_is_atomic_nonrefundable_and_never_reset(native_graph,monkeypatch,fault):
    from aiida_qcl_negf import native_actor
    f,ledger,snapshot,args,epoch=b_origin_route(native_graph,113111,True)
    initial_metadata=ledger.metadata;cache={};opens=[];real_open=orm.SinglefileData.open
    class Cancelled(BaseException):pass
    primary=OSError('origin input read failure') if fault=='failure' else Cancelled()
    @contextmanager
    def opening(node,*a,**kw):
        opens.append(node.uuid)
        with real_open(node,*a,**kw):
            class View:
                def read(self,size):raise primary
            yield View()
    monkeypatch.setattr(orm.SinglefileData,'open',opening)
    if fault=='insufficient_io':
        ledger.io=327679
        with pytest.raises(saved_sources.SourceError) as caught:
            saved_sources._native_origin(f['child'],args,snapshot,epoch,ledger,cache)
        assert caught.value.code=='budget_exceeded'
        assert opens==[] and ledger.io==327679 and ledger.metadata==initial_metadata
        assert not any(r[0]=='origin raw port full allowance' for r in ledger.reservations)
    else:
        initial_io=ledger.io
        for attempt in (1,2):
            with pytest.raises(saved_sources.SourceError if fault=='failure' else Cancelled) as caught:
                saved_sources._native_origin(f['child'],args,snapshot,epoch,ledger,cache)
            if fault=='failure':assert caught.value.code=='creator_proof_unavailable'
            else:assert caught.value is primary
            assert ledger.metadata==initial_metadata-attempt*2621440
            assert ledger.io==initial_io-attempt*327680
            assert len(opens)==attempt and cache=={}
        assert [r for r in ledger.reservations if r[0]=='origin raw port full allowance']==[
            ('origin raw port full allowance',2621440,327680)]*2


@pytest.mark.integration
@pytest.mark.parametrize('kind',['checkpoint','final'])
@pytest.mark.parametrize('scientific_access',[False,True],indirect=True,ids=['literal-inputs','actual-inputs'])
def test_closed_api_actual_calcjob_whole_pair_readback_scalar(scientific_access,monkeypatch,kind):
    f=scientific_access;previous=f['child']
    monkeypatch.setattr(orm.Computer,'get_scheduler',lambda self:SlurmScheduler())
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=previous.caller.pk,inputs={
        **{name:previous.inputs[name] for name in ('code','plan','source_graph','native_execution','execution_id','attempt','release_id')},
        'metadata':{'call_link_label':'b_closed_api_'+kind,'options':dict(resources={'num_machines':1,'num_mpiprocs_per_machine':1})}})
    f['child']=process.node;f=mapped_source(f);source=f['source']
    if kind=='checkpoint':source.update(kind=kind,diagnostics=None,observations=None,reason='final_not_published')
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(process.node,raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    try:
        with saved_sources._native_operation(saved_sources._NATIVE_METADATA,saved_sources._NATIVE_IO) as ledger:
            code=process.parse_retrieved_output();process.update_outputs()
            outputs={r.link_label:r.node for r in process.node.base.links.get_outgoing(link_type=LinkType.CREATE).all()}
            assert code.status==302 and set(outputs)=={'native_submit_script','retrieved','result','native_binding','selection'}
            names=[r[0] for r in ledger.reservations]
            assert names.count('Results reusable whole-call workspace')==1
            assert names.count('AiiDA binder/outward cache workspace')==1
            assert names.count('Results detached retained slot')==1
            assert 'scalar selection constructor/readback' in names
            assert sum(row[1] for row in ledger.reservations)==saved_sources._NATIVE_METADATA-ledger.metadata
            print('COMPLETE_CALCJOB_LEDGER',json.dumps(dict(kind=kind,plan_bytes=len(f['child'].inputs.plan.get_content().encode()),remaining_metadata=ledger.metadata,remaining_io=ledger.io,reservations=ledger.reservations)))
        monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline called A')))
        monkeypatch.setattr(native_metadata,'describe_native_metadata_context',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline called native metadata')))
        view=service.get_native_result_source(process.node.caller.uuid,process.node.inputs.execution_id.value,
            attempt=7,calcjob_uuid=process.node.uuid,point_id=source['selected_state']['identity']['point_id'])
        assert view['result_utf8']==raw.decode() and view['point']['creator']['calcjob_uuid']==process.node.uuid
        assert view['integrity']['native_contexts']=='not_reverified'
    finally:process.close()


def test_failed_native_operation_is_terminal_without_refund():
    with saved_sources._native_operation(saved_sources._NATIVE_METADATA,saved_sources._NATIVE_IO) as ledger:
        ledger.reserve('before failed call',metadata=19)
        before=ledger.metadata
        with pytest.raises(KeyboardInterrupt):
            with saved_sources._native_operation(saved_sources._NATIVE_METADATA,saved_sources._NATIVE_IO):
                raise KeyboardInterrupt('owned operation cancelled')
        assert ledger.metadata==before
        with pytest.raises(saved_sources.SourceError,match='budget'):
            ledger.reserve('forbidden dependent reuse',metadata=1)


def rebind_actual_source_inputs(root,state,observations):
    """New synthetic CP/final fixture around unchanged actual source-owned inputs.

    No Runner/Core checkpoint was recovered/produced; numeric payloads and
    scientific-negative classification remain manufactured wire fixtures.
    """
    import h5py
    from final_diagnostics_fixture import fixed_json
    from result_v2_fixture import reference,put
    repo=next(parent for parent in Path(__file__).resolve().parents if (parent/'.build/native-origin-Runner-independent-20261030-attempt1/upload/plan.json').is_file())
    raw_plan=(repo/'.build/native-origin-Runner-independent-20261030-attempt1/upload/plan.json').read_bytes()
    raw_model=(repo/'site/proxmox-single/generated/codex-execution-2026-10-27/P07-original-plan-point-model-20261030-attempt3/model.json').read_bytes()
    assert len(raw_plan)==113111 and hashlib.sha256(raw_plan).hexdigest()=='d9006b48f839f702a8f28214f29b3ba1961c886653176b313a375c7ee9dd53ba'
    assert len(raw_model)==5595 and hashlib.sha256(raw_model).hexdigest()=='c2f8c150d6903c5ce806e4def303ddc85fadb1ffc0b5a529423220c24c4f045b'
    plan=json.loads(raw_plan);model=json.loads(raw_model);point=plan['points'][0]
    identity={**state['identity'],'execution_id':point['execution_id'],'point_id':point['id'],
        'plan_sha256':hashlib.sha256(raw_plan).hexdigest(),'model_sha256':model['configuration_hash']}
    for name,raw in (('plan.json',raw_plan),('model.json',raw_model)):(root/name).write_bytes(raw)
    replacements={name:reference(root,name,schema,'application/json') for name,schema in
        (('plan.json','qcl-negf-scientific-plan-v2'),('model.json','qcl-negf-resolved-configuration-v3'))}
    def rewrite(value):
        if type(value)is dict:
            if set(('path','bytes','sha256','schema','media_type'))<=value.keys() and value['path'] in replacements:value.update(replacements[value['path']])
            if set(identity)==set(value):value.update(identity)
            for child in value.values():rewrite(child)
        elif type(value)is list:
            for child in value:rewrite(child)
    # Rebind identity and FileRefs in actual dependency order, never overwrite raw inputs.
    with h5py.File(root/'full.h5','r+') as handle:
        del handle['metadata/identity_json'];fixed_json(handle,'metadata/identity_json',identity)
    replacements['full.h5']=reference(root,'full.h5','qcl-negf-physics-v5','application/x-hdf5')
    cp=json.loads((root/'checkpoint.json').read_bytes());rewrite(cp)
    replacements['checkpoint.json']=put(root,'checkpoint.json',cp,cp['schema'])
    for filename,dataset in (('analysis.h5','final_diagnostics/declaration_json'),('observations.h5','final_observations/declaration_json')):
        with h5py.File(root/filename,'r+') as handle:
            value=json.loads(bytes(handle[dataset][()]));rewrite(value)
            if filename=='observations.h5':
                flags=deepcopy(model['configuration']['inputs']['scattering'])
                value['request']['scattering_requested']=flags
                value['context']['problem_scattering']['fields']=deepcopy(flags)
                for coordinate,name,field in (('temperature','temperature_K','T_L_K'),('voltage','voltage_per_period_V','V_period_V')):
                    sample=dict(kind='ieee',type='f64',bits=struct.pack('>d',float(point[name])).hex())
                    value['request'][name]=sample;value['context']['inputs'][field]=deepcopy(sample)
                    value['request']['coordinate_checks'][coordinate]=dict(status='verified',reason=None,requested=deepcopy(sample),actual=deepcopy(sample))
            del handle['metadata/identity_json'];fixed_json(handle,'metadata/identity_json',identity)
            del handle[dataset];fixed_json(handle,dataset,value)
        replacements[filename]=reference(root,filename,'qcl-negf-physics-analysis-v5','application/x-hdf5')
    manifest=json.loads((root/state['manifest']['path']).read_bytes());rewrite(manifest)
    state['identity']=identity;state['manifest']=put(root,'selected.json',manifest,manifest['schema'])
    observations.update(replacements['observations.h5'])
    assert (root/'plan.json').read_bytes()==raw_plan and (root/'model.json').read_bytes()==raw_model


@pytest.mark.parametrize('fault',['unknown_key','bounded_layout','foreign_target','execution','oversized_schema','shadow_cache_info'])
def test_binding_cache_profile_holds_before_consumer_factory_and_preserves_cache(monkeypatch,fault):
    import functools,sys
    import aiida_qcl_negf.science_association as owner
    import qcl_negf_contracts as contracts
    original=owner._binding_validator;before=original.cache_info();calls=[]
    if fault=='foreign_target':
        def foreign(*a):calls.append('foreign');return original(*a)
        wrapper=functools.cache(foreign);wrapper.__wrapped__=original.__wrapped__
    else:
        wrapper=functools.lru_cache(maxsize=1 if fault=='bounded_layout' else None)(original.__wrapped__)
        if fault=='shadow_cache_info':
            def shadow():calls.append('shadow');raise AssertionError('foreign cache_info reached')
            wrapper.cache_info=shadow
        if fault=='unknown_key':wrapper('scientific-plan.schema.json','#/$defs/Foreign')
        elif fault in ('execution','oversized_schema'):
            value=wrapper('result-common.schema.json','#/$defs/Id')
            if fault=='execution':value._validators=[]
            else:value._registry['https://qcl-negf.invalid/schema/result-common.schema.json'].contents['private_oversized']='x'*(6*M)
    def observe(frame,event,value):
        if event=='call' and frame.f_code is contracts.schema_validator.__code__:calls.append('factory')
    ledger=saved_sources._NativeLedger(saved_sources._NATIVE_METADATA,saved_sources._NATIVE_IO)
    with monkeypatch.context() as patch:
        patch.setattr(owner,'_binding_validator',wrapper)
        old=sys.getprofile();sys.setprofile(observe)
        try:
            with pytest.raises(saved_sources.SourceError) as caught:ledger.workspaces()
        finally:sys.setprofile(old)
    assert caught.value.detail=='native_binding_cache_profile' and not calls and ledger.failed
    assert original.cache_info()==before
    assert [r[0] for r in ledger.reservations]==['Results reusable whole-call workspace','AiiDA binder/outward cache workspace']


@pytest.mark.integration
@pytest.mark.parametrize('field',['metadata','io','identity_io'])
def test_retained_slot_and_io_admission_precede_original_public_call(scientific_access,monkeypatch,field):
    f=scientific_access;calls=[]
    ledger=saved_sources._NativeLedger(saved_sources._NATIVE_METADATA,saved_sources._NATIVE_IO);ledger.workspaces()
    if field=='metadata':ledger.reserve('already retained',metadata=ledger.metadata-saved_sources._NATIVE_RETAINED_SLOT+1)
    else:ledger.reserve('already delivered',io=ledger.io-saved_sources._NATIVE_OPERATION_IO-(6*M if field=='identity_io' else 0)+1)
    before=(ledger.metadata,ledger.io)
    monkeypatch.setattr(native_metadata,'describe_native_metadata_context',lambda *a,**k:calls.append(True))
    with pytest.raises(saved_sources.SourceError):saved_sources._native_contexts(f['root'],f['source'],f['refs']['plan'],f['refs']['model'],{},ledger)
    assert not calls and ledger.failed
    assert ledger.metadata==before[0]-(saved_sources._NATIVE_RETAINED_SLOT if field!='metadata' else 0)
    assert ledger.io==before[1]-(saved_sources._NATIVE_OPERATION_IO if field=='identity_io' else 0)
    with pytest.raises(saved_sources.SourceError):ledger.results('forbidden next ref')
    assert not calls


@pytest.mark.parametrize('scientific_access',[True],indirect=True,ids=['actual-inputs'])
@pytest.mark.parametrize('location,extra_bytes,maximum',[('offline',9*(1<<20),320*M),('offline',21*(1<<20),320*M),('parent',21*(1<<20),320*M),('offline',9*(1<<20),8404992)])
def test_original_complete_scalar_refuses_before_full_copy(scientific_access,monkeypatch,location,extra_bytes,maximum):
 f=mapped_source(scientific_access);source=f['source'];source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published');f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(f['child'],raw)
 monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
 result,binding=saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=saved_sources._NATIVE_IO)
 for label,node in [('result',result),('native_binding',binding)]:node.base.links.add_incoming(f['child'],LinkType.CREATE,label);node.store()
 pins={name:raw_pin(node,node.get_content().encode()) for name,node in [('result',result),('native_binding',binding)]}
 expected=saved_sources._readback_native_outputs(f['child'],expected_result=pins['result'],expected_binding=pins['native_binding'],maximum_metadata_bytes=saved_sources._NATIVE_METADATA)
 scalar=orm.Dict(dict={**expected,'oversized':'x'*extra_bytes});scalar.base.links.add_incoming(f['child'],LinkType.CREATE,'selection');scalar.store()
 child=f['child'] if location=='offline' else next_child(f['child'],parent_binding=binding)
 if location=='parent':retrieved(child,raw)
 seen=[];original=orm.Dict.get_dict
 def inspect(node):
  value=original(node)
  if node.uuid==scalar.uuid:seen.append(dict(W=saved_sources._native_weight(value),bytes=len(value['oversized'])))
  return value
 monkeypatch.setattr(orm.Dict,'get_dict',inspect)
 with saved_sources._native_operation(maximum,saved_sources._NATIVE_IO) as ledger:
  with pytest.raises(saved_sources.SourceError) as caught:
   if location=='offline':saved_sources._retained_native_result_source(child,point_id=source['selected_state']['identity']['point_id'],maximum_metadata_bytes=saved_sources._NATIVE_METADATA)
   else:saved_sources._prepare_native_outputs(child,raw,maximum_metadata_bytes=saved_sources._NATIVE_METADATA,maximum_io_bytes=saved_sources._NATIVE_IO)
  record=dict(location=location,extra_bytes=extra_bytes,observed=seen,failure=dict(code=caught.value.code,detail=caught.value.detail),remaining_metadata=ledger.metadata,remaining_io=ledger.io,reservations=ledger.reservations)
  print('SCALAR_ORIGINAL_LEDGER',json.dumps(record))
  assert not seen,'Original malformed stored scalar fully materialized before finite admission'
  assert caught.value.code=='budget_exceeded' and caught.value.detail=='native_scalar_text_bound' and ledger.failed


def _scalar_maximum():
    # Every variable-width field reaches its published closed-shape maximum.
    return dict(calcjob_uuid='00000000-0000-4000-8000-000000000001',execution_id='A'*128,
        attempt=18446744073709551615,result_sha256='a'*64,commits={},recovery_commit_sha256=None,
        native_result=dict(node_uuid='00000000-0000-4000-8000-000000000002',bytes='18446744073709551615',sha256='b'*64),
        native_binding=dict(node_uuid='00000000-0000-4000-8000-000000000003',bytes='18446744073709551615',sha256='c'*64),
        performance_status='unavailable',performance_reason='native_technical_context_unavailable')


def _scalar_reordered(value):
    return {key:_scalar_reordered(child) for key,child in reversed(list(value.items()))} if type(value) is dict else value


def _scalar_create(creator,value):
    node=orm.Dict(dict=value);node.base.links.add_incoming(creator,LinkType.CREATE,'selection');return node.store()


@pytest.mark.parametrize('case',['healthy','reordered','unicode','maximum','missing','wrong_type','tight','version','adapter'])
def test_scalar_sql_admission_has_no_original_Dict_hydration(aiida_profile_clean,monkeypatch,case):
    import aiida
    from sqlalchemy import event
    from aiida.orm.nodes.attributes import NodeAttributes
    creator=orm.CalcJobNode(process_type='aiida.calculations:qcl_negf.execution').store();creator_uuid=creator.uuid
    expected=_scalar_maximum();expected['calcjob_uuid']=creator_uuid
    if case!='maximum':expected['execution_id']='execution';expected['attempt']=1
    if case=='unicode':expected['execution_id']='é Ж 🧪' # JSON gate only, not scientific Id.
    value=_scalar_reordered(expected) if case=='reordered' else deepcopy(expected)
    if case=='wrong_type':
        node=orm.SinglefileData(file=io.BytesIO(b'{}'),filename='wrong.json');node.base.links.add_incoming(creator,LinkType.CREATE,'selection');node.store()
    elif case!='missing':node=_scalar_create(creator,value)
    backend=creator.backend;session=backend.get_session();engine=session.get_bind();statements=[];copies=[]
    def trace(connection,cursor,statement,parameters,context,executemany):statements.append(statement)
    original=orm.Dict.get_dict;original_all=NodeAttributes.all.fget
    def observe(node):copies.append('get_dict');return original(node)
    def observe_all(attributes):copies.append('attributes.all');return original_all(attributes)
    monkeypatch.setattr(orm.Dict,'get_dict',observe);monkeypatch.setattr(NodeAttributes,'all',property(observe_all))
    if case=='version':monkeypatch.setattr(aiida,'__version__','unsupported')
    if case=='adapter':backend=object()
    ledger=saved_sources._NativeLedger(saved_sources._NATIVE_SCALAR_METADATA-1 if case=='tight' else 320*M,256*M)
    event.listen(engine,'before_cursor_execute',trace)
    try:
        if case in ('healthy','reordered','unicode','maximum'):
            actual=saved_sources._native_scalar_value(backend,creator_uuid,ledger)
            assert actual==expected
            if case=='maximum':assert len(json.dumps(value).encode())==835 and saved_sources._native_weight(value)==8242
        else:
            with pytest.raises(saved_sources.SourceError):saved_sources._native_scalar_value(backend,creator_uuid,ledger)
            assert ledger.failed
        assert not copies
        if case in ('tight','version','adapter'):assert not statements
        else:assert all(statement.lstrip().upper().startswith(('SELECT','PRAGMA','WITH')) for statement in statements)
        print('SCALAR_SQL_GATE',json.dumps(dict(case=case,statements=statements,remaining_metadata=ledger.metadata,remaining_io=ledger.io)))
    finally:event.remove(engine,'before_cursor_execute',trace)


@pytest.mark.parametrize('variant,location',[('reordered','offline'),('extra','offline'),('nested_extra','offline'),('deep','offline'),('wrong_pin','offline'),('reordered','parent'),('extra','parent')])
@pytest.mark.parametrize('scientific_access',[True],indirect=True,ids=['actual-inputs'])
def test_scalar_complete_exact_semantics_after_bounded_admission(scientific_access,monkeypatch,variant,location):
    f=mapped_source(scientific_access);source=f['source'];source.update(kind='checkpoint',diagnostics=None,observations=None,reason='final_not_published')
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(f['child'],raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    result,binding=saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=320*M,maximum_io_bytes=256*M)
    for label,node in [('result',result),('native_binding',binding)]:node.base.links.add_incoming(f['child'],LinkType.CREATE,label);node.store()
    with binding.open(mode='rb') as stream:binding_raw=stream.read()
    expected=saved_sources._readback_native_outputs(f['child'],expected_result=raw_pin(result,raw),expected_binding=raw_pin(binding,binding_raw),maximum_metadata_bytes=320*M)
    value=_scalar_reordered(expected)
    if variant=='extra':value['extra']=1
    if variant=='nested_extra':value['native_result']['extra']=None
    if variant=='deep':
        nested=0
        for _ in range(48):nested={'next':nested}
        value['commits']=nested
    if variant=='wrong_pin':value['native_result']['node_uuid']=f['child'].inputs.plan.uuid
    _scalar_create(f['child'],value)
    child=f['child'] if location=='offline' else next_child(f['child'],parent_binding=binding)
    if location=='parent':retrieved(child,raw)
    else:
        monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline A')))
        monkeypatch.setattr(native_metadata,'describe_native_metadata_context',lambda *a,**k:(_ for _ in ()).throw(AssertionError('offline Results')))
    with saved_sources._native_operation(320*M,256*M) as ledger:
        def invoke():
            if location=='offline':return saved_sources._retained_native_result_source(child,point_id=source['selected_state']['identity']['point_id'],maximum_metadata_bytes=320*M)
            return saved_sources._prepare_native_outputs(child,raw,maximum_metadata_bytes=320*M,maximum_io_bytes=256*M)
        if variant=='reordered':
            returned=invoke()
            if location=='offline':assert returned['status']=='available' and returned['result_utf8']==raw.decode()
            else:
                assert len(returned)==2
                for node in returned:assert not node.is_stored
        else:
            with pytest.raises(saved_sources.SourceError) as caught:invoke()
            assert caught.value.code=='source_identity_mismatch' and ledger.failed
            if location=='parent':assert set(child.outputs)=={'retrieved'}
        print('SCALAR_EXACT_LEDGER',json.dumps(dict(variant=variant,location=location,remaining_metadata=ledger.metadata,remaining_io=ledger.io,reservations=ledger.reservations)))


def test_scalar_borrowed_session_is_not_flushed_committed_or_closed(aiida_profile_clean):
    from sqlalchemy import event
    from aiida.storage.sqlite_zip.models import DbUser
    creator=orm.CalcJobNode(process_type='aiida.calculations:qcl_negf.execution').store();uuid=creator.uuid
    expected=_scalar_maximum();expected['calcjob_uuid']=uuid;_scalar_create(creator,expected)
    backend=creator.backend;session=backend.get_session();connection=session.connection();pending=DbUser(email='scalar-pending@localhost');session.add(pending)
    signals=[];callbacks={}
    for name in ('before_commit','after_rollback','before_flush'):
        def callback(*args,_name=name):signals.append(_name)
        callbacks[name]=callback;event.listen(session,name,callback)
    try:
        assert saved_sources._native_scalar_value(backend,uuid,saved_sources._NativeLedger(320*M,256*M))==expected
        assert pending in session.new and not signals and not connection.closed and session.is_active
        assert saved_sources._native_scalar_rows(connection,'SELECT count(*) FROM db_dbuser WHERE email = ?',('scalar-pending@localhost',))==[(0,)]
    finally:
        for name,callback in callbacks.items():event.remove(session,name,callback)


@pytest.mark.parametrize('mutation',['duplicate','rebind','delete'])
def test_scalar_conditional_statement_rechecks_actual_CREATE_snapshot(aiida_profile_clean,monkeypatch,mutation):
    creator=orm.CalcJobNode(process_type='aiida.calculations:qcl_negf.execution').store();uuid=creator.uuid
    expected=_scalar_maximum();first=_scalar_create(creator,expected);second=orm.Dict(dict=expected).store();second_uuid=second.uuid
    actual=saved_sources._native_scalar_rows;changed=[]
    def interleave(connection,statement,parameters=()):
        rows=actual(connection,statement,parameters)
        if statement==saved_sources._NATIVE_SCALAR_SQLITE_IDENTITY_SQL:
            assert len(rows)==1
            if mutation=='duplicate':
                connection.exec_driver_sql("INSERT INTO db_dblink (input_id, output_id, type, label) SELECT creator.id, target.id, 'create', 'selection' FROM db_dbnode AS creator, db_dbnode AS target WHERE creator.uuid = ? AND target.uuid = ?",(uuid,second_uuid)).close()
            elif mutation=='rebind':
                connection.exec_driver_sql("UPDATE db_dblink SET output_id = (SELECT id FROM db_dbnode WHERE uuid = ?) WHERE input_id = (SELECT id FROM db_dbnode WHERE uuid = ?) AND label = 'selection' AND type = 'create'",(second_uuid,uuid)).close()
            else:
                connection.exec_driver_sql("DELETE FROM db_dblink WHERE input_id = (SELECT id FROM db_dbnode WHERE uuid = ?) AND label = 'selection' AND type = 'create'",(uuid,)).close()
            changed.append(True)
        return rows
    monkeypatch.setattr(saved_sources,'_native_scalar_rows',interleave)
    ledger=saved_sources._NativeLedger(320*M,256*M)
    with pytest.raises(saved_sources.SourceError) as caught:saved_sources._native_scalar_value(creator.backend,uuid,ledger)
    assert changed==[True] and caught.value.detail=='native_scalar_selection_missing' and ledger.failed


@pytest.mark.parametrize('cancel',[False,True])
def test_scalar_cursor_cleanup_preserves_primary(cancel):
    import asyncio
    primary=asyncio.CancelledError('cancel') if cancel else ValueError('read');closed=[]
    class Cursor:
        def fetchmany(self,n):raise primary
        def close(self):closed.append(True);raise RuntimeError('secondary close')
    class Connection:
        def exec_driver_sql(self,*args):return Cursor()
    with pytest.raises(BaseException) as caught:saved_sources._native_scalar_rows(Connection(),'fixed controlled SELECT')
    assert caught.value is primary and closed==[True]


def test_scalar_cursor_successful_read_does_not_hide_close_failure():
    error=RuntimeError('close')
    class Cursor:
        def fetchmany(self,n):return []
        def close(self):raise error
    class Connection:
        def exec_driver_sql(self,*args):return Cursor()
    with pytest.raises(RuntimeError) as caught:saved_sources._native_scalar_rows(Connection(),'fixed controlled SELECT')
    assert caught.value is error
