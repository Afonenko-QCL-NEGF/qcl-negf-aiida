"""B SOURCE/L: genuine isolated ORM links; no installed storage/creator claim.

The nonabsent native creator gate remains deliberately HOLD pending S00. An
absent-only pair exercises publication mechanics, never B01 creator-positive.
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
from qcl_negf_contracts.messages import encode

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
        saved_sources._readback_native_outputs(process.node,expected_result=pins['result'],expected_binding=pins['native_binding'],maximum_metadata_bytes=32*M)


def test_one_nonrefundable_ledger_reserves_atomic_boundaries():
    ledger=saved_sources._NativeLedger(10,20)
    ledger.reserve('first',metadata=7,io=11)
    with pytest.raises(ValueError):ledger.reserve('too large',metadata=4,io=1)
    assert (ledger.metadata,ledger.io)==(3,9)
    ledger.reserve('remaining',metadata=3,io=9)
    with pytest.raises(ValueError):ledger.reserve('repeat',metadata=1)
    assert (ledger.metadata,ledger.io)==(0,0)


@pytest.fixture
def scientific_access(native_graph,tmp_path,monkeypatch):
    """Original CP/model/final bytes and ORM plan use the SAME input, no solve."""
    import sys
    monkeypatch.syspath_prepend(str(Path(__file__).parents[2]/'qcl-negf-results'/'tests'))
    from consumer_context_fixture import fixture as source_fixture
    root=tmp_path/'selected';root.mkdir()
    state,observations,observation,checkpoint,_=source_fixture(root,plan_kind='scientific_plan_v2')
    manifest=json.loads((root/state['manifest']['path']).read_bytes())
    refs={a['role']:{k:a[k] for k in ('path','bytes','sha256','schema','media_type')} for a in manifest['artifacts'] if a['role'] in ('plan','model')}
    def blob(raw):return orm.SinglefileData(file=io.BytesIO(raw),filename='input.json')
    original_plan=(root/refs['plan']['path']).read_bytes()
    plan=blob(original_plan).store();raw_graph=encode(manifest['provenance']);graph=blob(raw_graph).store()
    f=native_graph;native=blob(b'{}');declaration=deepcopy(f['execution'])
    declaration.update(execution_id='exec',input_plan=raw_pin(plan,original_plan),source_graph=raw_pin(graph,raw_graph))
    declaration['storage']['execution_binding_uuid']=native.uuid
    native.set_file(io.BytesIO(encode(declaration)),filename='native.json');native.store()
    code=f['code'];release=f['calc'].inputs.release_id;attempt=orm.Int(7).store();execution=orm.Str('exec').store()
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
    result.update(plan_fingerprint=frozen['fingerprint'],plan_scientific_fingerprint=frozen['scientific_fingerprint'],selected_execution_id='exec',
        root_definition_id=frozen['root_definition_id'],name=frozen['name'])
    result['executions']=[{k:frozen['executions'][0][k] for k in ('id','definition_id','variant_id','method_id','purpose','label','operation','point_ids','repetition')}]
    row=result['points'][0];row.update(execution_id='exec',attempt=int(state['identity']['attempt']))
    row['coordinates'].update(temperature_K=200.)
    row['data'].pop('progress_events',None)
    row['data']['phase_source']=dict(status='unavailable',session_id=None,current_path=None,reason='not_recorded')
    source=dict(schema='qcl-negf.native-point-source.v1',kind='final',artifact_root='.',relation='current_point_attempt',selected_state=state,
        diagnostics=next({k:a[k] for k in ('path','bytes','sha256','schema','media_type')} for a in manifest['artifacts'] if a['path']=='analysis.h5'),
        observations=observations,reason=None)
    row['data']['native_source']=source
    # Independent literal identity and row order, then align BOTH raw inventories.
    assert {k:state['identity'][k] for k in ('run_id','execution_id','point_id','attempt')}==dict(run_id='run',execution_id='exec',point_id='point',attempt='1')
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
        saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=32*M,maximum_io_bytes=256*M)
    assert caught.value.code=='creator_proof_unavailable'
    assert [r['point_attempt'] for r in caught.value.detail]==[row['attempt'],row['attempt']]
    assert caught.value.detail[0]['operand']=='original_invocation_calcjob_origin'
    assert set(f['child'].outputs)=={'retrieved'}


@pytest.mark.integration
def test_actual_public_source_rejects_full_manifest_ref_before_constructor(scientific_access,monkeypatch):
    f=scientific_access;source=f['source'];source['selected_state']['manifest']['sha256']='f'*64
    f['result']['attempt_history']=deepcopy(f['result']['points']);raw=encode(f['result']);retrieved(f['child'],raw)
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw,((f['root'],source,f['refs']['plan'],f['refs']['model']),)))
    with pytest.raises(saved_sources.SourceError) as caught:
        saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=32*M,maximum_io_bytes=256*M)
    assert caught.value.code=='source_identity_mismatch'
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
        saved_sources._prepare_native_outputs(process.node,raw,maximum_metadata_bytes=32*M,maximum_io_bytes=256*M)
    assert caught.value is primary and set(process.outputs)==set()


@pytest.mark.integration
def test_parse_and_output_readback_share_one_original_nonrefundable_credit(native_process):
    process,raw,f=native_process
    with saved_sources._native_operation(32*M,256*M) as ledger:
        assert process.parse_retrieved_output().status==302
        assert len([r for r in ledger.reservations if r[0]=='A selected access full lifetime'])==1
        # Actual parser, output byte copies, genuine CREATE/readback and scalar
        # construction consumed this SAME ledger; no helper reset/refund.
        names=[r[0] for r in ledger.reservations]
        assert 'pure binder full allowance' in names and 'offline pure binder full allowance' in names
        assert 'scalar selection constructor/readback' in names
        assert sum(r[1] for r in ledger.reservations)==32*M-ledger.metadata
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
    with saved_sources._native_operation(32*M,256*M) as ledger:
        with pytest.raises(saved_sources.SourceError) as first:
            saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=32*M,maximum_io_bytes=256*M)
        assert first.value.code=='creator_proof_unavailable'
        after=ledger.metadata
        with pytest.raises(saved_sources.SourceError) as second:
            saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=32*M,maximum_io_bytes=256*M)
        assert second.value.code=='budget_exceeded'
        assert ledger.metadata<after and calls==[True]


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
        saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=32*M,maximum_io_bytes=256*M)
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
        saved_sources._prepare_native_outputs(child,raw,maximum_metadata_bytes=32*M,maximum_io_bytes=256*M)
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
    with saved_sources._native_operation(32*M,256*M) as ledger:
        with pytest.raises(saved_sources.SourceError) as caught:
            saved_sources._prepare_native_outputs(process.node,raw,maximum_metadata_bytes=8*M-1 if cap=='metadata' else 32*M,
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
    with saved_sources._native_operation(32*M,256*M) as ledger:
        with pytest.raises(type(primary) if primary is not None else ValueError) as caught:
            saved_sources._prepare_native_outputs(f['child'],raw,maximum_metadata_bytes=32*M,maximum_io_bytes=256*M)
        assert caught.value is (primary if primary is not None else secondary)
        operations=[entry[0] for entry in ledger.reservations]
        assert operations.count('FinalDiagnosticsReader construction')==1
        assert operations.count('FinalDiagnosticsReader describe')==1
        assert operations.count('FinalObservationsReader construction')==(target=='observations')
        assert operations.count('FinalObservationsReader describe')==(target=='observations')
        assert sum(entry[1] for entry in ledger.reservations)==32*M-ledger.metadata
        assert sum(entry[2] for entry in ledger.reservations)==256*M-ledger.io
    assert described==[target]
    assert closes=={'diagnostics':1,'observations':int(target=='observations'),'state':1}
    assert lifetime==['enter','exit'] and constructors==[]
    assert set(f['child'].outputs)=={'retrieved'}
