"""Help-only transport and genuine original ORM fixtures; synthetic, never native N."""
from copy import deepcopy
import io
import json
from pathlib import Path

import pytest
from aiida import orm
from aiida.common.links import LinkType
from plumpy.process_states import PauseInterruption

from aiida_qcl_negf import native_actor, native_backend, model_help
from test_model_help_input import modern_plan
from test_model_help_retention import inputs, path_for
from test_native_actor import native_graph
from test_native_selected_sources import packet


def test_exact_tuple_has_no_general_directory_permission():
    assert native_actor._help_retrieval(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    assert native_actor._help_retrieval(list(native_actor.CONTROL_FILES)+[list(model_help.HELP_RETRIEVE)])
    for extra in [('result','.',2),('result/executions','.',1),('executions','.',2),
                  ('result/executions','.',2,'extra'),model_help.HELP_RETRIEVE, 'result/executions']:
        with pytest.raises(ValueError):native_actor._help_retrieval(list(native_actor.CONTROL_FILES)+[extra,extra])


@pytest.mark.parametrize('fault',['array','symlink','extra_root','foreign_execution','owner','sum'])
def test_help_tree_refused_before_payload_read_or_destination(tmp_path,monkeypatch,fault):
    plan=modern_plan();s=inputs(plan,attempt=7);work=tmp_path/'job';work.mkdir()
    target=work/path_for(s);target.parent.mkdir(parents=True);target.write_bytes(json.dumps(s).encode())
    if fault=='array':(target.parent/'data.h5').write_bytes(b'never read')
    elif fault=='symlink':target.unlink();target.symlink_to(tmp_path/'foreign')
    elif fault=='extra_root':(work/'result'/'a.h5').write_bytes(b'never read')
    elif fault=='foreign_execution':target.parent.parent.parent.rename(target.parent.parent.parent.with_name('foreign'))
    elif fault=='owner':target.chmod(0o666)
    else:monkeypatch.setattr(model_help,'MAX_TOTAL',1)
    from types import SimpleNamespace as NS
    packet=NS(launch={'remote_workdir':str(work),'execution_id':s['context']['execution_id']},trees=[])
    budget=native_backend._HelpBudget();reads=[]
    original=native_backend._Tree.read
    def read(*a,**k):reads.append(a[1]);return original(*a,**k)
    monkeypatch.setattr(native_backend._Tree,'read',read)
    try:
        with pytest.raises((ValueError,PauseInterruption,OSError)):
            native_backend._help_tree(packet,plan,budget)
        assert reads==[]
    finally:
        for tree in packet.trees:tree.close()


def test_help_tree_exact_original_bytes_and_prepaid_copies(tmp_path):
    from types import SimpleNamespace as NS
    plan=modern_plan();s=inputs(plan,attempt=7);work=tmp_path/'job';work.mkdir()
    path=path_for(s);target=work/path;target.parent.mkdir(parents=True)
    raw=b' \n'+json.dumps(s).encode()+b'\n';target.write_bytes(raw)
    packet=NS(launch={'remote_workdir':str(work),'execution_id':s['context']['execution_id']},trees=[])
    budget=native_backend._HelpBudget()
    try:
        result=native_backend._help_tree(packet,plan,budget)
        assert result=={path[len('result/executions/'):]:raw}
        assert any('copies' in row[0] for row in budget.ledger.reservations)
        for tree in packet.trees:tree.check()
    finally:
        for tree in packet.trees:tree.close()


@pytest.fixture
def ordinary_process(aiida_profile_clean,aiida_computer_local):
    from aiida.manage import get_manager
    from aiida_qcl_negf.calculation import QCLExecutionCalculation
    from test_model_help_input import stationary_plan_bytes
    computer=aiida_computer_local()
    code=orm.InstalledCode(computer=computer,filepath_executable='/synthetic/never-executed',
                           default_calc_job_plugin='qcl_negf.execution').store()
    raw=stationary_plan_bytes();plan=orm.SinglefileData(file=io.BytesIO(raw),filename='plan.json').store()
    run=orm.WorkChainNode(process_type='aiida.workflows:qcl_negf.plan')
    run.base.links.add_incoming(plan,LinkType.INPUT_WORK,'plan');run.store()
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=run.pk,inputs=dict(
        code=code,plan=plan,attempt=orm.Int(2),execution_id=orm.Str('execution-1'),
        metadata=dict(call_link_label='capture',options=dict(resources={'num_machines':1,'num_mpiprocs_per_machine':1}))))
    try:yield process,run,json.loads(raw)
    finally:process.close()


@pytest.mark.integration
@pytest.mark.parametrize('fault',[None,'invalid','missing','whitespace_plan'])
def test_genuine_create_input_parser_negative_science_and_getter(ordinary_process,fault,monkeypatch,record_property):
    from aiida_qcl_negf import service
    process,run,plan=ordinary_process;s=inputs(plan);path=path_for(s)
    raw=b' \n'+json.dumps(s).encode()+b'\n'
    if fault=='invalid':raw=raw.replace(b'solver_input',b'not_solver_input')
    retrieved=orm.FolderData()
    retained_plan=None
    if fault=='whitespace_plan':
        from test_model_help_input import stationary_plan_bytes
        retained_plan=b' \n'+stationary_plan_bytes()+b'\n'
        retrieved.base.repository.put_object_from_bytes(retained_plan,'result/scientific_plan.json')
    if fault!='missing':retrieved.base.repository.put_object_from_bytes(raw,path)
    retrieved.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');retrieved.store()
    assert process.parse_retrieved_output().status==300
    process.update_outputs()
    outputs={r.link_label:r.node for r in process.node.base.links.get_outgoing(link_type=LinkType.CREATE).all()}
    assert set(outputs)=={'retrieved','inventory','model_help_index'}
    index=outputs['model_help_index'].get_dict();model_help.c1(index)
    ledgers=[];original_credit=model_help.credit
    def credit():
        value=original_credit();ledgers.append(value);return value
    monkeypatch.setattr(model_help,'credit',credit)
    dto=service.get_model_help_input(run.uuid,'execution-1','point-1',attempt=2,
                                    calcjob_uuid=process.node.uuid,fragment_id='runner-inputs')
    assert dto['frozen_plan']['bytes']==(retained_plan if retained_plan is not None else process.inputs.plan.base.repository.get_object_content('plan.json',mode='rb'))
    assert len(ledgers)==1
    record_property('help_metadata_work_credit_reserved',sum(row[1] for row in ledgers[0].reservations))
    record_property('help_io_credit_reserved',sum(row[2] for row in ledgers[0].reservations))
    record_property('snapshot_raw_bytes',len(raw))
    record_property('retained_plan_raw_bytes',len(dto['frozen_plan']['bytes']))
    if fault=='whitespace_plan':
        # Measured work/copy credit for two genuine 113111B original plans,
        # plus index/DTO; 128 MiB cannot admit this ordinary existing path.
        assert sum(row[1] for row in ledgers[0].reservations)>128*1024*1024
    if fault in (None,'whitespace_plan'):
        assert dto['snapshot']==model_help.record(raw)
        model_help.c1(json.loads(dto['snapshot']['bytes']))
        assert dto['input_status']==dict(status='available',reason='ok')
        assert index['entries'][0]['sha256']==dto['snapshot']['sha256']
    else:
        assert dto['snapshot'] is None
        assert dto['input_status']==(dict(status='invalid',reason='snapshot_invalid') if fault=='invalid' else
                                     dict(status='unavailable',reason='snapshot_missing'))
    view=dict(format='qcl-negf.model-help-view.v1',context=dto['context'],
        binding=dict(status='unknown',reason='source_binding_missing',release_id=None,
                     release_manifest_sha256=None,source_revision=None,bundle_sha256=None),
        fragment=None,inputs=[],input_status=dto['input_status'])
    from qcl_negf_contracts.model_help import validate_model_help_view
    assert validate_model_help_view(view,retained=dto['retained']) is view


@pytest.mark.integration
@pytest.mark.parametrize('preexisting',[False,True])
def test_genuine_presubmit_tuple_removal_is_legal_and_optional(native_graph,tmp_path,preexisting):
    calc=native_graph['calc'];work=tmp_path/'original-job';work.mkdir()
    calc.set_remote_workdir(str(work))
    controls=list(native_actor.CONTROL_FILES)+[native_actor.TECHNICAL_FILE]
    calc.set_retrieve_list(controls+[model_help.HELP_RETRIEVE])
    if preexisting:(work/'result').mkdir()
    native_backend._admit_help_submit(calc,dict(remote_workdir=str(work)),native_backend._HelpBudget())
    assert native_actor._help_retrieval(calc.get_retrieve_list()) is not preexisting
    assert [item for item in calc.get_retrieve_list() if isinstance(item,str)]==controls
    assert list(work.iterdir())==([work/'result'] if preexisting else [])


@pytest.mark.integration
def test_presubmit_cancellation_is_primary_and_never_removes_tuple(native_graph,tmp_path,monkeypatch):
    calc=native_graph['calc'];calc.set_remote_workdir(str(tmp_path))
    calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    primary=PauseInterruption('primary user pause')
    monkeypatch.setattr(native_backend,'_Tree',lambda *a,**k:(_ for _ in ()).throw(primary))
    with pytest.raises(PauseInterruption) as error:
        native_backend._admit_help_submit(calc,dict(remote_workdir=str(tmp_path)),native_backend._HelpBudget())
    assert error.value is primary and native_actor._help_retrieval(calc.get_retrieve_list())


from test_native_parser import scientific_access


@pytest.mark.integration
@pytest.mark.parametrize('scientific_access',[True],indirect=True)
def test_native_local7_transport2_genuine_source_and_public_view(scientific_access,monkeypatch):
    from aiida_qcl_negf import saved_sources,service
    from aiida_qcl_negf.parser import QCLExecutionParser
    from qcl_negf_contracts.messages import encode
    from qcl_negf_contracts.model_help import validate_model_help_view
    from test_native_parser import mapped_source,access,raw_pin
    f=scientific_access;previous=f['child']
    child=orm.CalcJobNode(computer=previous.computer,process_type=previous.process_type)
    child.base.links.add_incoming(previous.caller,LinkType.CALL_CALC,'help_transport_two')
    for name in ('plan','source_graph','native_execution','code','release_id','execution_id'):
        child.base.links.add_incoming(previous.inputs[name],LinkType.INPUT_CALC,name)
    child.base.links.add_incoming(orm.Int(2).store(),LinkType.INPUT_CALC,'attempt');child.store();f['child']=child
    f=mapped_source(f);row=f['result']['points'][0];row['attempt']=7
    f['source'].update(relation='retained_source',reason='last_committed_before_pause')
    f['result']['attempt_history']=deepcopy(f['result']['points'])
    raw=encode(f['result']);plan=json.loads(child.inputs.plan.get_content())
    snapshot=inputs(plan,attempt=7);snapshot_raw=b' \n'+encode(snapshot)+b'\n';path=path_for(snapshot)
    folder=orm.FolderData();folder.base.repository.put_object_from_bytes(raw,'series_result.json')
    folder.base.repository.put_object_from_bytes(snapshot_raw,path)
    folder.base.links.add_incoming(child,LinkType.CREATE,'retrieved');folder.store()
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:
        access(raw,((f['root'],f['source'],f['refs']['plan'],f['refs']['model']),)))
    parser=QCLExecutionParser(child);parser.parse()
    for label,node in parser.outputs.items():
        node.base.links.add_incoming(child,LinkType.CREATE,label);node.store()
    assert {'result','native_binding','model_help_index'}<=set(child.outputs)
    with child.outputs.native_binding.open(mode='rb') as handle:binding_raw=handle.read()
    scalar=saved_sources._readback_native_outputs(child,expected_result=raw_pin(child.outputs.result,raw),
        expected_binding=raw_pin(child.outputs.native_binding,binding_raw),maximum_metadata_bytes=saved_sources._NATIVE_METADATA)
    selection=orm.Dict(dict=scalar);selection.base.links.add_incoming(child,LinkType.CREATE,'selection');selection.store()
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:pytest.fail('getter touched native storage'))
    dto=service.get_model_help_input(child.caller.uuid,child.inputs.execution_id.value,plan['points'][0]['id'],
        attempt=2,calcjob_uuid=child.uuid,fragment_id='runner-inputs')
    assert dto['context']['attempt']==2 and json.loads(dto['snapshot']['bytes'])['context']['attempt']==7
    assert dto['snapshot']==model_help.record(snapshot_raw)
    assert dto['retained']['selection']['capture_context']==snapshot['context']
    view=dict(format='qcl-negf.model-help-view.v1',context=dto['context'],
        binding=dict(status='unknown',reason='source_binding_missing',release_id=dto['release_id'],
                     release_manifest_sha256=None,source_revision=None,bundle_sha256=None),
        fragment=None,inputs=[],input_status=dto['input_status'])
    assert validate_model_help_view(view,retained=dto['retained']) is view


@pytest.mark.integration
def test_stock_tuple_retrieval_original_metadata_only_and_actor_gate(packet,tmp_path):
    import asyncio
    from aiida.engine.daemon.execmanager import retrieve_calculation
    p=packet;controls=list(native_actor.CONTROL_FILES)
    p.calc.set_retrieve_list(controls+[model_help.HELP_RETRIEVE])
    native_backend._admit_help_submit(p.calc,dict(remote_workdir=str(p.work)),native_backend._HelpBudget())
    plan=json.loads(p.f['raw_plan']);s=inputs(plan,attempt=7);raw=b' \n'+json.dumps(s).encode()+b'\n'
    path=path_for(s);target=p.work/path;target.parent.mkdir(parents=True);target.write_bytes(raw)
    transport=p.transport()
    result=asyncio.run(retrieve_calculation(p.calc,transport,str(tmp_path/'temporary')))
    assert set(result.base.repository.list_object_names())==set(controls)|{'result'}
    assert result.base.repository.get_object_content(path,mode='rb')==raw
    result.base.links.add_incoming(p.calc,LinkType.CREATE,'retrieved');result.store()
    expected={**p.raw,'native-close.json':p.expected()}
    checked=native_actor._retrieved_packet(p.calc.uuid,expected,native_actor._Reads(native_actor.MAX_METADATA),
                                          technical_control=p.technical())
    assert checked==expected
    assert not transport.is_open and len(p.calls)==1


@pytest.mark.integration
def test_transport_suffix_and_forged_tuple_do_not_grant_source(packet,tmp_path):
    p=packet;destination=tmp_path/'out';destination.mkdir()
    with p.transport() as transport:
        with pytest.raises((ValueError,PauseInterruption)):
            transport.get(str(p.work/'result/executions'),str(destination/'result/executions'))
    assert not (destination/'result').exists()


@pytest.mark.integration
def test_help_aggregate_refusal_preserves_original_scientific_packet(packet,tmp_path,monkeypatch):
    import asyncio
    from aiida.engine.daemon.execmanager import retrieve_calculation
    p=packet;p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    native_backend._admit_help_submit(p.calc,dict(remote_workdir=str(p.work)),native_backend._HelpBudget())
    snapshot=inputs(json.loads(p.f['raw_plan']),attempt=7);path=path_for(snapshot)
    target=p.work/path;target.parent.mkdir(parents=True);target.write_bytes(json.dumps(snapshot).encode())
    monkeypatch.setattr(model_help,'MAX_TOTAL',1)
    result=asyncio.run(retrieve_calculation(p.calc,p.transport(),str(tmp_path/'temporary')))
    assert result.base.repository.get_object_content('series_result.json',mode='rb')==p.raw['series_result.json']
    assert not result.base.repository.get_object('result').objects
    result.base.links.add_incoming(p.calc,LinkType.CREATE,'retrieved');result.store()
    monkeypatch.setattr(model_help,'document',lambda *a,**k:pytest.fail('empty optional parent parsed an extra plan'))
    expected={**p.raw,'native-close.json':p.expected()}
    assert native_actor._retrieved_packet(p.calc.uuid,expected,native_actor._Reads(native_actor.MAX_METADATA),
        technical_control=p.technical())==expected


def authored_assets():
    """Literal manufactured Root-style metadata; no public source/review claim."""
    from qcl_negf_contracts.messages import encode
    graph=dict(format='qcl-negf.git-graph.v1',revision='a'*40,components=[])
    ref=dict(component='root',revision='a'*40,path='synthetic.json',
             selector=dict(kind='file',value=''),sha256='b'*64)
    pointer=dict(ref,selector=dict(kind='json_pointer',value='/chunk'))
    facts=[dict(name=name,status='unknown',text=None,sources=[]) for name in (
        'signature','units','default','constraint','effects','errors','formula_origin',
        'value_origin','assigning_origin','effective_origin')]
    fragment=dict(format='qcl-negf.model-help-fragment.v1',id='runner-inputs',source_revision='a'*40,
        title='Synthetic capture',summary='Synthetic mapping only.',scope='baseline',meaning_source=ref,
        full=dict(section='runner',path='runner.html',anchor=None),related=[],equations=[],api=[],
        inputs=[dict(path='production.energy_chunk',schema_source=pointer,facts=facts,reference_value_origin=None)],
        assumptions=[],limitations=['Synthetic, no full serving payload or review proof.'])
    fragment_raw=encode(fragment);path='help/fragments/runner-inputs.json';sha=model_help.record(fragment_raw)['sha256']
    generator=dict(name='Documenter',version='1.17.0',uuid='e30172f5-a6a5-5a46-863b-614d45cd2de4',
        git_tree_sha1='56e9c37b5e7c3b4f080ab1da18d72d5c290e184a',julia_version='1.13.0',
        project_sha256='c'*64,manifest_sha256='d'*64,recipes=[ref],toolchain_lock=ref)
    bundle=dict(format='qcl-negf.docs-bundle.v1',sources=graph,schema=ref,generator=generator,
        fragments=[dict(id='runner-inputs',file=path,sha256=sha)],
        files=[dict(path=path,media_type='application/json',bytes=len(fragment_raw),sha256=sha)],aliases=[])
    return graph,{'release.json':encode(dict(format='qcl-negf.release.v2',sources=graph,release_id='recorded-A')),
                   'bundle.json':encode(bundle),path:fragment_raw}


@pytest.mark.integration
def test_genuine_immutable_assets_public_view_keeps_used_and_unknown_reference(aiida_profile_clean,aiida_computer_local):
    from aiida_qcl_negf import service
    from qcl_negf_contracts.messages import encode
    from qcl_negf_contracts.model_help import validate_model_help_view
    from test_model_help_input import stationary_plan_bytes
    computer=aiida_computer_local();plan=orm.SinglefileData(file=io.BytesIO(stationary_plan_bytes()),filename='plan.json').store()
    graph,files=authored_assets();assets=orm.FolderData()
    for path,raw in files.items():assets.base.repository.put_object_from_bytes(raw,path)
    assets.store();source_graph=orm.SinglefileData(file=io.BytesIO(encode(graph)),filename='graph.json').store()
    release=orm.Str('recorded-A').store();run=orm.WorkChainNode(process_type='aiida.workflows:qcl_negf.plan')
    common=dict(plan=plan,source_graph=source_graph,model_help_assets=assets,release_id=release)
    for name,node in common.items():run.base.links.add_incoming(node,LinkType.INPUT_WORK,name)
    run.store();child=orm.CalcJobNode(computer=computer,process_type='aiida.calculations:qcl_negf.execution')
    child.base.links.add_incoming(run,LinkType.CALL_CALC,'capture')
    for name,node in {**common,'attempt':orm.Int(2).store(),'execution_id':orm.Str('execution-1').store()}.items():
        child.base.links.add_incoming(node,LinkType.INPUT_CALC,name)
    child.store();frozen=json.loads(stationary_plan_bytes());s=inputs(frozen);snapshot_raw=encode(s);path=path_for(s)
    retrieved=orm.FolderData();retrieved.base.repository.put_object_from_bytes(snapshot_raw,path)
    retrieved.base.links.add_incoming(child,LinkType.CREATE,'retrieved');retrieved.store()
    index=model_help.build_index(model_help.StoredLeafReader(retrieved.base.repository,owner=retrieved),model_help.inventory(retrieved.base.repository,owner=retrieved),plan=frozen,execution_id='execution-1')
    index_node=orm.Dict(dict=index);index_node.base.links.add_incoming(child,LinkType.CREATE,'model_help_index');index_node.store()
    dto=service.get_model_help_input(run.uuid,'execution-1','point-1',attempt=2,calcjob_uuid=child.uuid,fragment_id='runner-inputs')
    retained=dto['retained'];assert retained['fragment']['bytes']==files['help/fragments/runner-inputs.json']
    assert 'release_receipt' not in retained['metadata']
    # Explicit existing detached association for this synthetic validator
    # fixture only. The getter must not manufacture this publication evidence.
    retained=deepcopy(retained)
    retained['metadata']['release_receipt']=model_help.record(encode(dict(
        context=dto['context'],release_id='recorded-A',source_revision=graph['revision'],
        release_manifest_sha256=retained['release_manifest']['sha256'],bundle_sha256=retained['bundle']['sha256'])))
    def origin(reason):return dict(status='unknown',origin=dict(status='unknown',kind='unknown',locators=[],note='No retained reference.'),binding=None,reason=reason)
    view=dict(format='qcl-negf.model-help-view.v1',context=dto['context'],
        binding=dict(status='unavailable',reason='bundle_missing',release_id='recorded-A',
            release_manifest_sha256=retained['release_manifest']['sha256'],source_revision='a'*40,bundle_sha256=retained['bundle']['sha256']),
        fragment=None,input_status=dto['input_status'],inputs=[dict(path='production.energy_chunk',
            declared=dict(status='unknown',atom=None,evidence=None),used=dict(status='known',atom=dict(kind='integer',value=23,unit='1'),
                evidence=dict(container='execution_input',sha256=dto['snapshot']['sha256'],selector='/inputs/0/used/atom')),
            assigning=dict(status='unknown',kind='unknown',source=None,rule=None,evidence=None),effective_origin=dict(status='unknown',evidence=[]),
            value_origin=dict(declared=origin('value_unavailable'),used=origin('origin_not_recorded')))])
    unavailable=deepcopy(view);unavailable['inputs']=[]
    assert validate_model_help_view(unavailable,retained=dto['retained']) is unavailable
    from qcl_negf_contracts.messages import ContractError
    with pytest.raises(ContractError):validate_model_help_view(view,retained=dto['retained'])
    assert validate_model_help_view(view,retained=retained) is view
    assert view['inputs'][0]['used']['atom']['value']==23 and view['binding']['status']=='unavailable'


@pytest.mark.integration
def test_optional_help_preserves_original_one_MiB_submit_budget(native_graph,tmp_path,record_property):
    from test_native_selected_sources import _execution_graph
    from test_native_backend import reply
    from qcl_negf_contracts.messages import encode,decode
    f=_execution_graph(native_graph,'execution',plan_size=native_actor.MAX_PLAN)
    calc=f['calc'];work=tmp_path/'original-job';work.mkdir();calc.set_remote_workdir(str(work))
    requests=[]
    class Protocol(native_backend.NativeLocalTransport):
        def exec_command_wait_bytes(self,command,stdin=None,**kwargs):
            request=decode(stdin);requests.append(request)
            assert request['action']=='submit' and request['calcjob_uuid']==calc.uuid
            return 0,encode(reply(calc.uuid)),b''
    transport=Protocol(machine='localhost',native_computer_uuid=calc.computer.uuid)
    scheduler=native_backend.NativeSlurmScheduler();scheduler.set_transport(transport)
    calc.set_retrieve_list(list(native_actor.CONTROL_FILES))
    assert scheduler.submit_job(str(work),'_aiidasubmit.sh')=='321'
    assert len(requests)==1
    calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    record_property('valid_raw_plan_bytes',len(f['raw_plan']))
    assert scheduler.submit_job(str(work),'_aiidasubmit.sh')=='321'
    assert len(requests)==2


@pytest.mark.integration
@pytest.mark.parametrize('fault',['before','partial','readback','quota','cancel','foreign','close','cancel_close'])
def test_help_destination_owned_cleanup_or_primary_refusal(packet,tmp_path,monkeypatch,fault):
    import asyncio,errno,os
    from aiida.engine.daemon.execmanager import retrieve_calculation
    p=packet;p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    native_backend._admit_help_submit(p.calc,dict(remote_workdir=str(p.work)),native_backend._HelpBudget())
    snapshot=inputs(json.loads(p.f['raw_plan']),attempt=7);path=path_for(snapshot)
    target=p.work/path;target.parent.mkdir(parents=True);target.write_bytes(json.dumps(snapshot).encode())
    original_write=os.write;primary=PauseInterruption('user cancel partial help') if fault in ('cancel','cancel_close') else OSError(errno.EDQUOT if fault=='quota' else errno.ENOSPC,'partial destination full')
    destinations=[];foreign_at_boundary=[]
    def fail_write(fd,raw):
        spelling=os.readlink('/proc/self/fd/'+str(fd))
        if '/result/executions/' in spelling and spelling!=str(target):
            original_write(fd,raw[:17]);destinations.append(Path(spelling))
            if fault=='foreign':
                Path(spelling).unlink();Path(spelling).write_bytes(b'foreign owner replacement')
            raise primary
        return original_write(fd,raw)
    original_read=native_backend._Tree.read
    def fail_read(tree,name,*a,**k):
        if name=='model-help-inputs.json' and tree.root!=str(target.parent):
            destinations.append(Path(tree.root)/name);raise primary
        return original_read(tree,name,*a,**k)
    original_help=native_backend._deliver_help
    def observe(*args,**kwargs):
        try:return original_help(*args,**kwargs)
        except PauseInterruption:
            if fault=='foreign':foreign_at_boundary.append(destinations[0].read_bytes())
            raise
    monkeypatch.setattr(native_backend,'_deliver_help',observe)
    if fault=='before':monkeypatch.setattr(native_backend,'_deliver_help',lambda *a,**k:(_ for _ in ()).throw(primary))
    elif fault=='readback':monkeypatch.setattr(native_backend._Tree,'read',fail_read)
    elif fault!='close':monkeypatch.setattr(os,'write',fail_write)
    if fault in ('close','cancel_close'):
        original_close=os.close
        def close(fd):
            spelling=os.readlink('/proc/self/fd/'+str(fd))
            original_close(fd)
            if spelling.endswith('/model-help-inputs.json') and spelling!=str(target):
                destinations.append(Path(spelling))
                if fault=='close':raise primary
                raise OSError(errno.EIO,'secondary uncertain destination close')
        monkeypatch.setattr(os,'close',close)
    transport=p.transport()
    if fault=='close':
        with pytest.raises(OSError) as caught:asyncio.run(retrieve_calculation(p.calc,transport,str(tmp_path/'temporary')))
        assert caught.value is primary and primary.__notes__
    elif fault in ('cancel','cancel_close','foreign'):
        with pytest.raises(PauseInterruption) as caught:asyncio.run(retrieve_calculation(p.calc,transport,str(tmp_path/'temporary')))
        if fault in ('cancel','cancel_close'):
            assert caught.value is primary
            if fault=='cancel_close':assert primary.__notes__
        else:assert caught.value.__cause__ is primary and foreign_at_boundary==[b'foreign owner replacement']
    else:
        result=asyncio.run(retrieve_calculation(p.calc,transport,str(tmp_path/'temporary')))
        assert result.base.repository.get_object_content('series_result.json',mode='rb')==p.raw['series_result.json']
        assert not result.base.repository.get_object('result').objects
        result.base.links.add_incoming(p.calc,LinkType.CREATE,'retrieved');result.store()
        assert native_actor._retrieved_packet(p.calc.uuid,{**p.raw,'native-close.json':p.expected()},native_actor._Reads(native_actor.MAX_METADATA),technical_control=p.technical())=={**p.raw,'native-close.json':p.expected()}
    assert (fault=='before' or destinations) and not transport.is_open
    if fault!='foreign':assert all(not destination.exists() for destination in destinations)


@pytest.mark.integration
def test_genuine_getter_plan_size_admission_before_repository_open(ordinary_process,monkeypatch):
    from aiida.orm.nodes.repository import NodeRepository
    from aiida_qcl_negf import service
    process,run,plan=ordinary_process
    monkeypatch.setattr(service,'MAX_PLAN_BYTES',65536)
    monkeypatch.setattr(NodeRepository,'open',lambda *a,**k:pytest.fail('plan opened before size admission'))
    with pytest.raises(ValueError,match='Frozen plan size limit'):
        service.get_model_help_input(run.uuid,'execution-1','point-1',attempt=2,
            calcjob_uuid=process.node.uuid,fragment_id='runner-inputs')


@pytest.mark.integration
@pytest.mark.parametrize('packet',['large_plan'],indirect=True)
def test_optional_retrieval_preserves_same_near_budget_mandatory_packet(packet,tmp_path,monkeypatch,record_property):
    """Same valid controls and 16 MiB mandatory cap, differing only HELP."""
    import asyncio
    from aiida.engine.daemon.execmanager import retrieve_calculation
    p=packet;remaining=[];original_close=native_backend.NativeLocalTransport.close
    def close(transport):
        budget=transport._q1_budget
        try:return original_close(transport)
        finally:
            if budget is not None:remaining.append(budget.reads.remaining)
    monkeypatch.setattr(native_backend.NativeLocalTransport,'close',close)
    def retrieve(label):
        transport=p.transport()
        result=asyncio.run(retrieve_calculation(p.calc,transport,str(tmp_path/label)))
        assert not transport.is_open
        for name,raw in {**p.raw,'native-close.json':p.expected()}.items():
            assert result.base.repository.get_object_content(name,mode='rb')==raw
        return result
    # Measure the unchanged mandatory path, then spend its finite remaining
    # credit on JSON whitespace (32 work bytes per added raw control byte).
    # No model/plan value, original limit, owner or proof is changed.
    retrieve('measure');spare=remaining[-1]
    padding=(spare-131072)//32
    assert padding>0
    p.raw['series_result.json']+=b' '*padding;p.write()
    mandatory=retrieve('mandatory')
    assert 0<remaining[-1]<160000
    record_property('mandatory_metadata_cap_bytes',native_actor.MAX_METADATA)
    record_property('same_series_result_raw_bytes',len(p.raw['series_result.json']))
    record_property('mandatory_remaining_after_close',remaining[-1])
    p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    native_backend._admit_help_submit(p.calc,dict(remote_workdir=str(p.work)),native_backend._HelpBudget())
    snapshot=inputs(json.loads(p.f['raw_plan']),attempt=7);path=path_for(snapshot)
    target=p.work/path;target.parent.mkdir(parents=True);raw=json.dumps(snapshot).encode();target.write_bytes(raw)
    optional=retrieve('optional')
    assert optional.base.repository.get_object_content(path,mode='rb')==raw
    assert remaining[-1]==remaining[-2]
    assert set(mandatory.base.repository.list_object_names())==set(native_actor.CONTROL_FILES)
    assert len(p.calls)==3


@pytest.mark.integration
@pytest.mark.parametrize('fault',['mandatory_file','original_job'])
def test_optional_accounting_keeps_actual_mandatory_integrity_hold(packet,tmp_path,fault):
    p=packet;transport=p.transport();primary=None
    with pytest.raises(PauseInterruption) as caught:
        with transport:
            transport.get(str(p.work/'series_result.json'),str(tmp_path/'control'))
            packet=transport._q1_packet;remaining=packet.budget.reads.remaining
            if fault=='mandatory_file':(p.root/'native-selection.json').write_bytes(b'changed original')
            else:p.calc.set_job_id('999')
            try:packet.check(accounting=native_backend._HelpBudget())
            except PauseInterruption as error:
                primary=error
                assert packet.budget.reads.remaining==remaining
                raise
    assert caught.value is primary and not transport.is_open


@pytest.mark.integration
@pytest.mark.parametrize('unsupported',[False,True])
@pytest.mark.parametrize('raw_result,expected',[(None,300),(b'corrupt scientific JSON',301)])
def test_help_provider_denial_preserves_missing_and_invalid_science(ordinary_process,monkeypatch,unsupported,raw_result,expected):
    import importlib.metadata
    process,run,plan=ordinary_process
    folder=orm.FolderData();folder.base.repository.put_object_from_bytes(b'never opened HDF payload','result/scientific.h5')
    if raw_result is not None:folder.base.repository.put_object_from_bytes(raw_result,'result/series_result.json')
    folder.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');folder.store()
    if unsupported:
        version=importlib.metadata.version
        monkeypatch.setattr(importlib.metadata,'version',lambda name:'0' if name=='disk-objectstore' else version(name))
    assert process.parse_retrieved_output().status==expected
    process.update_outputs()
    assert process.node.outputs.inventory.get_dict()['complete'] is not unsupported


@pytest.mark.integration
@pytest.mark.parametrize('packet',['large_plan'],indirect=True)
@pytest.mark.parametrize('help_mode',['none','valid','credit_denied','provider_denied','unsafe_tree'])
def test_original_actor_control_credit_and_optional_refusal(packet,monkeypatch,record_property,help_mode):
    import importlib.metadata
    from aiida_qcl_negf.saved_sources import _NativeLedger
    p=packet;p.raw['series_result.json']+=b' '*(1790000-len(p.raw['series_result.json']))
    expected={**p.raw,'native-close.json':p.expected()};folder=orm.FolderData()
    for path,raw in expected.items():folder.base.repository.put_object_from_bytes(raw,path)
    if help_mode!='none':
        p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
        snapshot=inputs(json.loads(p.f['raw_plan']),attempt=7)
        folder.base.repository.put_object_from_bytes(json.dumps(snapshot).encode(),path_for(snapshot))
        if help_mode=='unsafe_tree':folder.base.repository.put_object_from_bytes(b'array forbidden','result/data.h5')
    folder.base.links.add_incoming(p.calc,LinkType.CREATE,'retrieved');folder.store()
    if help_mode=='credit_denied':monkeypatch.setattr(model_help,'credit',lambda:_NativeLedger(1,1))
    if help_mode=='provider_denied':
        version=importlib.metadata.version
        monkeypatch.setattr(importlib.metadata,'version',lambda name:'0' if name=='disk-objectstore' else version(name))
    ledger=native_actor._Reads(native_actor.MAX_METADATA)
    if help_mode=='unsafe_tree':
        with pytest.raises(ValueError,match='help repository'):native_actor._retrieved_packet(p.calc.uuid,expected,ledger,technical_control=p.technical())
    else:
        assert native_actor._retrieved_packet(p.calc.uuid,expected,ledger,technical_control=p.technical())==expected
        record_property('original_actor_cap',native_actor.MAX_METADATA)
        record_property('mandatory_credit_remaining',ledger.remaining)
        assert ledger.remaining==601163


@pytest.mark.integration
@pytest.mark.parametrize('cut',['first_effect','partial_copy','payload_but_no_close','postcopy','exact'])
def test_finite_help_credit_allows_only_closed_owned_destination(packet,tmp_path,monkeypatch,record_property,cut):
    import asyncio,os
    from aiida.engine.daemon.execmanager import retrieve_calculation
    from aiida_qcl_negf.saved_sources import _NativeLedger
    p=packet;p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    native_backend._admit_help_submit(p.calc,dict(remote_workdir=str(p.work)),native_backend._HelpBudget())
    snapshot=inputs(json.loads(p.f['raw_plan']),attempt=7);path=path_for(snapshot);raw=json.dumps(snapshot).encode()
    source=p.work/path;source.parent.mkdir(parents=True);source.write_bytes(raw)
    credit=model_help.credit;ledgers=[];limit=None;effects=[];facts={};active=None
    def new_credit():
        value=credit() if limit is None else _NativeLedger(limit,32*1024*1024)
        ledgers.append(value);return value
    monkeypatch.setattr(model_help,'credit',new_credit)
    def spent():return sum(row[1] for row in ledgers[-1].reservations)
    reserve=native_backend._Packet.reserve_help_close
    def reserve_close(packet):
        before=spent();reserve(packet)
        if limit is None:facts.update(before_close_reservation=before,shape=packet.help_close_shape_cost,close_metadata=packet.help_budget.close_metadata,close_io=packet.help_budget.close_io,close_files=packet.help_budget.close_files)
    monkeypatch.setattr(native_backend._Packet,'reserve_help_close',reserve_close)
    write=os.write
    def write_destination(fd,data):
        spelling=os.readlink('/proc/self/fd/'+str(fd))
        if '/result/executions/' in spelling and spelling!=str(source):
            effects.append(('write',active))
            if limit is None:facts.setdefault('first_write',spent())
        return write(fd,data)
    monkeypatch.setattr(os,'write',write_destination)
    mkdir=os.mkdir
    def mkdir_destination(name,*a,**kw):
        if kw.get('dir_fd') is not None and name=='executions':effects.append(('mkdir',active))
        return mkdir(name,*a,**kw)
    monkeypatch.setattr(os,'mkdir',mkdir_destination)
    def retrieve(label):
        nonlocal active
        active=label;transport=p.transport()
        result=asyncio.run(retrieve_calculation(p.calc,transport,str(tmp_path/label)))
        assert not transport.is_open
        for name,data in {**p.raw,'native-close.json':p.expected()}.items():
            assert result.base.repository.get_object_content(name,mode='rb')==data
        return result
    healthy=retrieve('healthy');assert healthy.base.repository.get_object_content(path,mode='rb')==raw
    total=spent()
    limit={'first_effect':1,'partial_copy':facts['first_write']+1,
        'payload_but_no_close':facts['before_close_reservation']+facts['shape']+facts['close_metadata']-1,
        'postcopy':total-1,'exact':total}[cut]
    if cut=='payload_but_no_close':assert limit>=total-facts['close_metadata']
    record_property('finite_help_credit',limit);record_property('healthy_work_with_close_reservation',total)
    record_property('prepaid_three_close_checks_metadata',facts['close_metadata'])
    record_property('prepaid_three_close_checks_io',facts['close_io'])
    record_property('prepaid_three_close_checks_file_probes',facts['close_files'])
    result=retrieve('limited')
    if cut=='exact':
        assert result.base.repository.get_object_content(path,mode='rb')==raw
        assert ledgers[-1].metadata==0
    else:
        assert not result.base.repository.get_object('result').objects
        if cut in ('first_effect','payload_but_no_close'):assert not any(label=='limited' for _,label in effects)
        if cut in ('partial_copy','postcopy'):assert ('write','limited') in effects


@pytest.mark.integration
@pytest.mark.parametrize('fault',['source_namespace','source_foreign','held_count','destination_foreign','close','cancel_close'])
def test_forecast_does_not_authorize_changed_objects_or_hide_close_failure(packet,tmp_path,monkeypatch,fault):
    import asyncio,os,errno
    from aiida.engine.daemon.execmanager import retrieve_calculation
    p=packet;p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    native_backend._admit_help_submit(p.calc,dict(remote_workdir=str(p.work)),native_backend._HelpBudget())
    snapshot=inputs(json.loads(p.f['raw_plan']),attempt=7);path=path_for(snapshot)
    source=p.work/path;source.parent.mkdir(parents=True);source.write_bytes(json.dumps(snapshot).encode())
    deliver=native_backend._deliver_help;foreign=[]
    def after_forecast(localpath,*a,**kw):
        result=deliver(localpath,*a,**kw)
        if fault=='source_namespace':(source.parent/'extra-array.h5').write_bytes(b'forbidden')
        elif fault=='source_foreign':source.unlink();source.write_bytes(b'foreign source inode')
        elif fault=='destination_foreign':
            destination=Path(str(localpath).replace('/./','/'))/Path(path).relative_to('result/executions')
            destination.unlink();destination.write_bytes(b'foreign destination inode');foreign.append(destination)
        return result
    monkeypatch.setattr(native_backend,'_deliver_help',after_forecast)
    close_packet=native_backend._Packet.close;primary=PauseInterruption('user pause at close')
    def close(packet):
        if fault=='held_count' and packet.help_namespaces:
            tree=next(t for t in packet.trees if t.budget is packet.help_budget and t.held)
            tree.held['unexpected-held-fd']=os.dup(next(iter(tree.held.values())))
        if fault=='cancel_close':
            packet.budget.reads.before=lambda:(_ for _ in ()).throw(primary)
        return close_packet(packet)
    monkeypatch.setattr(native_backend._Packet,'close',close)
    close_fd=os.close;secondary=OSError(errno.EIO,'held destination directory close failed')
    def closing(fd):
        spelling=os.readlink('/proc/self/fd/'+str(fd));close_fd(fd)
        if fault=='close' and spelling.endswith('/result/executions') and not spelling.startswith(str(p.work)):raise secondary
        if fault=='cancel_close' and spelling==str(source):raise secondary
    monkeypatch.setattr(os,'close',closing)
    transport=p.transport()
    with pytest.raises((PauseInterruption,OSError)) as caught:asyncio.run(retrieve_calculation(p.calc,transport,str(tmp_path/'temporary')))
    assert not transport.is_open
    if fault=='close':assert caught.value is secondary
    if fault=='cancel_close':assert caught.value is primary and primary.__notes__


@pytest.mark.integration
@pytest.mark.parametrize('unsupported',[False,True])
@pytest.mark.parametrize('namespace',['unrelated_empty_dirs','foreign_help'])
def test_unrelated_help_namespace_never_changes_missing_science(ordinary_process,tmp_path,monkeypatch,unsupported,namespace):
    import importlib.metadata
    process,run,plan=ordinary_process;folder=orm.FolderData()
    if namespace=='unrelated_empty_dirs':
        empty=tmp_path/'empty';empty.mkdir()
        for n in range(model_help.MAX_ENTRIES+1):folder.base.repository.put_object_from_tree(str(empty),path='unrelated/empty-'+str(n))
    else:folder.base.repository.put_object_from_bytes(b'foreign invalid optional bytes','result/executions/FOREIGN/point/attempt-7/data.h5')
    folder.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');folder.store()
    if unsupported:
        version=importlib.metadata.version
        monkeypatch.setattr(importlib.metadata,'version',lambda name:'0' if name=='disk-objectstore' else version(name))
    assert process.parse_retrieved_output().status==300


@pytest.mark.integration
@pytest.mark.parametrize('unsupported',[False,True])
@pytest.mark.parametrize('fault',['invalid_pause','recovery_file_bound'])
def test_scientific_fixed_namespace_keeps_recovery_and_file_refusals(ordinary_process,tmp_path,monkeypatch,unsupported,fault):
    import importlib.metadata
    process,run,plan=ordinary_process;folder=orm.FolderData()
    if fault=='invalid_pause':folder.base.repository.put_object_from_bytes(b'{}','result/pause-receipt.json')
    else:
        # The actual10001-file gate was qualified in primary earlier attempts.
        # This explicit smaller legal bound tests the same refusal cheaply.
        monkeypatch.setattr(model_help,'MAX_ENTRIES',16)
        files=tmp_path/'recovery-files';files.mkdir()
        for n in range(model_help.MAX_ENTRIES+1):(files/str(n)).write_bytes(b'x')
        # One public batch updates serialized metadata once; identical genuine
        # file tree, without quadratic per-put metadata serialization.
        folder.base.repository.put_object_from_tree(str(files),path='result/recovery/generation')
    folder.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');folder.store()
    if unsupported:
        version=importlib.metadata.version
        monkeypatch.setattr(importlib.metadata,'version',lambda name:'0' if name=='disk-objectstore' else version(name))
    assert process.parse_retrieved_output().status==(305 if fault=='invalid_pause' else 301)


def test_close_proc_forecast_covers_actual_request_caps(monkeypatch,record_property):
    import os,builtins
    uid=os.geteuid();gid=os.getegid()
    header=f'Name: synthetic\nUid: {uid} {uid} {uid} {uid}\nGid: {gid} {gid} {gid} {gid}\nFill: '
    status=(header+'x'*(65536-len(header)-1)+'\n').encode()
    head='1 0 0:1 / /qualified ro - nfs synthetic:/'
    mounts=(head+'x'*(65536-len(head)-4)+' ro\n').encode()
    table={'/proc/self/status':status,'/proc/self/mountinfo':mounts,
        '/proc/sys/kernel/random/boot_id':b'00000000-0000-4000-8000-000000000001\n'}
    original=builtins.open
    monkeypatch.setattr(builtins,'open',lambda path,*a,**kw:io.BytesIO(table[path]) if path in table else original(path,*a,**kw))
    budget=native_backend._HelpBudget();proc_io=2*65537+130
    budget.prepay_close(3*(8*proc_io+4*512),3*proc_io,12)
    remaining=budget.ledger.metadata;budget.closing=True
    for _ in range(3):native_backend._namespace('/qualified',budget)
    assert budget.close_metadata>=0 and budget.close_io>=0 and budget.close_files==0
    assert budget.ledger.metadata==remaining
    record_property('three_proc_checks_metadata_remaining',budget.close_metadata)
    record_property('three_proc_checks_io_remaining',budget.close_io)


@pytest.mark.integration
def test_scientific_receipt_scope_includes_direct_and_nested_generations(aiida_profile_clean):
    folder=orm.FolderData()
    for path in ('result/recovery/receipt.json','result/recovery/point/generation/receipt.json',
                 'result/executions/foreign/receipt.json','unrelated/receipt.json'):
        folder.base.repository.put_object_from_bytes(b'{}',path)
    folder.store()
    value=model_help.scientific_paths(folder.base.repository,owner=folder)
    assert value['complete'] is True
    assert set(value['paths'])=={'result/recovery/receipt.json','result/recovery/point/generation/receipt.json'}


@pytest.mark.integration
@pytest.mark.parametrize('read_failure',[False,True])
def test_optional_original_read_and_close_preserve_primary(packet,tmp_path,monkeypatch,read_failure):
    import asyncio,errno
    from contextlib import contextmanager
    from aiida.engine.daemon.execmanager import retrieve_calculation
    p=packet;p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    native_backend._admit_help_submit(p.calc,dict(remote_workdir=str(p.work)),native_backend._HelpBudget())
    snapshot=inputs(json.loads(p.f['raw_plan']),attempt=7);source=p.work/path_for(snapshot)
    source.parent.mkdir(parents=True);source.write_bytes(json.dumps(snapshot).encode())
    armed=False;init=native_backend._Packet.__init__
    def initialize(*a,**kw):
        nonlocal armed
        init(*a,**kw);armed=True
    monkeypatch.setattr(native_backend._Packet,'__init__',initialize)
    closing=OSError(errno.EIO,'original plan context close failed')
    primary=OSError(errno.EIO,'original plan read failed') if read_failure else closing
    original=orm.SinglefileData.open
    class ReadFailure:
        def read(self,*a,**kw):raise primary
    def opened(node,*a,**kw):
        context=original(node,*a,**kw)
        if not armed or node.uuid!=p.f['plan'].uuid:return context
        @contextmanager
        def faulty():
            try:
                with context as stream:yield ReadFailure() if read_failure else stream
            finally:raise closing
        return faulty()
    monkeypatch.setattr(orm.SinglefileData,'open',opened)
    transport=p.transport()
    with pytest.raises(OSError) as caught:asyncio.run(retrieve_calculation(p.calc,transport,str(tmp_path/'temporary')))
    assert caught.value is primary and primary.__notes__ and not transport.is_open


# Cold stored leaf/public backend controls (genuine isolated ORM, no solve).
"""Cold genuine stored metadata; forbidden whole-tree/open seams, no solve."""
import importlib.metadata
import io
import json
import pytest
from aiida import orm
from aiida.common.links import LinkType
from aiida.manage import get_manager
from aiida.schedulers.plugins.slurm import SlurmScheduler
from aiida.orm.nodes.repository import NodeRepository
from aiida.repository import Repository
from aiida_qcl_negf import model_help,native_actor,native_backend,saved_sources
from aiida_qcl_negf.calculation import QCLExecutionCalculation
from test_native_actor import native_graph
from test_native_selected_sources import packet
from test_native_parser import absent_raw,access


def forbid_retained_tree(monkeypatch,uuid):
    original=NodeRepository.open
    def cold_open(self,*a,**k):
        if self._node.uuid==uuid:pytest.fail('cold retrieved NodeRepository.open materializes whole metadata')
        return original(self,*a,**k)
    monkeypatch.setattr(NodeRepository,'open',cold_open)
    original_tree=Repository.from_serialized.__func__
    def tree(cls,backend,serialized):
        if 'unrelated' in serialized.get('o',{}):pytest.fail('cold retrieved Repository.from_serialized materializes unrelated metadata')
        return original_tree(cls,backend=backend,serialized=serialized)
    monkeypatch.setattr(Repository,'from_serialized',classmethod(tree))


def deny_provider(monkeypatch):
    original=importlib.metadata.version
    monkeypatch.setattr(importlib.metadata,'version',lambda name:'0' if name=='disk-objectstore' else original(name))


def unrelated(folder,tmp_path):
    root=tmp_path/'unrelated';root.mkdir()
    # The existing10001-dir primary remains; one public batch avoids quadratic
    # per-put serialization and exercises a genuinely stored over-cap tree.
    for n in range(model_help.MAX_ENTRIES+1):(root/str(n)).mkdir()
    folder.base.repository.put_object_from_tree(str(root),path='unrelated')
    folder.base.repository.put_object_from_bytes(b'foreign optional bytes','result/executions/FOREIGN/p/attempt-7/data.h5')


@pytest.mark.integration
@pytest.mark.parametrize('unsupported',[False,True])
@pytest.mark.parametrize('science,expected',[(None,300),(b'corrupt scientific bytes',301),(b'{}',305)])
def test_cold_ordinary_keeps_science_exit_without_retained_tree(ordinary_process,tmp_path,monkeypatch,unsupported,science,expected):
    process,run,plan=ordinary_process;folder=orm.FolderData();unrelated(folder,tmp_path)
    if science is not None:folder.base.repository.put_object_from_bytes(science,'result/pause-receipt.json' if expected==305 else 'result/series_result.json')
    folder.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');folder.store()
    assert orm.load_node(folder.uuid).base.repository._repository_instance is None
    forbid_retained_tree(monkeypatch,folder.uuid)
    if unsupported:deny_provider(monkeypatch)
    assert process.parse_retrieved_output().status==expected


@pytest.fixture
def cold_native(native_graph,tmp_path,monkeypatch):
    f=native_graph;monkeypatch.setattr(orm.Computer,'get_scheduler',lambda self:SlurmScheduler())
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=f['restart'].pk,inputs={
        **{name:f['calc'].inputs[name] for name in ('code','plan','source_graph','native_execution','execution_id','attempt','release_id')},
        'metadata':{'call_link_label':'cold_consumer','options':dict(resources={'num_machines':1,'num_mpiprocs_per_machine':1})}})
    raw=absent_raw();folder=orm.FolderData();folder.base.repository.put_object_from_bytes(raw,'series_result.json');unrelated(folder,tmp_path)
    folder.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');folder.store()
    monkeypatch.setattr(native_backend,'open_native_selected_sources',lambda *a,**k:access(raw))
    try:yield process,raw,folder
    finally:process.close()


@pytest.mark.integration
@pytest.mark.parametrize('unsupported',[False,True])
def test_cold_native_parser_and_repeated_saved_result_never_materialize(cold_native,monkeypatch,unsupported,record_property):
    process,raw,folder=cold_native;forbid_retained_tree(monkeypatch,folder.uuid)
    if unsupported:deny_provider(monkeypatch)
    for _ in range(2):
        ledger=saved_sources._NativeLedger(16*1024*1024,16*1024*1024)
        assert saved_sources._native_result(process.node,ledger)==raw
        assert ledger.reservations[-2:]==[('retrieved result handle/size',8192,0),('original retrieved result/workspace',8*(len(raw)+1),len(raw)+1)]
    assert process.parse_retrieved_output().status==302
    record_property('cold_retained_uuid',folder.uuid)


@pytest.mark.integration
@pytest.mark.parametrize('help_mode',['valid','credit_denied','provider_denied'])
def test_cold_actor_controls_do_not_enter_repository_file_tree(packet,tmp_path,monkeypatch,help_mode,record_property):
    from test_model_help_retention import inputs,path_for
    p=packet;p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    controls={**p.raw,'native-close.json':p.expected()};folder=orm.FolderData()
    for path,raw in controls.items():folder.base.repository.put_object_from_bytes(raw,path)
    snapshot=inputs(json.loads(p.f['raw_plan']),attempt=7);folder.base.repository.put_object_from_bytes(json.dumps(snapshot).encode(),path_for(snapshot))
    folder.base.links.add_incoming(p.calc,LinkType.CREATE,'retrieved');folder.store()
    forbid_retained_tree(monkeypatch,folder.uuid)
    if help_mode=='credit_denied':monkeypatch.setattr(model_help,'credit',lambda:saved_sources._NativeLedger(1,1))
    if help_mode=='provider_denied':deny_provider(monkeypatch)
    budget=native_actor._Reads(native_actor.MAX_METADATA)
    assert native_actor._retrieved_packet(p.calc.uuid,controls,budget,technical_control=p.technical())==controls
    record_property('mandatory_remaining',budget.remaining)


@pytest.mark.integration
@pytest.mark.parametrize('fault',['foreign','unstored','absolute','parent','dot','double','mode','depth','long_path','key_type','key_length','key_nul','mixed','directory'])
def test_public_leaf_denies_invalid_operand_before_backend_open(aiida_profile_clean,monkeypatch,fault):
    from aiida.repository.backend import DiskObjectStoreRepositoryBackend
    folder=orm.FolderData();folder.base.repository.put_object_from_bytes(b'original raw','result/series_result.json');folder.store();folder=orm.load_node(folder.uuid)
    repository=folder.base.repository;owner=folder;path='result/series_result.json';mode='rb';metadata=repository.metadata
    if fault=='foreign':owner=orm.FolderData().store()
    if fault=='unstored':owner=orm.FolderData();repository=owner.base.repository
    if fault=='absolute':path='/result/series_result.json'
    if fault=='parent':path='result/../series_result.json'
    if fault=='dot':path='result/./series_result.json'
    if fault=='double':path='result//series_result.json'
    if fault=='mode':mode='r'
    if fault=='depth':path='/'.join(['d']*33)
    if fault=='long_path':path='x'*2049
    if fault in ('key_type','key_length','key_nul','mixed','directory'):
        leaf=metadata['o']['result']['o']['series_result.json']
        leaf.update({'key_type':{'k':3},'key_length':{'k':'k'*1025},'key_nul':{'k':'bad\0key'},'mixed':{'o':{}},'directory':{}}[fault])
        if fault=='directory':leaf.pop('k')
        original_metadata=NodeRepository.metadata.fget
        monkeypatch.setattr(NodeRepository,'metadata',property(lambda self:metadata if self is repository else original_metadata(self)))
    monkeypatch.setattr(DiskObjectStoreRepositoryBackend,'open',lambda *a,**k:pytest.fail('backend opened before operand rejection'))
    with pytest.raises((ValueError,TypeError,FileNotFoundError)):
        with model_help.StoredLeafReader(repository,owner=owner).open(path,mode) as stream:stream.read(1)


@pytest.mark.integration
@pytest.mark.parametrize('fault',['clean','read','fatal','assert','runtime','cancel'])
def test_public_leaf_preserves_actual_stream_close_and_primary(aiida_profile_clean,monkeypatch,fault):
    from aiida.repository.backend import DiskObjectStoreRepositoryBackend
    from plumpy.process_states import PauseInterruption
    folder=orm.FolderData();folder.base.repository.put_object_from_bytes(b' \nactual stored bytes\n','result/series_result.json');folder.store();folder=orm.load_node(folder.uuid)
    original=DiskObjectStoreRepositoryBackend.open;held=[];closed=[]
    primary={'read':OSError('primary read'),'fatal':MemoryError('primary fatal'),'assert':AssertionError('primary invariant'),'runtime':RuntimeError('primary runtime'),'cancel':PauseInterruption('primary cancel')}.get(fault)
    class Context:
        def __init__(self,provider,key):self.context=original(provider,key)
        def __enter__(self):
            self.stream=self.context.__enter__()
            class FaultStream:
                def __getattr__(inner,name):return getattr(self.stream,name)
                def read(inner,size):raise primary
            returned=self.stream if primary is None else FaultStream()
            held.append(returned);return returned
        def __exit__(self,*args):
            self.context.__exit__(*args);closed.append(True);raise OSError('secondary close')
    monkeypatch.setattr(DiskObjectStoreRepositoryBackend,'open',lambda self,key:Context(self,key))
    deny_provider(monkeypatch)
    with pytest.raises(type(primary) if primary is not None else OSError) as caught:
        with model_help.StoredLeafReader(folder.base.repository,owner=folder).open('result/series_result.json','rb') as stream:
            assert stream is held[-1]
            stream.seek(0,2);assert stream.tell()==len(b' \nactual stored bytes\n');stream.seek(0)
            assert stream.read(4096)==b' \nactual stored bytes\n'
    assert closed==[True] and held[0].closed
    if primary is not None:
        assert caught.value is primary and 'OSError' in str(primary.__notes__)
    else:assert 'secondary close' in str(caught.value)


@pytest.mark.integration
def test_repeated_saved_result_keeps_preopen_handle_and_read_credits(cold_native,monkeypatch):
    from aiida.repository.backend import DiskObjectStoreRepositoryBackend
    process,raw,folder=cold_native;opened=[];original=DiskObjectStoreRepositoryBackend.open
    def observe(self,key):opened.append(key);return original(self,key)
    monkeypatch.setattr(DiskObjectStoreRepositoryBackend,'open',observe)
    with pytest.raises(ValueError):saved_sources._native_result(process.node,saved_sources._NativeLedger(16384+8191,1024*1024))
    assert opened==[]
    ledger=saved_sources._NativeLedger(16384+8192+8*(len(raw)+1),len(raw)+1)
    assert saved_sources._native_result(process.node,ledger)==raw
    assert len(opened)==1 and ledger.metadata==0 and ledger.io==0


@pytest.mark.integration
@pytest.mark.parametrize('tamper',[False,True])
def test_cold_public_leaf_keeps_existing_recovery_hash_and_copy_protocol(aiida_profile_clean,tmp_path,monkeypatch,tamper):
    import hashlib
    from aiida_qcl_negf.recovery import verify_bundle,recovery_data
    identity=dict(execution_id='fixture-execution',point_id='fixture-point',attempt=2,plan_fingerprint='f'*64)
    files={name:b'synthetic closure fixture '+name.encode() for name in ('physics.h5','history.h5','recovery.json','resolved_configuration.json')}
    commit=dict(checkpoint_ready=True,storage_class='recovery',identity=identity,state_id='fixture-state',state_sequence=1,
        artifacts=[dict(path=name,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),dependencies=[]) for name,raw in files.items()])
    raw_commit=json.dumps(commit).encode();receipt=dict(schema='qcl-negf-recovery-receipt-v1',status='verified',publication_scope='local_filesystem',
        identity=identity,commit_sha256=hashlib.sha256(raw_commit).hexdigest(),state_id='fixture-state',state_sequence=1)
    files.update({'commit.json':raw_commit,'receipt.json':json.dumps(receipt).encode()})
    original=dict(files)
    if tamper:files['physics.h5']=b'tampered actual retained bytes'
    folder=orm.FolderData()
    for name,raw in files.items():folder.base.repository.put_object_from_bytes(raw,'result/recovery/generation/'+name)
    unrelated(folder,tmp_path);folder.store();folder=orm.load_node(folder.uuid)
    forbid_retained_tree(monkeypatch,folder.uuid);deny_provider(monkeypatch)
    reader=model_help.StoredLeafReader(folder.base.repository,owner=folder)
    if tamper:
        with pytest.raises(ValueError,match='byte count|digest'):verify_bundle(reader,'result/recovery/generation',execution_id=identity['execution_id'],attempt=2,plan_fingerprint=identity['plan_fingerprint'])
    else:
        selected=verify_bundle(reader,'result/recovery/generation',execution_id=identity['execution_id'],attempt=2,plan_fingerprint=identity['plan_fingerprint'])
        result=recovery_data(reader,selected)
        assert {name:result.base.repository.get_object_content(name,mode='rb') for name in original}==original


@pytest.mark.integration
@pytest.mark.parametrize('failure',['tamper','read','fatal','assert','runtime','cancel','close'])
def test_cold_actor_control_tamper_and_actual_read_close_failures_remain_primary(packet,monkeypatch,failure):
    from aiida.repository.backend import DiskObjectStoreRepositoryBackend
    from plumpy.process_states import PauseInterruption
    p=packet;p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    expected={**p.raw,'native-close.json':p.expected()};folder=orm.FolderData();target=next(iter(expected))
    for path,raw in expected.items():folder.base.repository.put_object_from_bytes(raw+(b'tampered' if path==target and failure=='tamper' else b''),path)
    folder.base.links.add_incoming(p.calc,LinkType.CREATE,'retrieved');folder.store();folder=orm.load_node(folder.uuid)
    key=folder.base.repository.metadata['o'][target]['k'];forbid_retained_tree(monkeypatch,folder.uuid)
    primary={'read':OSError('actual control read'),'fatal':MemoryError('actual control fatal'),'assert':AssertionError('actual control invariant'),
        'runtime':RuntimeError('actual control runtime'),'cancel':PauseInterruption('actual control cancel'),'close':OSError('actual control close')}.get(failure)
    original=DiskObjectStoreRepositoryBackend.open;closed=[]
    class Context:
        def __init__(self,provider,key):self.context=original(provider,key)
        def __enter__(self):
            self.stream=self.context.__enter__()
            class Stream:
                def read(inner,count):raise primary
            return self.stream if failure=='close' else Stream()
        def __exit__(self,*args):
            self.context.__exit__(*args);closed.append(self.stream.closed)
            if failure=='close':raise primary
            raise OSError('secondary actual control close')
    if failure!='tamper':monkeypatch.setattr(DiskObjectStoreRepositoryBackend,'open',lambda self,k:Context(self,k) if k==key else original(self,k))
    budget=native_actor._Reads(native_actor.MAX_METADATA)
    with pytest.raises(native_actor.NativeActorUnavailable if failure=='tamper' else type(primary)) as caught:
        native_actor._retrieved_packet(p.calc.uuid,expected,budget,technical_control=p.technical())
    if failure=='tamper':assert str(caught.value)=='retrieved_packet_changed'
    else:
        assert caught.value is primary and closed==[True]
        if failure!='close':assert 'OSError' in str(primary.__notes__)


@pytest.mark.integration
@pytest.mark.parametrize('fault',['close','fatal','assert','runtime','cancel'])
def test_cold_native_parser_backend_failure_never_publishes_partial_pair(cold_native,monkeypatch,fault):
    from aiida.repository.backend import DiskObjectStoreRepositoryBackend
    from plumpy.process_states import PauseInterruption
    process,raw,folder=cold_native;key=folder.base.repository.metadata['o']['series_result.json']['k']
    forbid_retained_tree(monkeypatch,folder.uuid);deny_provider(monkeypatch)
    primary={'close':OSError('clean original close'),'fatal':MemoryError('native original read'),
        'assert':AssertionError('native original read'),'runtime':RuntimeError('native original read'),'cancel':PauseInterruption('native original cancel')}[fault]
    original=DiskObjectStoreRepositoryBackend.open;closed=[]
    class Context:
        def __init__(self,provider,key):self.context=original(provider,key)
        def __enter__(self):
            self.stream=self.context.__enter__()
            class Stream:
                def __getattr__(inner,name):return getattr(self.stream,name)
                def read(inner,count):raise primary
            return self.stream if fault=='close' else Stream()
        def __exit__(self,*args):
            self.context.__exit__(*args);closed.append(self.stream.closed)
            raise primary if fault=='close' else OSError('secondary original close')
    monkeypatch.setattr(DiskObjectStoreRepositoryBackend,'open',lambda self,k:Context(self,k) if k==key else original(self,k))
    if fault=='close':assert process.parse_retrieved_output().status==301
    else:
        with pytest.raises(type(primary)) as caught:process.parse_retrieved_output()
        assert caught.value is primary and 'OSError' in str(primary.__notes__)
    assert closed==[True]
    assert not {'result','native_binding','selection'} & set(process.outputs)


@pytest.mark.integration
@pytest.mark.parametrize('unsupported',[False,True])
def test_cold_ordinary_malformed_selected_key_retains_invalid_science_exit(ordinary_process,monkeypatch,unsupported):
    process,run,plan=ordinary_process;folder=orm.FolderData();folder.base.repository.put_object_from_bytes(b'original','result/series_result.json')
    folder.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');folder.store();uuid=folder.uuid
    original_metadata=NodeRepository.metadata.fget
    def malformed(self):
        if self._node.uuid==uuid:return {'o':{'result':{'o':{'series_result.json':{'k':None}}}}}
        return original_metadata(self)
    monkeypatch.setattr(NodeRepository,'metadata',property(malformed));forbid_retained_tree(monkeypatch,uuid)
    if unsupported:deny_provider(monkeypatch)
    assert process.parse_retrieved_output().status==301


# Admitted HELP leaf reads and terminal JSON credit (synthetic isolated ORM).
"""F4 genuine optional reads and terminal credits; synthetic metadata only."""
import io,json,hashlib
from copy import deepcopy
import pytest
from aiida import orm
from aiida.common.links import LinkType
from aiida.manage import get_manager
from aiida_qcl_negf import model_help,service,saved_sources
from aiida_qcl_negf.calculation import QCLExecutionCalculation
from test_model_help_retention import inputs,path_for
from test_model_help_input import stationary_plan_bytes
from test_native_selected_sources import packet
from test_native_actor import native_graph


def add_admitted_dirs(folder,tmp_path,stem):
    root=tmp_path/stem;root.mkdir()
    for i in range(2000):(root/str(i)).mkdir()
    folder.base.repository.put_object_from_tree(str(root),path='unrelated')


@pytest.mark.integration
def test_actual_parser_and_getter_admitted_snapshot_never_construct_tree(ordinary_process,tmp_path,monkeypatch):
    process,run,plan=ordinary_process;snapshot=inputs(plan);raw=json.dumps(snapshot).encode();path=path_for(snapshot)
    folder=orm.FolderData();folder.base.repository.put_object_from_bytes(raw,path)
    folder.base.repository.put_object_from_bytes(stationary_plan_bytes(),'result/scientific_plan.json')
    add_admitted_dirs(folder,tmp_path,'retrieved-dirs');folder.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');folder.store()
    folder=orm.load_node(folder.uuid);assert model_help.inventory(folder.base.repository,owner=folder)['complete'] is True
    forbid_retained_tree(monkeypatch,folder.uuid)
    assert process.parse_retrieved_output().status==300;process.update_outputs()
    index=process.node.outputs.model_help_index.get_dict();assert index['complete'] and index['entries'][0]['status']=='valid'
    dto=service.get_model_help_input(run.uuid,'execution-1','point-1',attempt=2,calcjob_uuid=process.node.uuid,fragment_id='runner-inputs')
    assert dto['snapshot']==model_help.record(raw) and dto['frozen_plan']==model_help.record(stationary_plan_bytes())


@pytest.fixture
def admitted_assets(aiida_profile_clean,aiida_computer_local,tmp_path):
    from qcl_negf_contracts.messages import encode
    computer=aiida_computer_local();plan_raw=stationary_plan_bytes();plan=orm.SinglefileData(file=io.BytesIO(plan_raw),filename='plan.json').store()
    graph,files=authored_assets();asset=orm.FolderData()
    for path,raw in files.items():asset.base.repository.put_object_from_bytes(raw,path)
    add_admitted_dirs(asset,tmp_path,'assets-dirs');asset.store()
    graph_node=orm.SinglefileData(file=io.BytesIO(encode(graph)),filename='graph.json').store();release=orm.Str('recorded-A').store()
    common=dict(plan=plan,source_graph=graph_node,model_help_assets=asset,release_id=release)
    run=orm.WorkChainNode(process_type='aiida.workflows:qcl_negf.plan')
    for name,node in common.items():run.base.links.add_incoming(node,LinkType.INPUT_WORK,name)
    run.store();child=orm.CalcJobNode(computer=computer,process_type='aiida.calculations:qcl_negf.execution')
    child.base.links.add_incoming(run,LinkType.CALL_CALC,'capture')
    for name,node in {**common,'attempt':orm.Int(2).store(),'execution_id':orm.Str('execution-1').store()}.items():child.base.links.add_incoming(node,LinkType.INPUT_CALC,name)
    child.store();frozen=json.loads(plan_raw);snapshot=inputs(frozen);raw=encode(snapshot);path=path_for(snapshot);folder=orm.FolderData()
    folder.base.repository.put_object_from_bytes(raw,path);folder.base.repository.put_object_from_bytes(plan_raw,'result/scientific_plan.json')
    add_admitted_dirs(folder,tmp_path,'snapshot-dirs');folder.base.links.add_incoming(child,LinkType.CREATE,'retrieved');folder.store()
    folder=orm.load_node(folder.uuid);inventory=model_help.inventory(folder.base.repository,owner=folder);assert inventory['complete']
    index=model_help.build_index(model_help.StoredLeafReader(folder.base.repository,owner=folder),inventory,plan=frozen,execution_id='execution-1')
    index_node=orm.Dict(dict=index);index_node.base.links.add_incoming(child,LinkType.CREATE,'model_help_index');index_node.store()
    return run,child,folder,asset,files,raw


@pytest.mark.integration
def test_actual_assets_plan_snapshot_getter_uses_no_whole_tree(admitted_assets,monkeypatch):
    run,child,folder,asset,files,raw=admitted_assets
    forbid_retained_tree(monkeypatch,folder.uuid);forbid_retained_tree(monkeypatch,asset.uuid)
    dto=service.get_model_help_input(run.uuid,'execution-1','point-1',attempt=2,calcjob_uuid=child.uuid,fragment_id='runner-inputs')
    assert dto['snapshot']==model_help.record(raw)
    assert all(dto['retained'][role]['bytes']==files[path] for role,path in [('release_manifest','release.json'),('bundle','bundle.json'),('fragment','help/fragments/runner-inputs.json')])
    assert 'release_receipt' not in dto['retained']['metadata']


def nine_snapshots():
    plan=json.loads(stationary_plan_bytes());original=plan['points'][0];plan['points']=[];raws={}
    for i in range(9):
        point=deepcopy(original);point.update(id='synthetic-point-'+str(i+1),order=i+1,predecessor_id=None);plan['points'].append(point)
        local=deepcopy(plan);local['points']=[point];snapshot=inputs(local,attempt=2);raw=json.dumps(snapshot).encode();raw+=b' '*(60000-len(raw));raws[path_for(snapshot)]=raw
    plan['executions'][0]['point_ids']=[p['id'] for p in plan['points']];plan['computation_count']=9
    from aiida_qcl_negf.validation import validate_plan
    validate_plan(plan)
    for path,raw in raws.items():model_help.c1(json.loads(raw));model_help.checked_snapshot(raw,plan=plan,path=path)
    return plan,raws


@pytest.mark.integration
def test_default_json_credit_is_terminal_before_index_projection(aiida_profile_clean,monkeypatch,record_property):
    plan,raws=nine_snapshots();folder=orm.FolderData()
    for path,raw in raws.items():folder.base.repository.put_object_from_bytes(raw,path)
    folder.store();folder=orm.load_node(folder.uuid);inventory=model_help.inventory(folder.base.repository,owner=folder);assert inventory['complete']
    ledger=model_help.credit();original=model_help.c1;calls=[]
    def c1(value):
        assert not ledger.failed,'dependent C1/index work continued after terminal credit failure'
        calls.append(value['format']);return original(value)
    monkeypatch.setattr(model_help,'c1',c1);forbid_retained_tree(monkeypatch,folder.uuid)
    with pytest.raises(saved_sources.SourceError,match='budget_exceeded'):
        model_help.build_index(model_help.StoredLeafReader(folder.base.repository,owner=folder),inventory,plan=plan,execution_id='execution-1',ledger=ledger)
    assert ledger.failed and len(calls)==8 and set(calls)=={'qcl-negf.model-help-inputs.v1'}
    before=list(ledger.reservations)
    with pytest.raises(saved_sources.SourceError):model_help.build_index(None,{'complete':False,'files':[]},plan=plan,execution_id='execution-1',ledger=ledger)
    assert ledger.reservations==before
    record_property('default_metadata_cap',256*1024*1024);record_property('raw_total',sum(map(len,raws.values())));record_property('validated_before_credit',9)


@pytest.mark.integration
def test_actual_parser_json_refusal_is_incomplete_and_preserves_missing_science(aiida_profile_clean,aiida_computer_local,monkeypatch):
    plan,raws=nine_snapshots();plan_node=orm.SinglefileData(file=io.BytesIO(json.dumps(plan).encode()),filename='plan.json').store()
    run=orm.WorkChainNode(process_type='aiida.workflows:qcl_negf.plan');run.base.links.add_incoming(plan_node,LinkType.INPUT_WORK,'plan');run.store()
    code=orm.InstalledCode(computer=aiida_computer_local(),filepath_executable='/synthetic/never-executed',default_calc_job_plugin='qcl_negf.execution').store()
    process=QCLExecutionCalculation(runner=get_manager().get_runner(),parent_pid=run.pk,inputs=dict(code=code,plan=plan_node,attempt=orm.Int(2),execution_id=orm.Str('execution-1'),metadata=dict(call_link_label='terminal_credit',options=dict(resources={'num_machines':1,'num_mpiprocs_per_machine':1}))))
    try:
        folder=orm.FolderData()
        for path,raw in raws.items():folder.base.repository.put_object_from_bytes(raw,path)
        folder.base.links.add_incoming(process.node,LinkType.CREATE,'retrieved');folder.store()
        assert process.parse_retrieved_output().status==300;process.update_outputs()
        assert process.node.outputs.model_help_index.get_dict()==dict(format='qcl-negf.model-help-index.v1',complete=False,entries=[])
    finally:process.close()


@pytest.mark.integration
@pytest.mark.parametrize('target',['plan','asset','snapshot'])
@pytest.mark.parametrize('failure',['read','close','fatal','assert','runtime','cancel'])
def test_admitted_optional_getter_actual_read_close_fault_is_primary(admitted_assets,monkeypatch,target,failure):
    from aiida.repository.backend import DiskObjectStoreRepositoryBackend
    from plumpy.process_states import PauseInterruption
    run,child,folder,asset,files,raw=admitted_assets
    if target=='plan':owner=folder;path='result/scientific_plan.json'
    elif target=='asset':owner=asset;path='release.json'
    else:owner=folder;path=path_for(inputs(json.loads(stationary_plan_bytes())))
    item=owner.base.repository.metadata
    for part in path.split('/'):item=item['o'][part]
    key=item['k'];original=DiskObjectStoreRepositoryBackend.open;closed=[]
    primary={'read':OSError('original optional read'),'close':OSError('original optional close'),
        'fatal':MemoryError('optional fatal'),'assert':AssertionError('optional invariant'),
        'runtime':RuntimeError('optional runtime'),'cancel':PauseInterruption('optional cancel')}[failure]
    class Context:
        def __init__(self,provider,key):self.context=original(provider,key)
        def __enter__(self):
            self.stream=self.context.__enter__()
            class Stream:
                def __getattr__(inner,name):return getattr(self.stream,name)
                def read(inner,count):raise primary
            return self.stream if failure=='close' else Stream()
        def __exit__(self,*args):
            self.context.__exit__(*args);closed.append(self.stream.closed)
            raise primary if failure=='close' else OSError('secondary optional close')
    monkeypatch.setattr(DiskObjectStoreRepositoryBackend,'open',lambda self,k:Context(self,k) if k==key else original(self,k))
    forbid_retained_tree(monkeypatch,folder.uuid);forbid_retained_tree(monkeypatch,asset.uuid)
    with pytest.raises(type(primary)) as caught:
        service.get_model_help_input(run.uuid,'execution-1','point-1',attempt=2,calcjob_uuid=child.uuid,fragment_id='runner-inputs')
    assert caught.value is primary and closed==[True]
    if failure!='close':assert 'OSError' in str(primary.__notes__)


@pytest.mark.integration
def test_help_actor_admitted_2000_empty_attempts_uses_no_file_tree(packet,tmp_path,monkeypatch,record_property):
    from aiida_qcl_negf import native_actor
    p=packet;p.calc.set_retrieve_list(list(native_actor.CONTROL_FILES)+[model_help.HELP_RETRIEVE])
    plan=json.loads(p.f['raw_plan']);snapshot=inputs(plan,attempt=7);context=snapshot['context'];path=path_for(snapshot)
    attempts=tmp_path/'admitted-attempts';attempts.mkdir()
    for i in range(2000):(attempts/f'attempt-{i+1}').mkdir()
    folder=orm.FolderData();expected={**p.raw,'native-close.json':p.expected()}
    for name,raw in expected.items():folder.base.repository.put_object_from_bytes(raw,name)
    folder.base.repository.put_object_from_tree(str(attempts),path=f"result/executions/{context['execution_id']}/{context['point_id']}")
    folder.base.repository.put_object_from_bytes(json.dumps(snapshot).encode(),path)
    folder.base.links.add_incoming(p.calc,LinkType.CREATE,'retrieved');folder.store();folder=orm.load_node(folder.uuid)
    rows=model_help.strict_repository_tree(folder.base.repository,plan,context['execution_id'],model_help.credit(),owner=folder)
    assert len(rows)==1
    forbid_retained_tree(monkeypatch,folder.uuid);budget=native_actor._Reads(native_actor.MAX_METADATA)
    assert native_actor._retrieved_packet(p.calc.uuid,expected,budget,technical_control=p.technical())==expected
    record_property('admitted_empty_attempts',2000);record_property('mandatory_remaining',budget.remaining)
