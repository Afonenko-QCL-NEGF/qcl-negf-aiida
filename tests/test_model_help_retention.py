"""Bounded optional metadata; no scientific payload is read by help inventory."""
from contextlib import contextmanager
from copy import deepcopy
from io import BytesIO
import hashlib
import importlib
import json
from pathlib import Path

import pytest
from aiida import orm
from aiida_qcl_negf import model_help
from test_model_help_input import stationary_plan_bytes

class HelpModule:
    def __getattr__(self,name):
        assert importlib.util.find_spec('aiida_qcl_negf.model_help'), 'missing bounded help retention'
        return getattr(importlib.import_module('aiida_qcl_negf.model_help'),name)


help=HelpModule()


def inputs(plan, attempt=2):
    point = plan['points'][0]
    context = {key: plan[key] for key in ('scientific_fingerprint', 'model_revision')}
    context.update(plan_fingerprint=plan['fingerprint'], execution_id=point['execution_id'],
                   point_id=point['id'], attempt=attempt,
                   configuration_hash=point['effective_inputs']['configuration_hash'])
    return dict(format='qcl-negf.model-help-inputs.v1', context=context, phase='solver_input',
                inputs=[dict(path='production.energy_chunk', declared=dict(status='unknown', atom=None),
                    used=dict(status='known', atom=dict(kind='integer', value=23, unit='1')),
                    assigning=dict(status='unknown',kind='unknown',source=None,rule=None),
                    effective_origin=dict(status='unknown',steps=[]))], origin_bindings=[])


def path_for(snapshot):
    c=snapshot['context']
    return f"result/executions/{c['execution_id']}/{c['point_id']}/attempt-{c['attempt']}/model-help-inputs.json"


class Repository:
    def __init__(self, files): self.files, self.reads, self.handles = files, [], []
    def walk(self):
        from pathlib import PurePosixPath
        folders={}
        for path in self.files:
            p=PurePosixPath(path);folders.setdefault(p.parent,[]).append(p.name)
        for root,names in folders.items():yield root,[],names
    def inventory(self):return dict(complete=True,files=[dict(path=p,size=len(raw)) for p,raw in self.files.items()])
    @contextmanager
    def open(self,path,mode='rb'):
        self.reads.append(path)
        with BytesIO(self.files[path]) as stream:
            self.handles.append(stream);yield stream


@pytest.fixture
def frozen():
    from test_model_help_input import stationary_plan_bytes
    return json.loads(stationary_plan_bytes())


def test_operator_has_no_solver_input_capture():
    from test_model_help_input import modern_plan
    plan=modern_plan();snapshot=inputs(plan)
    with pytest.raises(LookupError):help.checked_snapshot(json.dumps(snapshot).encode(),plan=plan,path=path_for(snapshot))


def test_index_same_bytes_and_no_array_read(frozen):
    snapshot=inputs(frozen);raw=b' \n'+json.dumps(snapshot).encode()+b'\n'
    path=path_for(snapshot);repo=Repository({path:raw,'result/a.h5':b'never read'})
    inventory=repo.inventory()
    index=help.build_index(repo,inventory,plan=frozen,execution_id=snapshot['context']['execution_id'])
    assert index == dict(format='qcl-negf.model-help-index.v1',complete=True,entries=[dict(
        path=path,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),
        execution_id=snapshot['context']['execution_id'],point_id=snapshot['context']['point_id'],
        attempt=2,status='valid',reason='ok')])
    assert repo.reads==[path] and all(h.closed for h in repo.handles)


@pytest.mark.parametrize('fault,reason',[('duplicate','malformed'),('phase','malformed'),
    ('hash','identity_mismatch'),('point','identity_mismatch'),('version','unsupported_version')])
def test_invalid_raw_is_indexed_without_mutation(frozen,fault,reason):
    snapshot=inputs(frozen);path=path_for(snapshot)
    if fault=='phase':snapshot['phase']='preview'
    if fault=='hash':snapshot['context']['configuration_hash']='c'*64
    if fault=='point':snapshot['context']['point_id']='other'
    if fault=='version':snapshot['format']='future'
    raw=json.dumps(snapshot).encode()
    if fault=='duplicate':raw=raw.replace(b'"phase": "solver_input"',b'"phase":"solver_input","phase":"solver_input"')
    repo=Repository({path:raw});index=help.build_index(repo,repo.inventory(),plan=frozen,execution_id=frozen['points'][0]['execution_id'])
    assert index['entries'][0]['status']=='invalid' and index['entries'][0]['reason']==reason
    assert repo.files[path]==raw and all(h.closed for h in repo.handles)


def test_aggregate_and_entry_admission_before_reads(frozen,monkeypatch):
    snapshot=inputs(frozen);raw=json.dumps(snapshot).encode();repo=Repository({path_for(snapshot):raw})
    monkeypatch.setattr(importlib.import_module('aiida_qcl_negf.model_help'),'MAX_TOTAL',len(raw)-1)
    index=help.build_index(repo,repo.inventory(),plan=frozen,execution_id=frozen['points'][0]['execution_id'])
    assert index['complete'] is False and not index['entries'] and not repo.reads


def test_cancel_and_secondary_close_keep_primary():
    class Cancel(BaseException):pass
    class Handle:
        def read(self,n):raise Cancel('primary cancellation')
    class Context:
        def __enter__(self):return Handle()
        def __exit__(self,*a):raise OSError('secondary close')
    with pytest.raises(Cancel,match='primary cancellation') as error:
        help.read_closed(Context(),maximum=32)
    assert 'OSError' in str(error.value.__notes__)


def test_close_error_never_returns_bytes():
    class Context:
        def __enter__(self):return BytesIO(b'ok')
        def __exit__(self,*a):raise OSError('close uncertain')
    with pytest.raises(OSError,match='close uncertain'):help.read_closed(Context(),maximum=32)


def test_optional_ports_use_existing_types():
    from aiida import orm
    from aiida_qcl_negf.calculation import QCLExecutionCalculation
    from aiida_qcl_negf.workflow import QCLPlanWorkChain
    assert orm.FolderData in QCLExecutionCalculation.spec().inputs['model_help_assets'].valid_type
    assert QCLExecutionCalculation.spec().outputs['model_help_index'].valid_type is orm.Dict
    assert orm.FolderData in QCLPlanWorkChain.spec().inputs['model_help_assets'].valid_type


@pytest.mark.parametrize('kind',['native','ordinary'])
def test_help_index_precedes_scientific_early_return(frozen,monkeypatch,kind):
    from types import SimpleNamespace as NS
    from aiida_qcl_negf.parser import QCLExecutionParser
    from aiida_qcl_negf import parser
    s=inputs(frozen);path=path_for(s);repo=Repository({path:json.dumps(s).encode()})
    class Ports(dict):
        __getattr__=dict.__getitem__
    calls=[]
    inputs_=Ports(plan=object(),execution_id=NS(value=frozen['points'][0]['execution_id']),attempt=NS(value=2))
    if kind=='native':inputs_['native_execution']=object()
    target=NS(node=NS(inputs=inputs_),retrieved=NS(base=NS(repository=repo)),
        logger=NS(warning=lambda *a:None),out=lambda key,value:calls.append((key,value)),
        _parse_native=lambda **k:'negative-native',_parse_ordinary=lambda **k:'negative-ordinary')
    monkeypatch.setattr(parser,'read_plan',lambda _:frozen)
    monkeypatch.setattr(parser.orm,'Dict',lambda dict:dict)
    monkeypatch.setattr(importlib.import_module('aiida_qcl_negf.model_help'),'inventory',lambda *a,**k:repo.inventory())
    monkeypatch.setattr(importlib.import_module('aiida_qcl_negf.model_help'),'StoredLeafReader',lambda repository,**k:repository)
    assert QCLExecutionParser.parse(target)=='negative-'+kind
    assert [key for key,_ in calls]==['inventory','model_help_index']
    assert calls[1][1]['entries'][0]['status']=='valid'
    assert repo.reads==[path]


@pytest.mark.parametrize('failure',[MemoryError,RuntimeError,AssertionError])
def test_parser_fatal_index_error_keeps_original_primary(monkeypatch,frozen,failure):
    from types import SimpleNamespace as NS
    from aiida_qcl_negf import parser
    class Ports(dict):__getattr__=dict.__getitem__
    target=NS(node=NS(inputs=Ports(plan=object(),execution_id=NS(value='execution-1'))),
        retrieved=NS(base=NS(repository=Repository({}))),out=lambda *a:None,
        logger=NS(warning=lambda *a:None),_parse_ordinary=lambda **k:pytest.fail('continued after fatal'))
    primary=failure('original index fatal')
    monkeypatch.setattr(parser,'read_plan',lambda _:frozen)
    monkeypatch.setattr(parser.orm,'Dict',lambda dict:dict)
    monkeypatch.setattr(importlib.import_module('aiida_qcl_negf.model_help'),'build_index',lambda *a,**k:(_ for _ in ()).throw(primary))
    with pytest.raises(failure) as caught:parser.QCLExecutionParser.parse(target)
    assert caught.value is primary


@pytest.mark.integration
def test_genuine_inventory_never_opens_scientific_payload(aiida_profile_clean,monkeypatch,record_property):
    folder=orm.FolderData();folder.base.repository.put_object_from_bytes(b'synthetic-no-science'*4096,'result/scientific.h5');folder.store()
    folder=orm.load_node(folder.uuid)
    repo=folder.base.repository;opened=[];original=repo.open
    def observed(path,*a,**k):opened.append(path);return original(path,*a,**k)
    monkeypatch.setattr(repo,'open',observed)
    from aiida.repository import Repository as AiiDARepository
    monkeypatch.setattr(AiiDARepository,'from_serialized',classmethod(lambda *a,**k:pytest.fail('materialized File hierarchy')))
    result=model_help.inventory(repo,owner=folder)
    record_property('actual_repository_payload_opens',repr(opened))
    assert result['complete'] is True
    assert opened==[], 'inventory size probing opened/copied scientific HDF5 bytes'


@pytest.mark.integration
def test_genuine_snapshot_size_rejection_precedes_payload_copy(aiida_profile_clean,monkeypatch,record_property):
    plan=json.loads(stationary_plan_bytes());snapshot=inputs(plan);path=path_for(snapshot)
    folder=orm.FolderData();folder.base.repository.put_object_from_bytes(b'x'*(model_help.MAX_SNAPSHOT+1),path);folder.store()
    repo=folder.base.repository;opened=[];original=repo.open
    def observed(path,*a,**k):opened.append(path);return original(path,*a,**k)
    monkeypatch.setattr(repo,'open',observed)
    with pytest.raises(ValueError):model_help.strict_repository_tree(repo,plan,'execution-1',model_help.credit(),owner=folder)
    record_property('pre_admission_payload_opens',repr(opened))
    assert opened==[], 'size rejection occurred after full raw temporary copy'


@pytest.mark.integration
@pytest.mark.parametrize('stored',[False,True])
def test_inventory_bounds_empty_directory_metadata_work(aiida_profile_clean,tmp_path,stored):
    """Genuine repository trees can have many empty dirs with zero file reads."""
    folder=orm.FolderData();repo=folder.base.repository
    empty=tmp_path/'empty';empty.mkdir()
    for n in range(model_help.MAX_ENTRIES+1):repo.put_object_from_tree(str(empty),path='empty-'+str(n))
    if stored:
        folder.store();folder=orm.load_node(folder.uuid);repo=folder.base.repository
    original=repo.walk;visited=[]
    def walk(*a,**k):
        for row in original(*a,**k):visited.append(str(row[0]));yield row
    repo.walk=walk
    result=model_help.inventory(repo,owner=folder)
    assert len(visited)<=model_help.MAX_ENTRIES and result['complete'] is False


@pytest.mark.integration
def test_qualified_size_cache_and_unknown_version_or_foreign_owner(aiida_profile_clean,monkeypatch):
    import importlib.metadata
    from disk_objectstore import Container
    folder=orm.FolderData();folder.base.repository.put_object_from_bytes(b'closed','meta.json');folder.store()
    repo=folder.base.repository;ledger=model_help.credit();calls=[];original=Container.get_object_meta
    def measured(self,key):calls.append(key);return original(self,key)
    monkeypatch.setattr(Container,'get_object_meta',measured)
    monkeypatch.setattr(repo,'open',lambda *a,**k:pytest.fail('size metadata opened payload'))
    assert model_help.object_size(folder,'meta.json',ledger)==6
    assert model_help.object_size(folder,'meta.json',ledger)==6
    assert len(calls)==1 and sum(r[0]=='help provider single-object metadata/cache' for r in ledger.reservations)==1
    foreign=orm.FolderData().store()
    with pytest.raises(ValueError):model_help.metadata_census(repo,owner=foreign,ledger=model_help.credit())
    original_version=importlib.metadata.version
    monkeypatch.setattr(importlib.metadata,'version',lambda name:'0' if name=='disk-objectstore' else original_version(name))
    assert model_help.inventory(repo,owner=folder)==dict(complete=False,files=[])


@pytest.mark.integration
@pytest.mark.parametrize('cancel',[False,True])
def test_metadata_provider_secondary_close_keeps_primary(aiida_profile_clean,monkeypatch,cancel):
    from aiida.repository.backend import DiskObjectStoreRepositoryBackend
    from plumpy.process_states import PauseInterruption
    folder=orm.FolderData();folder.base.repository.put_object_from_bytes(b'x','meta.json');folder.store()
    provider=folder.backend.get_repository();ledger=model_help.credit()
    primary=PauseInterruption('cancel metadata') if cancel else MemoryError('fatal metadata')
    class Context:
        def __enter__(self):return self
        def __exit__(self,*a):raise OSError('secondary metadata close')
        def get_object_meta(self,key):raise primary
    monkeypatch.setattr(DiskObjectStoreRepositoryBackend,'key_format',property(lambda self:'sha256'))
    monkeypatch.setattr(provider,'_container',Context())
    monkeypatch.setattr(folder.backend,'get_repository',lambda:provider)
    with pytest.raises(type(primary)) as caught:model_help.object_size(folder,'meta.json',ledger)
    assert caught.value is primary and 'OSError' in str(primary.__notes__) and ledger.failed
