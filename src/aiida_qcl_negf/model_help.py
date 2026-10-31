"""Optional bounded help metadata over original AiiDA inputs and retrieval.

The index describes retained bytes, never scientific acceptance. Source/creator
joins remain a separate prerequisite of the selected public projection.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import re

from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.messages import decode, encode

MAX_SNAPSHOT = 64 * 1024
MAX_TOTAL = 8 * 1024 * 1024
MAX_INDEX = 2 * 1024 * 1024
MAX_ENTRIES = 10000
HELP_RETRIEVE = ('result/executions', '.', 2)
PATH = re.compile(r'result/executions/([A-Za-z0-9][A-Za-z0-9._-]{0,127})/'
                  r'([A-Za-z0-9][A-Za-z0-9._-]{0,127})/attempt-([1-9][0-9]{0,15})/'
                  r'model-help-inputs\.json\Z')
LOCAL = ('plan_fingerprint','scientific_fingerprint','model_revision',
         'execution_id','point_id','attempt','configuration_hash')


def read_closed(context, *, maximum, ledger=None, size=None):
    """Finish cleanup before publishing bytes; a secondary close cannot mask cancel."""
    if ledger is not None:
        n=maximum if size is None else size
        ledger.reserve('help original read/buffer copies',metadata=8*(n+1)+8192,io=n+1)
    stream=context.__enter__();primary=None
    try:
        raw=stream.read(maximum+1)
        if len(raw)>maximum:raise ValueError('help metadata size limit')
    except BaseException as error:
        primary=error;raise
    finally:
        try:
            context.__exit__(type(primary),primary,primary.__traceback__) if primary is not None else context.__exit__(None,None,None)
        except BaseException as cleanup:
            if primary is None:raise
            primary.add_note('help repository close failed: '+type(cleanup).__name__)
    return raw


def credit():
    # Existing nonrefundable ledger. This is finite logical work/copy credit,
    # not a bound on Python/AiiDA/PostgreSQL allocations or measured RSS.
    from .saved_sources import _NativeLedger
    return _NativeLedger(256*1024*1024,32*1024*1024)


def document(raw, maximum, ledger=None):
    if ledger is not None:
        # Prepaid before decoding or schema allocation, including adversarial
        # dense containers/strings. This deliberately can HOLD below byte caps.
        ledger.reserve('help JSON/schema workspace',metadata=512*(len(raw)+1)+8192)
    return decode(raw,maximum=maximum)


def c1(value):
    schema_validator('model-help.schema.json').validate(value)
    return value


def record(raw):return dict(bytes=raw,sha256=hashlib.sha256(raw).hexdigest())


# Additional namespace-work caps. ORM hydration is opaque and already happened;
# this bounds our extra traversal/copies, never DB decoding or whole-process RSS.
MAX_NAMESPACE_BYTES = 2 * 1024 * 1024
MAX_DEPTH = 32


class HelpUnavailable(ValueError):
    """Expected optional provider refusal, never an integrity/cancel failure."""



class StoredLeafReader:
    """Binary-only source operand for the existing recovery/control readers.

    Look up only a bounded requested path in already ORM-hydrated public
    metadata. No File tree is constructed, including for refused HELP siblings.
    Backend keys are opaque identifiers, not byte hashes or origin evidence.
    Existing callers retain their byte/credit/hash/manifest checks. ORM and
    public backend internals are opaque; this is not a whole-process RSS bound.
    """
    def __init__(self, repository, *, owner):
        self.repository=repository
        self.owner=owner

    @contextmanager
    def open(self, path, mode='rb'):
        from aiida import orm
        owner=self.owner;repository=self.repository
        if (not isinstance(owner,orm.Node) or not owner.is_stored or
                owner.base.repository is not repository):
            raise ValueError('stored source repository owner unavailable')
        if type(mode) is not str or mode!='rb':
            raise ValueError('stored source binary read required')
        if type(path) is not str or not 1<=len(path)<=2048 or '\\' in path or '\x00' in path:
            raise ValueError('stored source relative path')
        parts=path.split('/')
        if len(parts)>MAX_DEPTH or any(not 1<=len(p)<=256 or p in ('.','..') for p in parts):
            raise ValueError('stored source relative path')
        item=repository.metadata  # Public, already hydrated; never copied/walked.
        for part in parts:
            if type(item) is not dict or len(item)>1 or 'k' in item:
                raise ValueError('stored source directory metadata')
            children=item.get('o',{})
            if type(children) is not dict:raise ValueError('stored source directory metadata')
            if part not in children:raise FileNotFoundError(path)
            item=children[part]
        if type(item) is not dict or len(item)!=1 or 'k' not in item:
            raise ValueError('stored source file metadata')
        key=item['k']
        if type(key) is not str or not 1<=len(key)<=1024 or '\x00' in key:
            raise ValueError('stored source opaque key')
        # get_repository() may return a fresh backend instance. Its public
        # association with this genuine stored owner supplies the repository.
        context=owner.backend.get_repository().open(key)
        stream=context.__enter__();primary=None
        try:
            yield stream
        except BaseException as error:
            primary=error;raise
        finally:
            try:
                context.__exit__(type(primary),primary,primary.__traceback__) if primary is not None else context.__exit__(None,None,None)
            except BaseException as cleanup:
                if primary is None:raise
                primary.add_note('stored source backend close failed: '+type(cleanup).__name__)


def _qualified_owner(repository, owner):
    from importlib.metadata import version,PackageNotFoundError
    from aiida import orm
    try:versions=(version('aiida-core'),version('disk-objectstore'))
    except PackageNotFoundError as error:raise HelpUnavailable('qualified help metadata packages unavailable') from error
    if (not isinstance(owner, orm.Node) or not owner.is_stored or
            owner.base.repository is not repository or
            owner.backend.profile.storage_backend not in ('core.psql_dos','core.sqlite_dos') or
            versions!=('2.9.2','1.5.0')):
        raise HelpUnavailable('qualified stored help metadata owner unavailable')
    return owner


def metadata_census(repository, *, owner=None, ledger=None):
    """Qualified optional census; unknown size providers cannot grant HELP."""
    ledger=credit() if ledger is None else ledger
    _qualified_owner(repository,owner)
    return _namespace_census(repository,ledger=ledger)


def scientific_paths(repository, *, owner):
    """Only existing ordinary result/pause/recovery metadata, never HELP sizes.

    select_recovery examines result/recovery/**/receipt.json, while pause and
    result have fixed leaves. Referenced state/archive files remain verified by
    the existing recovery/result validators, not by this path selector.
    """
    from aiida import orm
    if not isinstance(owner,orm.Node) or not owner.is_stored or owner.base.repository is not repository:
        raise ValueError('scientific repository owner unavailable')
    root=repository.metadata
    if type(root) is not dict or set(root)-{'o'}:raise ValueError('scientific repository root metadata')
    children=root.get('o',{})
    if type(children) is not dict:raise ValueError('scientific repository root metadata')
    result=children.get('result')
    if result is None:return dict(complete=True,paths=[])
    if type(result) is not dict or set(result)-{'o'}:return dict(complete=False,paths=[])
    children=result.get('o',{})
    if type(children) is not dict:return dict(complete=False,paths=[])
    paths=[]
    for name in ('series_result.json','pause-receipt.json'):
        item=children.get(name)
        if item is None:continue
        if type(item) is not dict or set(item)!={'k'}:return dict(complete=False,paths=[])
        paths.append('result/'+name)
    recovery=children.get('recovery')
    if recovery is not None:
        if type(recovery) is not dict or set(recovery)-{'o'}:return dict(complete=False,paths=[])
        census=_namespace_census(repository,strict_keys=False,root=recovery)
        if not census['complete'] or len(census['files'])+len(paths)>MAX_ENTRIES:
            return dict(complete=False,paths=[])
        # Only receipt candidates are needed by select_recovery. State payloads
        # are read later through their original manifest/receipt byte guards.
        paths.extend('result/recovery/'+path for path,_ in census['files'] if path=='receipt.json' or path.endswith('/receipt.json'))
    return dict(complete=True,paths=paths)


def _namespace_census(repository, *, ledger=None, strict_keys=True, root=None):
    """Incremental public serialized names; never a File hierarchy or payload."""
    def charge(operation,metadata):
        if ledger is not None:ledger.reserve(operation,metadata=metadata)
    charge('help namespace census workspace',8192)
    if root is None:root=repository.metadata  # Already hydrated; no additional tree copy.
    files=[];directories=[];count=0;weight=0;complete=True
    def visit(value,path,depth):
        nonlocal count,weight,complete
        if not complete:return
        if depth>MAX_DEPTH or count>=MAX_ENTRIES:
            complete=False;return
        if type(value) is not dict or len(value)>1:raise ValueError('help repository metadata shape')
        if weight+64>MAX_NAMESPACE_BYTES:
            complete=False;return
        count+=1;weight+=64
        charge('help namespace entry work/copies',512+8*(len(path)+1))
        if 'k' in value:
            key=value['k']
            if type(key) is not str or strict_keys and re.fullmatch('[0-9a-f]{64}',key) is None:
                raise ValueError('help repository SHA256 key')
            key_weight=64 if strict_keys else 4*len(key)
            if weight+key_weight>MAX_NAMESPACE_BYTES:
                complete=False;return
            weight+=key_weight;files.append((path,key));return
        children=value.get('o',{})
        if type(children) is not dict:raise ValueError('help repository directory metadata')
        directories.append(path)
        if len(children)>MAX_ENTRIES-count:
            complete=False;return
        for name,child in children.items():
            if (type(name) is not str or not 1<=len(name)<=256 or name in ('.','..') or
                    '/' in name or '\\' in name):raise ValueError('help repository path metadata')
            # Charge a UTF8 upper bound before constructing/encoding new paths.
            forecast=4*len(name)+64
            if weight+forecast>MAX_NAMESPACE_BYTES or len(path)+len(name)+1>2048:
                complete=False;return
            charge('help namespace path copies',8*(forecast+len(path)+1))
            weight+=forecast
            visit(child,path+'/'+name if path else name,depth+1)
            if not complete:return
    visit(root,'',0)
    return dict(complete=complete,files=files,directories=directories)


def _metadata_size(repository,owner,key,ledger):
    """Qualified aiida2.9.2/disk-objectstore1.5.0 metadata-only provider seam.

    The sole private access is the Root-qualified provider._container. Its public
    get_object_meta uses with_streams=False (SQL metadata or loose-object stat).
    Unknown providers never fall back to NodeRepository.get_object_size/as_path.
    """
    from aiida.repository.backend import DiskObjectStoreRepositoryBackend
    from disk_objectstore.exceptions import NotExistent
    _qualified_owner(repository,owner)
    cache=getattr(ledger,'_help_size_cache',None)
    if cache is None:cache={};ledger._help_size_cache=cache
    identity=(owner.uuid,key)
    if identity in cache:return cache[identity]
    if len(cache)>=MAX_ENTRIES:raise ValueError('help size metadata count')
    ledger.reserve('help provider single-object metadata/cache',metadata=32768)
    provider=owner.backend.get_repository()
    if type(provider) is not DiskObjectStoreRepositoryBackend or provider.key_format!='sha256':
        raise HelpUnavailable('qualified help metadata provider unavailable')
    context=provider._container;container=context.__enter__();primary=None
    try:
        try:size=container.get_object_meta(key).size
        except NotExistent as error:raise FileNotFoundError('help repository object missing') from error
        if type(size) is not int or size<0:raise ValueError('help object metadata size')
    except BaseException as error:ledger.failed=True;primary=error;raise
    finally:
        try:context.__exit__(type(primary),primary,primary.__traceback__) if primary is not None else context.__exit__(None,None,None)
        except BaseException as cleanup:
            ledger.failed=True
            if primary is None:raise
            primary.add_note('help metadata provider close failed: '+type(cleanup).__name__)
    cache[identity]=size
    return size


def object_size(owner,path,ledger):
    repository=owner.base.repository
    census=metadata_census(repository,owner=owner,ledger=ledger)
    if not census['complete']:raise ValueError('help repository namespace incomplete')
    for candidate,key in census['files']:
        if candidate==path:return _metadata_size(repository,owner,key,ledger)
    raise FileNotFoundError(path)


def strict_repository_tree(repository,plan,execution_id,ledger,*,owner=None):
    """Admit serialized namespace before any File hierarchy or payload effect."""
    census=metadata_census(repository,owner=owner,ledger=ledger)
    if not census['complete']:raise ValueError('help repository namespace incomplete')
    points={p['id'] for p in plan['points'] if p['execution_id']==execution_id}
    if 'result' not in census['directories']:raise ValueError('help repository result namespace')
    for path in census['directories']:
        parts=path.split('/')
        if parts[0]!='result' or len(parts)==1:continue
        if (parts[1]!='executions' or len(parts)>5 or
                len(parts)>2 and parts[2]!=execution_id or
                len(parts)>3 and parts[3] not in points or
                len(parts)>4 and re.fullmatch('attempt-[1-9][0-9]{0,15}',parts[4]) is None):
            raise ValueError('help repository directory grammar')
    rows=[]
    for path,key in census['files']:
        if path.split('/')[0]!='result':continue
        match=PATH.fullmatch(path)
        if match is None or match[1]!=execution_id or match[2] not in points:
            raise ValueError('help repository file grammar')
        if len(rows)>=min(MAX_ENTRIES,len(points)):raise ValueError('help repository count')
        size=_metadata_size(repository,owner,key,ledger)
        if size>MAX_SNAPSHOT:raise ValueError('help repository file size')
        rows.append(dict(path=path,size=size))
    if sum(row['size'] for row in rows)>MAX_TOTAL:raise ValueError('help repository aggregate')
    return rows


def inventory(repository,*,owner=None):
    """Bounded qualified size metadata; HDF5/payload objects are never opened."""
    ledger=credit();files=[]
    try:
        census=metadata_census(repository,owner=owner,ledger=ledger)
        if not census['complete']:return dict(complete=False,files=[])
        for path,key in census['files']:
            files.append(dict(path=path,size=_metadata_size(repository,owner,key,ledger)))
        return dict(complete=True,files=files)
    except (OSError,ValueError):return dict(complete=False,files=files)

def local_context(plan,execution_id,point_id,attempt):
    points=[p for p in plan['points'] if p['execution_id']==execution_id and p['id']==point_id]
    executions=[e for e in plan['executions'] if e['id']==execution_id]
    if len(points)!=1 or len(executions)!=1:raise ValueError('help frozen point membership')
    point=points[0];passport=point['effective_inputs']
    if (point_id not in executions[0]['point_ids'] or
            passport.get('hash_encoding')!='qcl-negf-canonical-bytes-v1' or
            passport.get('axes_applied') is not True or
            any(type(passport.get(k)) is not str or re.fullmatch('[0-9a-f]{64}',passport[k]) is None
                for k in ('configuration_hash','model_sha256')) or
            any(type(point[k]) is bool or passport.get(k)!=point[k]
                for k in ('temperature_K','voltage_per_period_V'))):
        raise ValueError('help nested frozen passport')
    return dict(plan_fingerprint=plan['fingerprint'],scientific_fingerprint=plan['scientific_fingerprint'],
                model_revision=plan['model_revision'],execution_id=execution_id,point_id=point_id,
                attempt=attempt,configuration_hash=passport['configuration_hash'])


def checked_snapshot(raw, *, plan, path, ledger=None):
    match=PATH.fullmatch(path)
    if match is None:raise ValueError('help path grammar')
    value=document(raw,MAX_SNAPSHOT,ledger)
    if value.get('format')!='qcl-negf.model-help-inputs.v1':raise LookupError('unsupported_version')
    c1(value)
    expected=local_context(plan,match[1],match[2],int(match[3]))
    execution=next(e for e in plan['executions'] if e['id']==match[1])
    if execution.get('operation')!='stationary':raise LookupError('identity_mismatch')
    if value['context']!=expected:raise LookupError('identity_mismatch')
    paths=[row['path'] for row in value['inputs']]
    if len(set(paths))!=len(paths):raise ValueError('duplicate help leaf')
    return value


def build_index(repository, files, *, plan, execution_id, ledger=None):
    # Production passes the existing owner-bound binary leaf operand. Unit
    # readers retain the same .open protocol; this function grants no ORM access.
    from .saved_sources import SourceError
    ledger=credit() if ledger is None else ledger
    ledger.reserve('help index credit state')
    result=dict(format='qcl-negf.model-help-index.v1',complete=files['complete'],entries=[])
    if not files['complete']:return result
    candidates=[row for row in files['files'] if PATH.fullmatch(row['path'])]
    count=min(MAX_ENTRIES,sum(p['execution_id']==execution_id for p in plan['points']))
    # Count, aggregate and index projection admission precede every payload read.
    if (len(candidates)>count or len({r['path'] for r in candidates})!=len(candidates) or
            sum(r['size'] for r in candidates)>MAX_TOTAL or 1024+len(candidates)*1024>MAX_INDEX):
        result['complete']=False;return result
    ledger.reserve('help index rows/encoding/copies',metadata=4096+len(candidates)*4096)
    for row in candidates:
        match=PATH.fullmatch(row['path']);size=row['size']
        entry=dict(path=row['path'],bytes=size,sha256='0'*64,execution_id=match[1],
                   point_id=match[2],attempt=int(match[3]),status='invalid',reason='malformed')
        # Oversize raw remains in retrieved. Do not hash a large scientific or
        # malicious payload merely to make an index entry look complete.
        if size>MAX_SNAPSHOT:
            result['complete']=False;return result
        raw=read_closed(repository.open(row['path'],'rb'),maximum=size,ledger=ledger,size=size)
        if len(raw)!=size:raise ValueError('help repository size changed')
        entry['sha256']=hashlib.sha256(raw).hexdigest()
        try:
            if match[1]!=execution_id:raise LookupError('identity_mismatch')
            checked_snapshot(raw,plan=plan,path=row['path'],ledger=ledger)
            entry.update(status='valid',reason='ok')
        except SourceError:
            # Resource refusal is not evidence that the original bytes are
            # malformed. No schema/encoding/projection work follows a failed
            # ledger; the parser's existing optional handler publishes incomplete.
            raise
        except LookupError as error:
            entry['reason']=str(error) if str(error) in ('identity_mismatch','unsupported_version') else 'identity_mismatch'
        except (ValueError,TypeError,KeyError) :pass
        except Exception as error:
            from jsonschema import ValidationError
            if not isinstance(error,ValidationError):raise
        result['entries'].append(entry)
    c1(result)
    if len(encode(result))>MAX_INDEX:raise ValueError('help index size limit')
    return result


def related(node,kind,label,ledger,*,outgoing=False):
    """Genuine typed link; properties or a caller-supplied UUID are insufficient."""
    from aiida import orm
    ledger.reserve('help typed original link query',metadata=32768)
    query=orm.QueryBuilder().append(orm.Node,filters={'uuid':node.uuid},tag='anchor')
    edge={'type':kind.value}
    if label is not None:edge['label']=label
    query.append(orm.Node,**{'with_incoming' if outgoing else 'with_outgoing':'anchor'},
                 edge_filters=edge,project='*')
    rows=query.limit(2).all(flat=True)
    if len(rows)!=1:raise ValueError('help missing or ambiguous original link')
    return rows[0]


def original_output(child,label,kind,ledger):
    from aiida.common.links import LinkType
    node=related(child,LinkType.CREATE,label,ledger,outgoing=True)
    if not isinstance(node,kind) or not node.is_stored:raise ValueError('help original output type')
    return node


def original_inputs(run,child,ledger):
    from aiida import orm
    from aiida.common.links import LinkType
    if not isinstance(child,orm.CalcJobNode) or not child.is_stored:raise LookupError('help actual CalcJob unavailable')
    parent=related(child,LinkType.CALL_CALC,None,ledger)
    if parent.process_type=='aiida.workflows:qcl_negf.execution_restart':
        parent=related(parent,LinkType.CALL_WORK,None,ledger)
    if parent.uuid!=run.uuid:raise ValueError('help selected child has another owning run')
    child_plan=related(child,LinkType.INPUT_CALC,'plan',ledger)
    run_plan=related(run,LinkType.INPUT_WORK,'plan',ledger)
    if child_plan.uuid!=run_plan.uuid:raise ValueError('help original input plan differs')


def native_attempt(child,point_id,ledger):
    """Use the existing original pair/scalar/creator/source-epoch readback."""
    from .saved_sources import _retained_native_result_source, _NATIVE_METADATA
    ledger.reserve('help native original readback allowance',metadata=8192)
    source=_retained_native_result_source(child,point_id=point_id,maximum_metadata_bytes=_NATIVE_METADATA)
    point=source['point']
    if (point['source']['kind']=='absent' or point['creator'] is None or
            point['creator']['calcjob_uuid']!=child.uuid or point['source_invocation'] is None):
        raise LookupError('help selected point was not captured by this original child')
    return point['point_attempt']


def ordinary_attempt(child,entries,point_id,transport_attempt,ledger):
    # Original retrieved CREATE and exact INPUT/CALL prove the direct ordinary
    # local counter. A different counter requires the existing committed owner.
    attempts={entry['attempt'] for entry in entries if entry['point_id']==point_id}
    if attempts=={transport_attempt}:return transport_attempt
    if len(attempts)!=1:raise LookupError('help ambiguous local point counter')
    attempt=next(iter(attempts))
    from aiida import orm
    from .science_association import source_point_owner
    selection=original_output(child,'selection',orm.Dict,ledger)
    original_output(child,'result',orm.SinglefileData,ledger)
    ledger.reserve('help original selection projection',metadata=8*MAX_INDEX)
    commit=selection.get_dict().get('commits',{}).get(point_id,{})
    identity=commit.get('identity',{})
    if source_point_owner(child,attempt,child.inputs.plan.uuid,point_id=point_id,
            commit_sha256=commit.get('commit_sha256'),identity=identity)!=child.uuid:
        raise LookupError('help local point owner unavailable')
    return attempt


def assets_mapping(run,child,retained,fragment_id,ledger):
    """Minimal retained mapping, never full serving/source-review authority.

    The trusted submitter supplies original FolderData with the existing Root
    filenames. Missing historical inputs are not backfilled from deployment.
    Full bundle/source/review/payload proof remains unavailable until supplied
    through its owning retained association; no reviewed=True flag is invented.
    """
    from aiida import orm
    from aiida.common.links import LinkType
    if 'model_help_assets' not in child.inputs or 'source_graph' not in child.inputs:return
    asset=related(child,LinkType.INPUT_CALC,'model_help_assets',ledger)
    run_asset=related(run,LinkType.INPUT_WORK,'model_help_assets',ledger)
    graph_node=related(child,LinkType.INPUT_CALC,'source_graph',ledger)
    run_graph=related(run,LinkType.INPUT_WORK,'source_graph',ledger)
    if (not isinstance(asset,orm.FolderData) or not asset.is_stored or asset.uuid!=run_asset.uuid or
            not isinstance(graph_node,orm.SinglefileData) or graph_node.uuid!=run_graph.uuid):
        raise ValueError('help immutable asset/graph input identity')
    raw_graph=read_closed(graph_node.open(mode='rb'),maximum=65536,ledger=ledger)
    graph=document(raw_graph,65536,ledger)
    validator=schema_validator('model-help.schema.json')
    validator.evolve(schema={'$defs':validator.schema['$defs'],'$ref':'#/$defs/SourceGraph'}).validate(graph)
    repository=asset.base.repository
    roles=(('release_manifest','release.json',MAX_INDEX),('bundle','bundle.json',MAX_INDEX),
           ('fragment','help/fragments/'+fragment_id+'.json',32768))
    sizes=[]
    for role,path,maximum in roles:
        try:size=object_size(asset,path,ledger)
        except FileNotFoundError:return
        if not 0<=size<=maximum:raise ValueError('help retained asset size')
        sizes.append(size)
    # All three file sizes/copy/parse forecasts are admitted before first read.
    ledger.reserve('help asset full original copies/decoding',metadata=sum(520*(n+1)+8192 for n in sizes),io=0)
    records={};documents={}
    for (role,path,maximum),size in zip(roles,sizes):
        raw=read_closed(StoredLeafReader(repository,owner=asset).open(path,'rb'),maximum=size,ledger=ledger,size=size)
        records[role]=record(raw);documents[role]=document(raw,maximum)
    release,bundle,fragment=(documents[role] for role in ('release_manifest','bundle','fragment'))
    c1(bundle);c1(fragment)
    if (release.get('format')!='qcl-negf.release.v2' or release.get('sources')!=graph or
            bundle['sources']!=graph or fragment['source_revision']!=graph['revision'] or
            fragment['id']!=fragment_id or
            release.get('release_id',retained['selection']['release_id'])!=retained['selection']['release_id']):
        raise ValueError('help original release/source/fragment mismatch')
    expected_path=roles[2][1]
    descriptors=[r for r in bundle['fragments'] if r['id']==fragment_id]
    files=[r for r in bundle['files'] if r['path']==expected_path]
    if (len(descriptors)!=1 or len(files)!=1 or descriptors[0]['file']!=expected_path or
            descriptors[0]['sha256']!=records['fragment']['sha256'] or
            files[0]!=dict(path=expected_path,media_type='application/json',bytes=sizes[2],sha256=records['fragment']['sha256'])):
        raise ValueError('help original mapping inventory mismatch')
    # Retain original bytes only. An execution-specific publication receipt
    # requires the existing owning source/review association, currently absent;
    # manufacturing one here would falsely authorize a mapping.
    retained.update(records)
    retained['selection']['sources']=graph
    retained['output_allowlist']=(expected_path,)


def selected_inputs(run,child,plan,response,fragment_id,*,ledger=None):
    """Detach one original repository; no sibling hunting or deployment fallback."""
    from aiida import orm
    from plumpy.process_states import KillInterruption,PauseInterruption
    ledger=credit() if ledger is None else ledger
    context=response['context']
    retained=dict(selection=dict(context=context,release_id=response['release_id'],fragment_id=fragment_id),
                  metadata=dict(frozen_plan=response['frozen_plan']))
    status=dict(status='unavailable',reason='inventory_unavailable');snapshot=None
    if not isinstance(child,orm.CalcJobNode) or not child.is_stored:
        return dict(response,retained=retained)
    original_inputs(run,child,ledger)
    assets_mapping(run,child,retained,fragment_id,ledger)
    executions=[e for e in plan['executions'] if e['id']==context['execution_id']]
    if len(executions)!=1 or executions[0]['operation']!='stationary':
        return dict(response,input_status=dict(status='unavailable',reason='point_not_executed_by_selected_child'),retained=retained)
    try:
        retrieved=original_output(child,child.link_label_retrieved,orm.FolderData,ledger)
        index_node=original_output(child,'model_help_index',orm.Dict,ledger)
    except ValueError:
        return dict(response,retained=retained)
    ledger.reserve('help original index detached projection/encoding',metadata=8*MAX_INDEX)
    index=index_node.get_dict();c1(index)
    if len(encode(index))>MAX_INDEX:raise ValueError('help original index limit')
    if index['complete'] is not True:
        return dict(response,input_status=dict(status='unavailable',reason='inventory_incomplete'),retained=retained)
    if (len(index['entries'])>min(MAX_ENTRIES,len(plan['points'])) or
            sum(entry['bytes'] for entry in index['entries'])>MAX_TOTAL or
            len({entry['path'] for entry in index['entries']})!=len(index['entries'])):
        raise ValueError('help original index admission')
    for entry in index['entries']:
        match=PATH.fullmatch(entry['path'])
        if (match is None or (match[1],match[2],int(match[3]))!=
                (entry['execution_id'],entry['point_id'],entry['attempt']) or
                entry['execution_id']!=context['execution_id']):
            raise ValueError('help index path/selected execution mismatch')
        local_context(plan,entry['execution_id'],entry['point_id'],entry['attempt'])
    entries=[entry for entry in index['entries'] if entry['execution_id']==context['execution_id'] and
             entry['point_id']==context['point_id']]
    if not entries:
        return dict(response,input_status=dict(status='unavailable',reason='snapshot_missing'),retained=retained)
    try:
        local=(native_attempt(child,context['point_id'],ledger) if 'native_execution' in child.inputs else
               ordinary_attempt(child,entries,context['point_id'],context['attempt'],ledger))
    except (KillInterruption,PauseInterruption):raise
    except (ValueError,LookupError):
        return dict(response,input_status=dict(status='unavailable',reason='point_not_executed_by_selected_child'),retained=retained)
    entries=[entry for entry in entries if entry['attempt']==local]
    if len(entries)!=1:raise ValueError('help exact selected local capture is ambiguous')
    entry=entries[0];match=PATH.fullmatch(entry['path'])
    if (match is None or (match[1],match[2],int(match[3]))!=
            (entry['execution_id'],entry['point_id'],local) or entry['bytes']>MAX_SNAPSHOT):
        raise ValueError('help original entry identity')
    size=object_size(retrieved,entry['path'],ledger)
    if size!=entry['bytes']:raise ValueError('help retained size differs from original index')
    raw=read_closed(StoredLeafReader(retrieved.base.repository,owner=retrieved).open(entry['path'],'rb'),maximum=size,ledger=ledger,size=size)
    if hashlib.sha256(raw).hexdigest()!=entry['sha256']:raise ValueError('help retained digest differs from original index')
    if entry['status']=='invalid':
        status=dict(status='invalid',reason='snapshot_invalid')
    else:
        value=checked_snapshot(raw,plan=plan,path=entry['path'],ledger=ledger)
        snapshot=record(raw);status=dict(status='available',reason='ok')
        retained['metadata']['execution_input']=snapshot
        retained['selection']['capture_context']=value['context']
    # Every file/repository context has successfully closed before the DTO.
    return dict(response,snapshot=snapshot,input_status=status,retained=retained)
