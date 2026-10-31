"""Read typed selected performance, explicitly retiring the old close wire.

No NFS, catalog/latest lookup, scientific acceptance or retrospective mutation.
"""
from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qcl_negf_results.state import StateReader
    from qcl_negf_results.performance_views import TelemetrySelection, PerformanceLimits

from qcl_negf_contracts.artifacts import relative_path

MAX_METADATA = 1024 * 1024
# Whole native operation only; the existing 1MiB wire/file cap stays separate.
NATIVE_METADATA = 640 * 1024 * 1024
SELECTION = ("calcjob_uuid", "execution_id", "attempt", "plan_fingerprint")


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def point_pins(points, commits, selection, source_owners=None):
    """Use parser-verified commit bytes/identities, never the sidecar's claims."""
    if not isinstance(points, list) or len(points) > 256:
        raise ValueError("bounded scientific point inventory required")
    pins, seen = [], set()
    for point in points:
        point_id = point["id"]
        if point_id in seen or point["execution_id"] != selection["execution_id"]:
            raise ValueError("duplicate/foreign scientific point")
        seen.add(point_id)
        reference = (point.get("data") or {}).get("result_commit")
        if reference is None:
            continue
        reference = relative_path(reference)
        native = commits[point_id]
        identity = native["identity"]
        source_attempt = (point.get("data") or {}).get("checkpoint_source_attempt", point["attempt"])
        expected = {"execution_id": selection["execution_id"], "point_id": point_id,
                    "attempt": source_attempt, "plan_fingerprint": selection["plan_fingerprint"]}
        if (type(source_attempt) is not int or source_attempt < 1 or native["path"] != reference
                or any(type(identity.get(k)) is not type(v) or identity[k] != v for k, v in expected.items())):
            raise ValueError("native source attempt/commit contradicts scientific result")
        owner = (source_owners or {}).get(point_id)
        if "calcjob_uuid" in identity and (owner is None or identity["calcjob_uuid"] != owner):
            raise ValueError("native CalcJob identity lacks its exact provenance relation")
        pins.append({"point_id": point_id, "source_attempt": source_attempt,
                     "point_commit_path": "result/" + reference,
                     "point_commit_sha256": native["commit_sha256"]})
    return pins


def verify_sidecar(raw, *, selection, result_raw, points, commits, source_owners=None):
    """Retained callable signature; the removed v1 wire cannot be reverified."""
    raise ValueError("legacy_performance_binding_v1_retired")


def verify_native_performance(reader: StateReader, *, telemetry: TelemetrySelection,
                              limits: PerformanceLimits | None = None) -> dict:
    """Read a public typed view, keeping caller assertions separate from origin.

    Current A has no technical operand. A manually supplied TelemetrySelection
    does not become an actual ORM creator certificate through this function.
    """
    from qcl_negf_results.performance_views import read_performance_summary
    if telemetry is None:
        return {"status":"unavailable","reason":"native_technical_context_unavailable"}
    return read_performance_summary(reader,telemetry=telemetry,limits=limits)


def retained_source(child, *, maximum=None, anchor_key=None):
    """Rebind exact retained output node/hash; no remote transport or host paths."""
    import re
    from . import native_actor
    if anchor_key is not None and (type(anchor_key) is not str or re.fullmatch('[0-9a-f]{64}',anchor_key) is None):
        raise ValueError('T4 exact retained anchor key required')
    orm=native_actor._orm() if hasattr(child, "backend") else None
    if orm is not None and isinstance(child,orm.CalcJobNode):
        from aiida.common.links import LinkType
        credit=_T4Credit(NATIVE_METADATA if maximum is None else maximum)
        credit.reserve('retained native INPUT label query',32768)
        query=orm.QueryBuilder().append(orm.CalcJobNode,filters={'uuid':child.uuid},tag='child')
        query.append(orm.Node,with_outgoing='child',edge_filters={'type':LinkType.INPUT_CALC.value,'label':'native_execution'},project='*')
        rows=query.limit(2).all(flat=True)
        if len(rows)>1:raise ValueError('T4 ambiguous native INPUT label')
        if rows:
            original=_original(child.uuid,credit)
            return _native_response(original,credit,mode="retained",anchor_key=anchor_key,current_snapshot=None)
    maximum=(1<<20) if maximum is None else min(maximum,1<<20)
    try:
        pin_node = child.outputs.selection
    except (AttributeError, KeyError):
        return {"status": "unavailable", "reason": "performance_binding_missing"}
    pin = pin_node.get_dict()
    if "native_binding" in pin:
        return {"status":"unavailable","reason":"native_technical_context_unavailable"}
    reference = pin.get("performance_binding")
    if reference is None:
        if pin.get("performance_status") == "invalid":
            return {"status": "unavailable", "reason": "performance_binding_rejected"}
        return {"status": "unavailable", "reason": "performance_binding_missing"}
    try:
        sidecar = child.outputs.performance_binding
    except (AttributeError, KeyError) as error:
        raise ValueError("pinned performance output is absent") from error
    if (pin_node.creator.uuid != child.uuid or sidecar.creator.uuid != child.uuid
            or reference["sidecar_uuid"] != sidecar.uuid):
        raise ValueError("performance retained-node provenance changed")
    selection = {"calcjob_uuid": child.uuid, "execution_id": child.inputs.execution_id.value,
                 "attempt": child.inputs.attempt.value,
                 "plan_fingerprint": reference["selection"]["plan_fingerprint"]}
    if reference["selection"] != selection or pin["result_sha256"] != reference["runner_result"]["sha256"]:
        raise ValueError("performance selection/result pin contradicts its CalcJob")
    with sidecar.open(mode="rb") as stream:
        raw = stream.read(min(MAX_METADATA, maximum) + 1)
    if (len(raw) > min(MAX_METADATA, maximum) or len(raw) != reference["sidecar_bytes"]
            or sha(raw) != reference["sidecar_sha256"]):
        raise ValueError("retained performance bytes changed")
    # Original UUID/creator/bytes checks above still protect retained data. The
    # legacy payload supplies no current typed contexts, so never claim verified
    # or available metrics from the retired wire.
    return {"status":"unavailable","reason":"legacy_performance_binding_v1_retired",
            "sidecar_uuid":sidecar.uuid,"sidecar_utf8":raw.decode("utf-8"),
            "integrity":{"native_contexts":"not_reverified","current_access":"not_checked"}}


# These are gross, nonrefundable operation reservations, never RSS or delivered
# bytes. NATIVE_METADATA is the Root-approved measured whole-operation cap.
# MAX_METADATA remains the separate legacy/file format bound.
_ORIGINAL_ALLOWANCE = 16 << 20
_ACCESS_METADATA = 16 << 20
_ACCESS_IO = 64 << 20
_READER_METADATA = 16 << 20
_READER_IO = 16 << 20


class _T4Credit:
    """One T4 invocation, including nested owners and final CREATE/readbacks."""
    def __init__(self, maximum):
        if type(maximum) is not int or not 0 < maximum <= NATIVE_METADATA:
            raise ValueError('T4 metadata allowance required')
        self.maximum=maximum;self.spent=4096;self.prepaid_io=0;self.failed=False;self.denial=None
        if self.spent>maximum:raise ValueError('T4 budget_exceeded before ledger header')
        self.costs=[['T4 operation/aggregated ledger header',4096,0,1]]
        self._cost_indices={'T4 operation/aggregated ledger header':0}

    def reserve(self, name, metadata=0, io=0):
        if type(metadata) is not int or type(io) is not int or min(metadata,io)<0:
            raise ValueError('T4 invalid reservation')
        if self.failed:raise ValueError('T4 budget_exceeded after terminal denial')
        overhead=0 if name in self._cost_indices else 512
        self.spent+=metadata+overhead;self.prepaid_io+=io
        if self.spent>self.maximum:
            self.failed=True;self.denial=(name,metadata+overhead,io)
            raise ValueError('T4 budget_exceeded before '+name)
        if name in self._cost_indices:
            row=self.costs[self._cost_indices[name]];row[1]+=metadata;row[2]+=io;row[3]+=1
        else:
            self._cost_indices[name]=len(self.costs)
            self.costs.append([name,metadata+overhead,io,1])

    # Existing AiiDA nofollow _Tree protocol, charged to this SAME T4 credit.
    def before(self,size=0):self.reserve('T4 metadata file bookkeeping',size)
    def file(self):self.reserve('T4 metadata FD operation',512)
    def io(self,size):self.reserve('T4 metadata file delivery',io=size)


def _json_forecast(raw):
    return 64*len(raw)+512*(1+sum(raw.count(c) for c in (b'{',b'[',b',',b':')))


def _utf8(text,credit):
    if type(text) is not str or len(text)>1<<20:raise ValueError('T4 embedded text bound')
    credit.reserve('embedded UTF8 encode',4*len(text)+1024)
    raw=text.encode()
    if len(raw)>1<<20:raise ValueError('T4 embedded UTF8 bound')
    return raw


def _decode(raw,credit,*,cap=1<<20):
    from qcl_negf_contracts.messages import decode
    if type(raw) is not bytes or len(raw)>cap:raise ValueError('T4 raw bound')
    credit.reserve('whole JSON decode/Contracts forest',_json_forecast(raw))
    return decode(raw,maximum=cap)


def _pin(node,raw):
    return dict(node_uuid=node.uuid,bytes=str(len(raw)),sha256=sha(raw))


def _related(child,label,credit,*,outgoing=True):
    from . import native_actor
    from aiida.common.links import LinkType
    credit.reserve('unique typed '+label+' query',32768)
    reads=native_actor._Reads(32768)
    node=native_actor._related(child,LinkType.CREATE if outgoing else LinkType.INPUT_CALC,label,reads,outgoing=outgoing)
    credit.reserve('typed node qualification',4096)
    return node


def _output(child,credit):
    from . import native_actor
    from aiida.common.links import LinkType
    orm=native_actor._orm()
    credit.reserve('bounded optional performance_source CREATE query',32768)
    query=orm.QueryBuilder().append(orm.CalcJobNode,filters={'uuid':child.uuid},tag='owner')
    query.append(orm.Node,with_incoming='owner',edge_filters={'type':LinkType.CREATE.value,'label':'performance_source'},project='*')
    rows=query.limit(2).all(flat=True)
    if len(rows)>1:raise ValueError('T4 ambiguous performance_source CREATE')
    return rows[0] if rows else None


def _node_raw(node,credit,*,cap=1<<20):
    from . import native_actor
    orm=native_actor._orm()
    if not isinstance(node,orm.SinglefileData) or not node.is_stored:
        raise ValueError('T4 actual stored SinglefileData required')
    credit.reserve('whole retained repository open/read/EOF/close',8*(cap+1),cap+1)
    context=node.open(mode='rb');stream=context.__enter__();primary=None
    try:
        raw=stream.read(cap+1)
        if type(raw) is not bytes or len(raw)>cap:raise ValueError('T4 retained raw bound')
        return raw
    except BaseException as error:
        primary=error;raise
    finally:
        try:context.__exit__(type(primary),primary,primary.__traceback__) if primary is not None else context.__exit__(None,None,None)
        except BaseException as error:
            if primary is None:raise
            primary.add_note('T4 repository close failed: '+type(error).__name__)


def _original(identifier,credit):
    from . import native_actor
    from .saved_sources import _native_origin_script
    credit.reserve('actual original4 public actor allowance',_ORIGINAL_ALLOWANCE)
    provider,pins,raw4=native_actor.read_native_performance_inputs(identifier,maximum_metadata_bytes=_ORIGINAL_ALLOWANCE)
    native=_decode(raw4[2],credit,cap=65536)
    plan=_decode(raw4[0],credit);graph=_decode(raw4[1],credit,cap=65536)
    credit.reserve('original CalcJob typed lookup',16384)
    orm=native_actor._orm()
    rows=orm.QueryBuilder().append(orm.CalcJobNode,filters={'uuid':identifier},project='*').limit(2).all(flat=True)
    if len(rows)!=1:raise ValueError('T4 original CalcJob missing')
    child=rows[0]
    _native_origin_script(raw4[3],child,dict(execution_binding=native,selection=provider))
    return dict(provider=provider,pins=pins,raw4=raw4,native=native,plan=plan,graph=graph,child=child)


def _integrity(origin='not_qualified'):
    return dict(origin=origin,native_contexts='not_reverified',current_access='not_checked',scientific_acceptance='not_assessed')


def _refusal(provider,reason,*,stage='origin_qualification',status='unavailable',candidates=()):
    return dict(schema='qcl-negf.performance-source.v2',status=status,stage=stage,reason=reason,
        provider_selection=provider,candidates=list(candidates),integrity=_integrity())


def _reference(raw):
    return dict(path='sources/'+sha(raw)+'.json',bytes=str(len(raw)),sha256=sha(raw),
        schema='qcl-negf.performance-source-index.v1',media_type='application/json')


def _index(raw,original,credit):
    from qcl_negf_contracts.telemetry import validate_performance_source_index
    value=validate_performance_source_index(_decode(raw,credit))
    if value['provider_selection']!=original['provider'] or value['closed'] is not True:
        raise ValueError('T4 original provider/terminal closed index differs')
    fixed=dict(storage=original['native']['storage'],area='native_execution',relative_path='performance')
    if value['technical']['root']!=fixed:raise ValueError('T4 fixed original transaction root differs')
    return value


def _entry_origins(index,original,credit):
    """Whole inventory original proof; selection never hides another entry."""
    from .saved_sources import _native_epoch
    from qcl_negf_contracts.telemetry import validate_performance_anchor
    from . import native_actor
    from aiida import orm
    from aiida.common.links import LinkType
    known={original['provider']['calcjob_uuid']:original};hops=[];current=original
    routes={original['provider']['calcjob_uuid']:()}

    def ancestor(identifier):
        nonlocal current
        if identifier in known:return known[identifier]
        while len(hops)<16:
            binding=_related(current['child'],'parent_binding',credit,outgoing=False)
            if not isinstance(binding,orm.SinglefileData) or not binding.is_stored:
                raise ValueError('T4 parent binding is not an actual stored SinglefileData')
            credit.reserve('parent native_binding CREATE query',32768)
            parent=native_actor._related(binding,LinkType.CREATE,'native_binding',native_actor._Reads(32768))
            if parent.uuid in known:raise ValueError('T4 cyclic parent_binding chain')
            candidate=_original(parent.uuid,credit)
            if (candidate['provider']['run_uuid']!=original['provider']['run_uuid'] or
                    candidate['raw4'][:2]!=original['raw4'][:2]):
                raise ValueError('T4 foreign original parent graph/plan')
            hops.append(dict(child_calcjob_uuid=current['child'].uuid,parent_binding_uuid=binding.uuid))
            known[parent.uuid]=candidate;routes[parent.uuid]=tuple(hops);current=candidate
            if parent.uuid==identifier:return candidate
        raise ValueError('T4 bounded parent origin unavailable')

    qualified=[]
    for entry in index['entries']:
        anchor=validate_performance_anchor(_decode(_utf8(entry['runner_anchor_utf8'],credit),credit))
        science=anchor['scientific'];manifest=_decode(_utf8(science['manifest_utf8'],credit),credit)
        enclosing=dict(calcjob_uuid=original['child'].uuid,native_execution_uuid=original['pins']['native_execution']['node_uuid'])
        phase_calc,phase_native,_=_native_epoch(anchor['invocation'])
        if anchor['origin']!=enclosing or (phase_calc,phase_native)!=(enclosing['calcjob_uuid'],enclosing['native_execution_uuid']):
            raise ValueError('T4 enclosing phase original c1 differs')
        source_id,native_id,epoch=_native_epoch(science['invocation'])
        source=ancestor(source_id)
        expected_origin=dict(calcjob_uuid=source_id,native_execution_uuid=source['pins']['native_execution']['node_uuid'])
        if (native_id!=expected_origin['native_execution_uuid'] or science['source_origin']!=expected_origin or
                manifest['provenance']!=source['graph'] or manifest['invocation']!=science['invocation'] or
                manifest['identity']['plan_sha256']!=sha(source['raw4'][0])):
            raise ValueError('T4 scientific original c1/graph/plan mismatch')
        if science['relation']=='produced':
            if source is not original or anchor['origin']!=expected_origin:raise ValueError('T4 produced origin differs')
        elif source is original:raise ValueError('T4 borrowed source requires actual parent')
        points=[point for point in source['plan']['points'] if point['id']==manifest['identity']['point_id'] and point['execution_id']==source['provider']['execution_id']]
        if (len(points)!=1 or manifest['identity']['execution_id']!=source['provider']['execution_id'] or
                manifest['identity']['model_sha256']!=points[0]['effective_inputs']['model_sha256']):
            raise ValueError('T4 exact original point/model differs')
        canonical=entry['telemetry']['canonical_plan_ref'];transport=entry['telemetry']['source_transport']
        expected_selection={k:source['provider'][k] for k in ('calcjob_uuid','execution_id','attempt')}
        expected_selection['plan_fingerprint']=source['plan']['fingerprint']
        if (transport['selection']!=expected_selection or transport['frozen_plan']!=canonical or
                canonical['bytes']!=str(len(source['raw4'][0])) or canonical['sha256']!=sha(source['raw4'][0]) or
                entry['telemetry']['expected_producer_provenance']!=source['graph']):
            raise ValueError('T4 canonical original transport differs')
        # Every role must name an independently observed original storage. Role
        # area/relative operands remain assertions until the current A mapping.
        for role in ('runner_anchor_root','scientific_root','technical_root','canonical_plan_root'):
            descriptor=entry[role]
            if not any(descriptor['storage']==item['native']['storage'] for item in known.values()):
                raise ValueError('T4 root lacks original storage relation')
        if entry['scientific_root']['storage']!=source['native']['storage'] or entry['canonical_plan_root']['storage']!=source['native']['storage']:
            raise ValueError('T4 scientific/plan storage differs from original source')
        if (entry['runner_anchor_root']!=dict(storage=original['native']['storage'],area='native_execution',relative_path='.') or
                entry['scientific_root']['area']!='native_execution' or entry['scientific_root']['relative_path']!=science['artifact_root'] or
                entry['canonical_plan_root']!=dict(storage=source['native']['storage'],area='native_workdir',relative_path='.') or
                canonical['path']!='plan.json'):
            raise ValueError('T4 exact original role/root operand differs')
        enclosing_selection={k:original['provider'][k] for k in ('calcjob_uuid','execution_id','attempt')}
        enclosing_selection['plan_fingerprint']=original['plan']['fingerprint']
        enclosing_transport=entry['telemetry']['expected_transport']
        if (enclosing_transport['selection']!=enclosing_selection or
                any(enclosing_transport['frozen_plan'][k]!=canonical[k] for k in ('bytes','sha256','schema','media_type'))):
            raise ValueError('T4 enclosing original transport differs')
        qualified.append((entry,anchor,manifest,source,routes[source_id]))
    return qualified,known


def _association(item,credit,views):
    """Existing public B proof, including whole points/attempt_history relation."""
    entry,anchor,manifest,source,_=item;science=anchor['scientific']
    if anchor['scientific']['relation']=='produced' and manifest['checkpoint'] is None and int(manifest['history']['records'])>0:
        return None,None
    from .service import get_native_result_source,_performance_result_scope,_T4_SELECTOR_ALLOWANCE
    from .saved_sources import _NATIVE_METADATA,_NATIVE_IO
    provider=source['provider'];point=manifest['identity']['point_id'];owner=source['child'].uuid
    if owner not in views:
        credit.reserve('public B retained whole operation',_NATIVE_METADATA,_NATIVE_IO)
        credit.reserve('T4 finite public B selector full allowance',_T4_SELECTOR_ALLOWANCE)
        with _performance_result_scope(provider):
            views[owner]=get_native_result_source(**provider,point_id=point)
    view=views[owner]
    if len([row for row in view['binding']['points'] if row['point_id']==point])!=1:
        raise ValueError('T4 public B primary point unsupported')
    credit.reserve('B whole point/history exact relation workspace',32768)
    rows=(*view['binding']['points'],*view['binding'].get('attempt_history',[]))
    matches=[]
    for row in rows:
        selected=row.get('source',row.get('native_source',{}))
        # Binder rows retain exact source StateRef/invocation, independently of
        # their outward primary projection and native attempt counters.
        if row.get('point_id')==point and selected.get('selected_state')==science['selected_state'] and row.get('source_invocation')==science['invocation'] and row.get('point_attempt')==int(manifest['identity']['attempt']):
            matches.append(row)
    if not matches:raise ValueError('T4 B exact StateRef/invocation association unavailable')
    return view['native_result'],view['native_binding']


def _source(raw,index,qualified,original,credit,*,mode,node_pin=None,anchor_key=None,associations=None):
    from qcl_negf_contracts.telemetry import validate_performance_source
    if anchor_key is None:
        if not qualified:return _refusal(original['provider'],'scientific_anchor_pending',stage='anchor_selection',status='staging')
        if len(qualified)==1 and qualified[0][1]['scientific']['relation']=='produced':anchor_key=qualified[0][0]['anchor_key']
        else:
            candidates=[dict(anchor_key=entry['anchor_key'],point_id=manifest['identity']['point_id'],native_attempt=manifest['identity']['attempt'],
                source_epoch=manifest['invocation']['source_epoch'],relation=anchor['scientific']['relation']) for entry,anchor,manifest,_,_ in qualified]
            return _refusal(original['provider'],'anchor_selector_required',stage='anchor_selection',status='staging',candidates=candidates)
    matches=[item for item in qualified if item[0]['anchor_key']==anchor_key]
    if len(matches)!=1:return _refusal(original['provider'],'anchor_not_selected',stage='anchor_selection')
    item=matches[0];entry,anchor,manifest,source,hops=item
    result_pin,binding_pin=associations[entry["anchor_key"]]
    credit.reserve('source response copies/Contracts validation',8*_json_forecast(raw)+8192)
    value=dict(schema='qcl-negf.performance-source.v2',status='available',stage='qualified_source',reason=None,
        provider_selection=original['provider'],index=dict(ref=_reference(raw),raw_utf8=raw.decode(),node=node_pin),anchor_key=anchor_key,entry=entry,
        origin=dict(mode=mode,source_calcjob_uuid=source['child'].uuid,native_execution_uuid=source['pins']['native_execution']['node_uuid'],
            input_nodes=source['pins'],create_nodes=dict(performance_source=node_pin,result=result_pin,native_binding=binding_pin),
            hops=list(hops),native_epoch=manifest['invocation']['source_epoch']),
        retained=dict(performance_source=node_pin,native_result=result_pin,native_binding=binding_pin),
        integrity=_integrity('verified_original_inputs' if mode=='current' else 'verified_original_create'))
    return validate_performance_source(value)


def _read_current(raw,index,qualified,operand,credit):
    """Fixed closed-v2 role preflight, then the independent public Results read.

    The forecast counts original raw syntax, parsed/copy/encode work, reference
    occurrences and Parquet footer/schema scratch. Standalone reader limits are
    retained/object/IO bounds, never an allocation arena or native RSS promise.
    """
    from .native_backend import _Tree,_relative
    from qcl_negf_contracts import schema_validator
    from qcl_negf_contracts.telemetry import validate_telemetry_catalog,TABLE_FIELDS
    from qcl_negf_results.state import StateReader,ResultReadLimits
    from qcl_negf_results.performance_views import TelemetrySelection,PerformanceLimits,read_performance_context
    mappings=operand['root_mappings'];trees={};files={};used=set();primary=None
    common=schema_validator('result-common.schema.json')
    validator=common.evolve(schema={'$ref':'#/$defs/FileRef','$defs':common.schema['$defs']})
    def path(root):
        matches=[path for descriptor,path in mappings if descriptor==root]
        if len(matches)!=1:raise ValueError('T4 complete current root mapping required')
        return matches[0]
    def read(root,ref,*,expected=None):
        credit.reserve('closed role FileRef validation/header',8192)
        validator.validate(ref);size=int(ref['bytes'])
        if not 0<size<=1<<20:raise ValueError('T4 closed role raw bound')
        parts=_relative(ref['path']);key=(str(root),ref['path']);used.add(key)
        if key in files:
            old_ref,raw,_,_=files[key]
            if old_ref!=ref or (expected is not None and raw!=expected):raise ValueError('T4 conflicting closed metadata role')
            return raw
        directory=root.joinpath(*parts[:-1]);name=parts[-1]
        if directory not in trees:trees[directory]=_Tree(str(directory),credit)
        raw=trees[directory].read(name,size,expected,hold=True)
        if str(len(raw))!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('T4 closed role whole reference differs')
        credit.reserve('closed role retained raw/ref witness',4*len(raw)+8192)
        files[key]=(ref,raw,trees[directory],name)
        return raw
    def json_role(root,ref,*,expected=None):
        raw=read(root,ref,expected=expected)
        if ref['media_type']!='application/json':raise ValueError('T4 closed JSON role required')
        return _decode(raw,credit)
    try:
        for entry,anchor,manifest,source,_ in qualified:
            used=set()
            technical=path(entry['technical_root']);scientific=path(entry['scientific_root']);plan_root=path(entry['canonical_plan_root'])
            state=anchor['scientific']['selected_state'];telemetry=entry['telemetry']
            manifest_raw=_utf8(anchor['scientific']['manifest_utf8'],credit)
            json_role(scientific,state['manifest'],expected=manifest_raw)
            json_role(plan_root,telemetry['canonical_plan_ref'],expected=source['raw4'][0])
            binding=json_role(technical,telemetry['binding_ref'])
            # Closed binding embeds exact current commit, summary and source
            # binding bytes; public Results independently validates every join.
            commit=json_role(technical,telemetry['commit_ref'],expected=_utf8(binding['commit']['raw_utf8'],credit))
            if binding['commit']['ref']!=telemetry['commit_ref']:raise ValueError('T4 close/commit reference differs')
            json_role(technical,commit['summary'],expected=_utf8(binding['summary']['raw_utf8'],credit))
            if binding['summary']['ref']!=commit['summary']:raise ValueError('T4 close/summary reference differs')
            json_role(technical,telemetry['source_binding_ref'],expected=_utf8(binding['source_binding']['raw_utf8'],credit))
            if binding['source_binding']['ref']!=telemetry['source_binding_ref']:raise ValueError('T4 close/source reference differs')
            catalog=json_role(technical,commit['catalog'])
            validate_telemetry_catalog(catalog,expected_transport=telemetry['expected_transport'])
            if int(commit['catalog']['bytes'])>65536:raise ValueError('T4 catalog raw ceiling')
            if commit['previous'] is not None:
                previous=json_role(technical,commit['previous']['commit'])
                json_role(technical,commit['previous']['summary'])
                json_role(technical,previous['catalog'])
            for row in commit['contexts']['phase_context_refs']:
                json_role(technical,row['retained_publication_ref']);json_role(technical,row['retained_segment_ref'])
            for row in commit['contexts']['resume_context_refs']:
                json_role(technical,row['commit']);json_role(technical,row['summary'])
            parquet_workspace=0
            for descriptor in catalog['artifacts']:
                ref=descriptor['ref']
                if ref['media_type']=='application/json':json_role(technical,ref)
                elif ref['media_type']=='application/vnd.apache.parquet':
                    payload=read(technical,ref)
                    if len(payload)<12 or payload[-4:]!=b'PAR1':raise ValueError('T4 Parquet footer shape')
                    footer=int.from_bytes(payload[-8:-4],'little')
                    if footer>len(payload)-12:raise ValueError('T4 Parquet footer bound')
                    parquet_workspace+=2*footer+len(TABLE_FIELDS[descriptor['table']])*1024+4096+4*len(payload)
                else:raise ValueError('T4 unsupported closed member media')
            # Fixed SOURCE-qualified read_performance_context2 consumes exactly
            # these roles: one previous delta, committed phase/resume pairs and
            # current direct catalog members. It never follows their old refs.
            role_raws=[files[key][1] for key in used if files[key][0]['media_type']=='application/json']
            syntax=sum(_json_forecast(data) for data in role_raws)
            raw_bytes=sum(len(data) for data in role_raws)
            reference_count=len(used)
            credit.reserve('public closed roles decode/validate/copy/encode forecast',
                8*syntax+32*raw_bytes+16384*(reference_count+1)+parquet_workspace,
                4<<20)
            # Manifest constructor/stability and detached public source copies
            # are based on its exact known raw, never a fresh per-reader cap.
            credit.reserve('public StateReader known manifest/copy forecast',8*_json_forecast(manifest_raw)+16384)
            selected=TelemetrySelection(technical,canonical_plan_root=plan_root,**telemetry)
            reader=StateReader(scientific,state_ref=state,expected_identity=state['identity'],
                limits=ResultReadLimits(maximum_metadata_bytes=_READER_METADATA,maximum_io_bytes=_READER_IO))
            reader_primary=None
            try:
                assertion,contexts=read_performance_context(reader,telemetry=selected,limits=PerformanceLimits(maximum_objects=16384))
                manifests=[data for scope,ref,data in contexts if scope=='scientific' and ref==state['manifest']]
                plans=[data for scope,ref,data in contexts if scope=='canonical_plan' and ref==telemetry['canonical_plan_ref']]
                if manifests!=[manifest_raw] or plans!=[source['raw4'][0]]:raise ValueError('T4 public whole original manifest/plan differs')
                for scope,ref,data in contexts:
                    root=scientific if scope=='scientific' else plan_root if scope=='canonical_plan' else technical
                    if files.get((str(root),ref['path']),())[:2]!=(ref,data):raise ValueError('T4 unforecast public metadata role')
            except BaseException as error:
                reader_primary=error;raise
            finally:
                try:reader.close()
                except BaseException as error:
                    if reader_primary is None:raise
                    reader_primary.add_note('T4 Results reader close failed: '+type(error).__name__)
        # Held original regular FDs + named leaves/ancestry and whole bytes are
        # checked after all public readers close, before final A close/output.
        for ref,data,tree,name in files.values():tree.read(name,int(ref['bytes']),data,hold=True)
    except BaseException as error:
        primary=error;raise
    finally:
        failure=primary
        for tree in reversed(tuple(trees.values())):
            try:tree.close()
            except BaseException as error:
                if failure is None:failure=error
                else:failure.add_note('T4 preflight FD close failed: '+type(error).__name__)
        if primary is None and failure is not None:raise failure


def publish_native_performance(process,*,maximum=None):
    """Optional owning CalcJob publication, only after every current reader closes."""
    import io
    from . import native_actor,native_backend
    credit=_T4Credit(NATIVE_METADATA if maximum is None else maximum)
    original=_original(process.node.uuid,credit)
    credit.reserve('A selected access full allowance',_ACCESS_METADATA,_ACCESS_IO)
    with native_backend.open_native_selected_sources(process.node.uuid,maximum_metadata_bytes=_ACCESS_METADATA,
            maximum_io_bytes=_ACCESS_IO,include_performance=True) as (_,_,operand):
        raw=operand['raw_index']
        if raw is None:return None
        index=_index(raw,original,credit);qualified,known=_entry_origins(index,original,credit)
        _read_current(raw,index,qualified,operand,credit)
        views={}
        for item in qualified:_association(item,credit,views)
    # All public Results and A final close/FD checks precede any output effect.
    for identifier,before in known.items():
        after=_original(identifier,credit)
        if (after['provider'],after['pins'],after['raw4'])!=(before['provider'],before['pins'],before['raw4']):
            raise ValueError('T4 original4 final readback changed')
    credit.reserve('performance_source constructor/CREATE reservation',8*(len(raw)+1)+32768,len(raw)+1)
    orm=native_actor._orm()
    node=_output(process.node,credit)
    if node is None:
        node=orm.SinglefileData(file=io.BytesIO(raw),filename='performance-source.json')
        process.out('performance_source',node);process.update_outputs()
        actual=_related(process.node,'performance_source',credit)
        if actual.uuid!=node.uuid:raise ValueError('T4 actual performance_source CREATE differs')
    if node.filename!='performance-source.json' or _node_raw(node,credit)!=raw:
        raise ValueError('T4 output whole original bytes differ')
    readback=_index(_node_raw(node,credit),original,credit)
    if readback!=index:raise ValueError('T4 output provider index readback differs')
    # Kept only on the owning in-memory process for L cost observations. This is
    # never a serialized certificate, scalar mutation or cross-call allowance.
    process._t4_last_credit=credit
    return _pin(node,raw)


def native_source(run_uuid,execution_id,*,attempt,calcjob_uuid,mode='retained',anchor_key=None,current_snapshot=None,maximum=None):
    from . import native_actor
    import re
    if anchor_key is not None and (type(anchor_key) is not str or re.fullmatch('[0-9a-f]{64}',anchor_key) is None):
        raise ValueError('T4 exact anchor key required')
    if current_snapshot is not None and (type(current_snapshot) is not dict or set(current_snapshot)!={'index_ref','index_utf8','anchor_key'}):
        raise ValueError('T4 exact current snapshot required')
    if current_snapshot is not None:
        key=current_snapshot['anchor_key'];text=current_snapshot['index_utf8']
        if key is not None and (type(key) is not str or re.fullmatch('[0-9a-f]{64}',key) is None):raise ValueError('T4 exact snapshot key required')
        if type(text) is not str or len(text)>1<<20:raise ValueError('T4 exact bounded snapshot text required')
        if type(current_snapshot['index_ref']) is not dict or set(current_snapshot['index_ref'])!={'path','bytes','sha256','schema','media_type'}:
            raise ValueError('T4 exact snapshot FileRef required')
    if mode not in ('current','retained') or (mode=='retained' and current_snapshot is not None):
        raise ValueError('T4 exact current/retained operands required')
    if type(attempt) is not int or not 0<attempt<=9007199254740991:raise ValueError('T4 exact safe attempt required')
    native_actor._uuid(run_uuid);native_actor._uuid(calcjob_uuid)
    if type(execution_id) is not str or not 1<=len(execution_id)<=128:raise ValueError('T4 exact execution required')
    provider=dict(run_uuid=run_uuid,execution_id=execution_id,attempt=attempt,calcjob_uuid=calcjob_uuid)
    try:
        credit=_T4Credit(NATIVE_METADATA if maximum is None else maximum)
        original=_original(calcjob_uuid,credit)
        if original['provider']!=provider:raise ValueError('T4 original selection differs')
        return _native_response(original,credit,mode=mode,anchor_key=anchor_key,current_snapshot=current_snapshot)
    except (ValueError,TypeError,KeyError,AttributeError,OSError,ImportError) as error:
        reason=('budget_exceeded' if 'budget' in str(error) else
            'origin_unavailable' if isinstance(error,(native_actor.NativeActorUnavailable,ImportError)) else 'origin_mismatch')
        return _refusal(provider,reason)


def _native_response(original,credit,*,mode,anchor_key,current_snapshot):
    from . import native_actor
    from qcl_negf_contracts.telemetry import validate_performance_source
    provider=original['provider'];calcjob_uuid=provider['calcjob_uuid']
    if mode=='current' and current_snapshot is None:
        credit.reserve('current launch actor full allowance',_ORIGINAL_ALLOWANCE)
        launch=native_actor.load_native_launch(calcjob_uuid,maximum_metadata_bytes=_ORIGINAL_ALLOWANCE)
        credit.reserve('current locator copy/validation',64*sum(map(len,original['raw4']))+8192)
        value=dict(schema='qcl-negf.performance-source.v2',status='staging',stage='current_locator',reason='current_snapshot_required',
            provider_selection=provider,locator=dict(technical_root=dict(storage=original['native']['storage'],area='native_execution',relative_path='performance'),
                index_pointer=dict(schema='qcl-negf.performance-source-pointer.v1',path='current-source.json'),launch=launch,
                native_execution_utf8=original['raw4'][2].decode(),original_inputs=original['pins']),integrity=_integrity())
        return validate_performance_source(value)
    node_pin=None
    if mode=='retained':
        node=_output(original['child'],credit)
        if node is None:return _refusal(provider,'retained_performance_source_missing')
        if node.filename!='performance-source.json':raise ValueError('T4 fixed retained filename differs')
        raw=_node_raw(node,credit);node_pin=_pin(node,raw)
    else:
        if type(current_snapshot) is not dict or set(current_snapshot)!={'index_ref','index_utf8','anchor_key'}:
            raise ValueError('T4 exact current snapshot required')
        text=current_snapshot['index_utf8']
        if type(text) is not str or len(text)>1<<20:raise ValueError('T4 snapshot UTF8 bound')
        credit.reserve('current snapshot encode',4*len(text)+8192)
        raw=text.encode()
        if anchor_key is None:anchor_key=current_snapshot['anchor_key']
        if current_snapshot['index_ref']!=_reference(raw) or current_snapshot['anchor_key']!=anchor_key:
            raise ValueError('T4 immutable current snapshot differs')
    index=_index(raw,original,credit);qualified,known=_entry_origins(index,original,credit)
    views={}
    associations={item[0]['anchor_key']:_association(item,credit,views) for item in qualified}
    value=_source(raw,index,qualified,original,credit,mode=mode,node_pin=node_pin,anchor_key=anchor_key,associations=associations)
    for identifier,before in known.items():
        after=_original(identifier,credit)
        if (after['provider'],after['pins'],after['raw4'])!=(before['provider'],before['pins'],before['raw4']):
            raise ValueError('T4 final original4 changed')
    if mode=='retained':
        actual=_output(original['child'],credit)
        if actual is None or _pin(actual,_node_raw(actual,credit))!=node_pin:
            raise ValueError('T4 retained CREATE final readback changed')
    return value
