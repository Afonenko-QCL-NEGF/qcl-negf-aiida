"""L1 historical saved-source registration on the existing AiiDA owner.

No CalcJob outputs/retrieval are changed. Site bindings/admissions are trusted
administrative inputs; this module neither invents nor measures them. Reads of a
stored registration use retained metadata and the AiiDA graph, never NFS.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import stat
from uuid import UUID

from aiida import orm
from aiida.common.exceptions import NotExistent
from qcl_negf_contracts.artifacts import validate_commit
from qcl_negf_contracts.messages import decode, scientific_plan

SCHEMA = "qcl-negf.aiida-saved-source.v1"
MAX_METADATA = 1024*1024
MAX_RESPONSE = 2*1024*1024
MAX_POINTS = 256
PIN_PATH = "_aiida/selection.json"
_bindings = {}
_pending = {}


class SourceError(ValueError):
    def __init__(self, code, detail=None):
        super().__init__(code)
        self.code, self.detail = code, detail


def _fail(code, detail=None): raise SourceError(code, detail)
def _sha(raw): return hashlib.sha256(raw).hexdigest()
def _canonical(value): return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
def _same(a, b): return type(a) is type(b) and _canonical(a) == _canonical(b)


def _uuid(value):
    try:
        if type(value) is not str or str(UUID(value)) != value: _fail("invalid_binding")
    except (ValueError,TypeError,AttributeError) as error:
        raise SourceError("invalid_binding") from error
    return value


def _hash(value):
    if type(value) is not str or len(value)!=64 or any(c not in "0123456789abcdef" for c in value): _fail("invalid_binding")
    return value


def _positive(value, maximum=None):
    if type(value) is not int or value<1 or (maximum is not None and value>maximum): _fail("invalid_binding")
    return value


def _relative(value):
    if (type(value) is not str or not value or "\\" in value or "\0" in value or
            any(ord(c)<32 for c in value) or PurePosixPath(value).is_absolute() or
            any(p in ("", ".", "..") for p in value.split("/"))): _fail("invalid_binding")
    return value


def _absolute(value):
    if (type(value) is not str or not value.startswith("/") or value=="/" or
            str(PurePosixPath(value))!=value or ".." in PurePosixPath(value).parts or
            "\\" in value or any(ord(c)<32 for c in value)): _fail("invalid_binding")
    return value


def configure_saved_sources(namespaces):
    """Owner-only injection of measured site bindings, with no private defaults.

    No filesystem access occurs here. A registration additionally requires its
    exact CalcJob admission/hold; serving retained brief does not require a mount.
    """
    global _bindings
    if not isinstance(namespaces, dict): _fail("invalid_binding")
    _bindings=deepcopy(namespaces)


def _owner():
    from . import service
    return service


def _query(fields, limit=257):
    filters={"attributes.qcl_saved_source_kind":SCHEMA}
    filters.update({"attributes."+k:v for k,v in fields.items()})
    return orm.QueryBuilder().append(orm.Dict,filters=filters).limit(limit).all(flat=True)


def _request(request_uuid):
    rows=_query({"request_uuid":request_uuid},2)
    if len(rows)>1: _fail("source_identity_mismatch", "duplicate_request")
    return rows[0] if rows else None


def _exact_child(run_uuid, execution_id, attempt, calcjob_uuid):
    _uuid(run_uuid); _uuid(calcjob_uuid); _positive(attempt)
    if type(execution_id) is not str or not _owner().IDENTIFIER.fullmatch(execution_id): _fail("invalid_binding")
    try:
        child,a,c=_owner()._export_child(run_uuid,execution_id,attempt=attempt,calcjob_uuid=calcjob_uuid)
        run=_owner()._node(run_uuid)
    except (ValueError,LookupError,AttributeError,KeyError) as error:
        raise SourceError("source_identity_mismatch") from error
    if (a,c)!=(attempt,calcjob_uuid) or child.is_terminated is not True: _fail("source_not_quiescent")
    try:
        remote=child.outputs.remote_folder
        if remote.creator.uuid!=child.uuid or child.inputs.code.computer.uuid!=remote.computer.uuid:
            _fail("source_identity_mismatch")
        _uuid(remote.uuid); _uuid(remote.computer.uuid)
    except (AttributeError,KeyError) as error: raise SourceError("source_unavailable") from error
    return run,child,remote


def _node_bytes(node, maximum, budget):
    with node.open(mode="rb") as stream: raw=stream.read(min(maximum,budget.remaining)+1)
    budget.charge(len(raw))
    if len(raw)>maximum: _fail("budget_exceeded")
    return raw


class _Budget:
    def __init__(self, maximum): self.remaining=_positive(maximum); self.delivered=0
    def charge(self, count):
        if count>self.remaining: _fail("budget_exceeded")
        self.remaining-=count; self.delivered+=count


def _safe_file(root, relative):
    root=Path(_absolute(str(root)))
    for part in [*reversed(root.parents),root]:
        if part.is_symlink(): _fail("source_identity_mismatch", "symlink_ancestry")
    path=root
    for part in _relative(relative).split("/"):
        path=path/part
        if path.is_symlink(): _fail("source_identity_mismatch", "symlink_path")
    return path


def _nfs_read(root, relative, maximum, budget):
    path=_safe_file(root,relative)
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
    with os.fdopen(descriptor,"rb") as stream:
        info=os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode): _fail("source_identity_mismatch", "not_regular_file")
        if info.st_size>maximum or info.st_size>budget.remaining: _fail("budget_exceeded")
        raw=stream.read(min(maximum,budget.remaining)+1)
    budget.charge(len(raw))
    if len(raw)>maximum: _fail("budget_exceeded")
    return raw


def _json(raw, maximum):
    try: return decode(raw,maximum=maximum)
    except (ValueError,UnicodeError,TypeError) as error: raise SourceError("corrupt_result") from error


def _binding(namespace_id, policy, selection, remote):
    try:
        binding=_bindings[namespace_id]
        admission=binding["admissions"][selection["calcjob_uuid"]]
        root=_absolute(binding["root"]); remote_root=PurePosixPath(_absolute(binding["remote_root"]))
        actual=PurePosixPath(_absolute(remote.get_remote_path()))
        relative=_relative(str(actual.relative_to(remote_root)/"result"))
        expected={"run_uuid":selection["run_uuid"],"execution_id":selection["execution_id"],"attempt":selection["attempt"],
                  "remote_folder_uuid":remote.uuid,"relative_result_root":relative,
                  "storage_identity":binding["storage_identity"],"retention_policy_id":policy}
        if any(not _same(admission.get(k),v) for k,v in expected.items()): _fail("source_identity_mismatch")
        if binding["computer_uuid"]!=remote.computer.uuid: _fail("source_identity_mismatch")
        if admission["scheduler_stopped"] is not True or admission["writers_stopped"] is not True:
            _fail("source_not_quiescent")
        for container,key in ((binding,"storage_identity"),(binding,"registration_source_id"),
                (admission,"hold_ref"),(admission,"access_binding_ref"),(admission,"retention_policy_id"),(admission,"retention_owner_id")):
            value=container[key]
            if type(value) is not str or not 1<=len(value)<=256 or value.startswith("/") or any(ord(c)<32 for c in value): _fail("invalid_binding")
        maximum=_positive(admission["maximum_read_bytes"])
    except SourceError: raise
    except (ValueError,KeyError,AttributeError,TypeError) as error: raise SourceError("source_unavailable", "admission_required") from error
    return binding,admission,_safe_file(root,relative),relative,_Budget(maximum)


def _plan(run,child,maximum,budget):
    try:
        node=child.inputs.plan
        raw=_node_bytes(node,maximum,budget)
        parent_raw=_node_bytes(run.inputs.plan,maximum,budget)
        if raw!=parent_raw: _fail("source_identity_mismatch")
        plan=scientific_plan(_json(raw,maximum)); _hash(plan["fingerprint"])
        if plan.get("scientific_fingerprint") is not None: _hash(plan["scientific_fingerprint"])
    except SourceError: raise
    except (ValueError,AttributeError,KeyError) as error: raise SourceError("corrupt_result") from error
    return node,raw,plan


def _retained_selection(child, selection, result_sha256, maximum, budget):
    try:
        node=child.outputs.selection
        if node.creator.uuid!=child.uuid: _fail("source_identity_mismatch")
        _uuid(node.uuid)
        pin=node.get_dict()
    except (AttributeError,KeyError) as error:
        raise SourceError("source_selection_required","retained_point_pin_unavailable") from error
    if not isinstance(pin,dict) or not isinstance(pin.get("commits"),dict): _fail("corrupt_result")
    if len(pin["commits"])>MAX_POINTS: _fail("budget_exceeded")
    expected={"calcjob_uuid":selection["calcjob_uuid"],"execution_id":selection["execution_id"],
              "attempt":selection["attempt"],"result_sha256":result_sha256}
    if any(not _same(pin.get(key),value) for key,value in expected.items()): _fail("source_identity_mismatch")
    encoded=_canonical(pin)
    if len(encoded)>maximum: _fail("budget_exceeded")
    budget.charge(len(encoded))
    return node,encoded,pin


def _check_point_pin(pin, point, selection):
    original=pin["commits"].get(point["point_id"])
    if not isinstance(original,dict): _fail("source_selection_required","retained_point_pin_unavailable")
    expected={"path":point["commit_path"],"commit_sha256":point["commit_sha256"],"identity":point["native_identity"],
              "state_id":point["state_id"],"state_sequence":point["state_sequence"]}
    if any(not _same(original.get(key),value) for key,value in expected.items()): _fail("source_identity_mismatch")
    identity=point["native_identity"]
    if "calcjob_uuid" in identity and not _same(identity["calcjob_uuid"],point["source_calcjob_uuid"]):
        _fail("source_identity_mismatch")
    if point["source_calcjob_uuid"]==selection["calcjob_uuid"] and "run_uuid" in identity and not _same(identity["run_uuid"],selection["run_uuid"]):
        _fail("source_identity_mismatch")


def _check_ack(name, acknowledgement, series, points, selection):
    if (acknowledgement.get("execution_id")!=selection["execution_id"] or
            acknowledgement.get("publication_scope")!="local_filesystem"):
        _fail("source_identity_mismatch")
    if name=="stop-receipt.json":
        if (acknowledgement.get("schema")!="qcl-negf-stop-receipt-v1" or
                series.get("status") not in ("completed","completed_with_warnings","failed","cancelled") or
                acknowledgement.get("status")!=series["status"] or
                not _same(acknowledgement.get("attempt"),selection["attempt"])):
            _fail("source_identity_mismatch")
    else:
        matches=[point for point in points if point["commit_path"]==acknowledgement.get("commit_path")]
        if len(matches)!=1 or acknowledgement.get("schema")!="qcl-negf-pause-receipt-v1": _fail("source_identity_mismatch")
        point=matches[0]
        if (acknowledgement.get("status")!="paused" or series.get("status")!="paused" or
                point["recorded_statuses"].get("status")!="paused" or acknowledgement.get("point_id")!=point["point_id"] or
                not _same(acknowledgement.get("attempt"),point["source_attempt"]) or
                any(not _same(acknowledgement.get(key),point[key]) for key in ("commit_sha256","state_id","state_sequence"))):
            _fail("source_identity_mismatch")


def _graph(record, budget):
    """Rebind actual AiiDA relations and exact retained bytes, with no NFS I/O."""
    selection=record["selection"]
    run,child,remote=_exact_child(**selection)
    if (record["storage"]["remote_folder_uuid"]!=remote.uuid or record["storage"]["computer_uuid"]!=remote.computer.uuid or
            record["plan"]["input_node_uuid"]!=child.inputs.plan.uuid): _fail("source_identity_mismatch")
    plan_node,plan_bytes,plan=_plan(run,child,MAX_METADATA,budget)
    if (_sha(plan_bytes)!=record["plan"]["sha256"] or plan["fingerprint"]!=record["plan"]["plan_fingerprint"] or
            plan.get("scientific_fingerprint")!=record["plan"]["scientific_fingerprint"]): _fail("source_identity_mismatch")
    try:
        result=child.outputs.result
        if result.creator.uuid!=child.uuid: _fail("source_identity_mismatch")
    except (AttributeError,KeyError) as error: raise SourceError("source_series_required","no_series_registration_deferred") from error
    series=_node_bytes(result,MAX_METADATA,budget)
    if len(series)!=record["snapshot"]["result_bytes"] or _sha(series)!=record["snapshot"]["result_sha256"]:
        _fail("source_identity_mismatch")
    return plan_node,result,plan_bytes,series,child


def _validate_record(record,metadata,*,request_uuid,request_sha256,selection):
    expected_keys={"schema","selection","plan","storage","snapshot","points","metadata","integrity","copy_status","request_uuid","request_sha256","registration_source_id"}
    if set(record)!=expected_keys or record["schema"]!=SCHEMA or record["request_uuid"]!=request_uuid or record["request_sha256"]!=request_sha256:
        _fail("corrupt_result")
    if set(record["selection"])!={"run_uuid","execution_id","attempt","calcjob_uuid"}: _fail("corrupt_result")
    for key in ("run_uuid","execution_id","attempt","calcjob_uuid"):
        if not _same(record["selection"].get(key),selection[key]): _fail("corrupt_result")
    _uuid(record["plan"]["input_node_uuid"]); _hash(record["plan"]["sha256"]); _hash(record["plan"]["plan_fingerprint"])
    if record["plan"]["scientific_fingerprint"] is not None: _hash(record["plan"]["scientific_fingerprint"])
    storage=record["storage"]
    if set(storage)!={"namespace_id","storage_identity","computer_uuid","remote_folder_uuid","relative_result_root",
            "retention_policy_id","retention_owner_id","hold_ref","access_binding_ref"}: _fail("corrupt_result")
    _uuid(storage["computer_uuid"]); _uuid(storage["remote_folder_uuid"]); _relative(storage["relative_result_root"])
    for key in ("namespace_id","storage_identity","retention_policy_id","retention_owner_id","hold_ref","access_binding_ref"):
        value=storage[key]
        if type(value) is not str or not 1<=len(value)<=256 or value.startswith("/") or any(ord(c)<32 for c in value): _fail("corrupt_result")
    snapshot=record["snapshot"]
    _hash(snapshot["result_sha256"]); _positive(snapshot["result_bytes"],MAX_METADATA); _hash(snapshot["metadata_set_sha256"])
    if type(snapshot["complete"]) is not bool or snapshot["publication_state"]!="closed_registered": _fail("corrupt_result")
    if record["integrity"]!={"metadata":"verified","payload":"not_measured","payload_receipt_ref":None}:
        _fail("unsupported_contract")
    if (not isinstance(record["points"],list) or not isinstance(record["metadata"],list) or
            len(record["points"])>MAX_POINTS or len(record["metadata"])>1024): _fail("budget_exceeded")
    for point in record["points"]:
        _relative(point["commit_path"]); _hash(point["commit_sha256"]); _hash(point["receipt_sha256"])
        source_attempt=_positive(point["source_attempt"]); row_attempt=_positive(point["row_attempt"])
        if not source_attempt<=row_attempt<=record["selection"]["attempt"]: _fail("corrupt_result")
        if point["source_calcjob_uuid"] is not None: _uuid(point["source_calcjob_uuid"])
        if point["storage_class"] not in ("archive","recovery"): _fail("corrupt_result")
        _positive(point["state_sequence"])
        owner=point["declared_analysis"]
        if owner is not None:
            _relative(owner["path"]); _hash(owner["sha256"])
            if type(owner["bytes"]) is not int or owner["bytes"]<0 or owner["schema"]!="qcl-negf-physics-analysis-v4": _fail("corrupt_result")
    if not isinstance(metadata,dict) or len(record["metadata"])>1024 or len(record["points"])>MAX_POINTS: _fail("budget_exceeded")
    if set(metadata)!={item["path"] for item in record["metadata"]}: _fail("corrupt_result")
    total=0
    for item in record["metadata"]:
        _relative(item["path"]); _uuid(item["retained_node_uuid"]); _hash(item["sha256"])
        if type(metadata[item["path"]]) is not str: _fail("corrupt_result")
        raw=metadata[item["path"]].encode(); total+=len(raw)
        if type(item["bytes"]) is not int or item["bytes"]!=len(raw) or item["sha256"]!=_sha(raw): _fail("corrupt_result")
    if total>MAX_METADATA: _fail("budget_exceeded")
    if record["snapshot"]["metadata_set_sha256"]!=_sha(_canonical(record["metadata"])): _fail("corrupt_result")
    return None


def _load(node, budget=None):
    budget=budget or _Budget(8*MAX_METADATA)
    if not isinstance(node,orm.Dict): _fail("source_unavailable")
    values=node.get_dict()
    budget.charge(len(_canonical(values)))
    if set(values)!={"qcl_saved_source_kind","request_uuid","request_sha256","run_uuid","execution_id","attempt","calcjob_uuid","record_json","retained_metadata"} or values["qcl_saved_source_kind"]!=SCHEMA:
        _fail("corrupt_result")
    _uuid(node.uuid); _uuid(values["request_uuid"]); _hash(values["request_sha256"])
    record=_json(values["record_json"].encode(),MAX_RESPONSE)
    metadata=values["retained_metadata"]
    _validate_record(record,metadata,request_uuid=values["request_uuid"],request_sha256=values["request_sha256"],
                     selection={key:values[key] for key in ("run_uuid","execution_id","attempt","calcjob_uuid")})
    plan_node,result,plan_raw,series_raw,child=_graph(record,budget)
    pin_node,pin_raw,pin=_retained_selection(child,record["selection"],record["snapshot"]["result_sha256"],MAX_METADATA,budget)
    references={item["path"]:item["retained_node_uuid"] for item in record["metadata"]}
    if references.get("series_result.json")!=result.uuid:
        _fail("source_identity_mismatch")
    if references.get(PIN_PATH)!=pin_node.uuid or metadata.get(PIN_PATH,"").encode()!=pin_raw:
        _fail("source_identity_mismatch")
    if any(owner_uuid!=node.uuid for name,owner_uuid in references.items() if name not in ("series_result.json",PIN_PATH)):
        _fail("source_identity_mismatch")
    if metadata.get("series_result.json","").encode()!=series_raw: _fail("source_identity_mismatch")
    series=_json(series_raw,MAX_METADATA)
    rows={row["id"]:row for row in series.get("points",[])}
    plan=_json(plan_raw,MAX_METADATA)
    frozen={point["id"] for point in plan["points"] if point["execution_id"]==record["selection"]["execution_id"]}
    if record["snapshot"]["complete"] and {point["point_id"] for point in record["points"]}!=frozen:
        _fail("source_identity_mismatch")
    for point in record["points"]:
        _check_point_pin(pin,point,record["selection"])
        row=rows.get(point["point_id"])
        if row is None or point["point_id"] not in frozen or row.get("data",{}).get("result_commit")!=point["commit_path"]:
            _fail("source_identity_mismatch")
        encoded=metadata.get(point["commit_path"],"").encode()
        commit=_json(encoded,MAX_METADATA)
        artifacts=validate_commit(commit)
        if (_sha(encoded)!=point["commit_sha256"] or not _same(commit["identity"],point["native_identity"]) or
                not _same(commit["identity"].get("attempt"),point["source_attempt"]) or
                not _same(row.get("attempt"),point["row_attempt"])): _fail("source_identity_mismatch")
        if _source_child_uuid(child,point["source_attempt"],plan_node.uuid)!=point["source_calcjob_uuid"]:
            _fail("source_identity_mismatch")
        prefix=point["commit_path"].removesuffix("commit.json")
        receipt_raw=metadata.get(prefix+"receipt.json","").encode(); receipt=_json(receipt_raw,MAX_METADATA)
        if (_sha(receipt_raw)!=point["receipt_sha256"] or receipt.get("commit_sha256")!=point["commit_sha256"] or
                not _same(receipt.get("identity"),point["native_identity"])): _fail("source_identity_mismatch")
        owners=[owner for owner in artifacts if owner.role=="physics.analysis"]
        if len(owners)>1: _fail("corrupt_result")
        declared=None if not owners else {"path":prefix+owners[0].path,"bytes":owners[0].size,
            "sha256":owners[0].sha256,"schema":owners[0].schema}
        if not _same(declared,point["declared_analysis"]): _fail("source_identity_mismatch")

    for name in ("stop-receipt.json","pause-receipt.json"):
        if name in metadata:
            acknowledgement=_json(metadata[name].encode(),MAX_METADATA)
            _check_ack(name,acknowledgement,series,record["points"],record["selection"])
            if name=="stop-receipt.json" and acknowledgement.get("result_sha256")!=record["snapshot"]["result_sha256"]:
                _fail("source_identity_mismatch")

    # Stored NFS plan may differ in whitespace but must bind the retained plan.
    if not _same(_json(metadata["scientific_plan.json"].encode(),MAX_METADATA),_json(plan_raw,MAX_METADATA)):
        _fail("source_identity_mismatch")
    if len(_canonical(values))>MAX_RESPONSE: _fail("budget_exceeded")
    return record,metadata,plan_raw


def _receipt(node,status="stored",budget=None):
    record,metadata,plan_raw=_load(node,budget)
    return {"status":status,"source_uuid":node.uuid,"source_sha256":_sha(_canonical(record)),
            "selection":record["selection"],"metadata_status":"verified","copy_status":record["copy_status"]}


def _meaning(**fields): return _sha(_canonical({"operation":"register_saved_source",**fields}))


def _replay(request_uuid,digest):
    existing=_request(request_uuid)
    if existing is not None:
        if existing.get_dict().get("request_sha256")!=digest: _fail("request_conflict")
        response=_receipt(existing,"replayed"); _pending.pop(request_uuid,None); return response
    if request_uuid in _pending:
        if _pending[request_uuid]["digest"]!=digest: _fail("request_conflict")
        return {"status":"unknown","reason":"registration_outcome_unknown",**_pending[request_uuid]}
    return None


def _source_child_uuid(child, source_attempt, plan_uuid):
    if source_attempt==_owner()._export_child_identity(child)[0]: return child.uuid
    candidates=set()
    for name in ("recovery_bundle","prior_archive"):
        try:
            prior=getattr(child.inputs,name).creator
            if (prior.inputs.execution_id.value==child.inputs.execution_id.value and
                    prior.inputs.attempt.value==source_attempt and prior.inputs.plan.uuid==plan_uuid):
                candidates.add(_uuid(prior.uuid))
        except (AttributeError,KeyError,ValueError):
            continue
    return next(iter(candidates)) if len(candidates)==1 else None


def _capture(run_uuid, execution_id, attempt, calcjob_uuid, namespace_id, retention_policy_id,
             expected_result_sha256, expected_result_bytes, maximum):
    selection={"run_uuid":run_uuid,"execution_id":execution_id,"attempt":attempt,"calcjob_uuid":calcjob_uuid}
    run,child,remote=_exact_child(**selection)
    binding,admission,root,relative,budget=_binding(namespace_id,retention_policy_id,selection,remote)
    try:
        result=child.outputs.result
        if result.creator.uuid!=child.uuid: _fail("source_identity_mismatch")
    except (AttributeError,KeyError) as error:
        raise SourceError("source_series_required","no_series_registration_deferred") from error
    # Both original series sides are mandatory, before any constructor/retention.
    try:
        retained=_node_bytes(result,maximum,budget)
        nfs_series=_nfs_read(root,"series_result.json",maximum,budget)
    except FileNotFoundError as error:
        raise SourceError("source_series_required","no_series_registration_deferred") from error
    series=_json(retained,maximum); _json(nfs_series,maximum)
    if (len(retained)!=expected_result_bytes or _sha(retained)!=expected_result_sha256 or nfs_series!=retained):
        _fail("source_identity_mismatch")
    pin_node,pin_raw,pin=_retained_selection(child,selection,expected_result_sha256,maximum,budget)
    plan_node,plan_raw,plan=_plan(run,child,maximum,budget)
    if (series.get("schema")!="qcl-negf-series-result-v3" or series.get("contract_set")!="qcl-negf.results.v1"):
        _fail("corrupt_result")
    if series.get("plan_fingerprint")!=plan["fingerprint"] or series.get("selected_execution_id")!=execution_id:
        _fail("source_identity_mismatch")
    executions=[e for e in plan["executions"] if e["id"]==execution_id]
    if len(executions)!=1: _fail("source_identity_mismatch")
    frozen={p["id"]:p for p in plan["points"] if p.get("execution_id")==execution_id}
    if set(executions[0].get("point_ids",[]))!=set(frozen): _fail("corrupt_result")
    metadata={"series_result.json":retained}
    def capture(path):
        path=_relative(path)
        if path not in metadata:
            metadata[path]=_nfs_read(root,path,maximum,budget)
            if sum(map(len,metadata.values()))>maximum: _fail("budget_exceeded")
        return _json(metadata[path],maximum)
    nfs_plan=capture("scientific_plan.json")
    if not _same(nfs_plan,plan): _fail("source_identity_mismatch")
    rows=series.get("points")
    if not isinstance(rows,list) or len(rows)>MAX_POINTS: _fail("budget_exceeded")
    points=[]; seen=set()
    for row in rows:
        if not isinstance(row,dict): _fail("corrupt_result")
        if row.get("execution_id")!=execution_id: _fail("source_identity_mismatch")
        point_id=row.get("id")
        if point_id not in frozen or point_id in seen: _fail("source_identity_mismatch")
        seen.add(point_id)
        row_attempt=_positive(row.get("attempt"))
        if row_attempt>attempt: _fail("source_identity_mismatch")
        coordinates=row.get("coordinates",{})
        for key in ("temperature_K","voltage_per_period_V","branch","order"):
            if key not in coordinates or not _same(coordinates[key],frozen[point_id].get(key)): _fail("source_identity_mismatch")
        data=row.get("data") or {}
        reference=data.get("result_commit")
        if reference is None: continue
        reference=_relative(reference)
        if not reference.endswith("/commit.json"): _fail("corrupt_result")
        commit=capture(reference)
        artifacts=validate_commit(commit)
        identity=commit["identity"]
        source_attempt=_positive(identity.get("attempt"))
        expected_attempt=data.get("checkpoint_source_attempt",row_attempt)
        if source_attempt!=expected_attempt or source_attempt>row_attempt: _fail("source_identity_mismatch")
        if "checkpoint_source_attempt" in data and (row.get("status")!="paused" or data.get("pause_reason")!="resource_pressure" or
                data.get("resume_kind")!="checkpoint" or data.get("recovery_origin")!="last_committed_before_resource_pause" or
                not source_attempt<row_attempt): _fail("source_identity_mismatch")
        expected={"point_id":point_id,"execution_id":execution_id,"plan_fingerprint":plan["fingerprint"]}
        if any(not _same(identity.get(k),v) for k,v in expected.items()): _fail("source_identity_mismatch")
        state_id=commit.get("state_id"); sequence=commit.get("state_sequence")
        if type(state_id) is not str or not state_id or type(sequence) is not int or sequence<1: _fail("corrupt_result")
        prefix=reference.removesuffix("commit.json")
        receipt_path=prefix+"receipt.json"; receipt=capture(receipt_path)
        commit_digest=_sha(metadata[reference])
        if (receipt.get("schema")!="qcl-negf-recovery-receipt-v1" or receipt.get("status")!="verified" or
                receipt.get("publication_scope")!="local_filesystem" or receipt.get("commit_sha256")!=commit_digest or
                not _same(receipt.get("identity"),identity) or receipt.get("state_id")!=state_id or
                not _same(receipt.get("state_sequence"),sequence)): _fail("source_identity_mismatch")
        owners=[a for a in artifacts if a.role=="physics.analysis"]
        if len(owners)>1: _fail("corrupt_result")
        analysis=None
        if owners:
            owner=owners[0]
            if owner.schema!="qcl-negf-physics-analysis-v4": _fail("corrupt_result")
            analysis={"path":prefix+owner.path,"bytes":owner.size,"sha256":owner.sha256,"schema":owner.schema}
        # Only bounded declared JSON metadata dependencies, never physical bytes.
        for artifact in artifacts:
            if artifact.role=="model":
                model=capture(prefix+artifact.path)
                encoded=metadata[prefix+artifact.path]
                if len(encoded)!=artifact.size or _sha(encoded)!=artifact.sha256: _fail("source_identity_mismatch")
                if model.get("schema")!="qcl-negf-resolved-configuration-v3": _fail("corrupt_result")
                if model.get("hash_encoding")!="qcl-negf-canonical-bytes-v1": _fail("corrupt_result")
                _hash(model.get("configuration_hash"))
                passport=frozen[point_id].get("effective_inputs",{})
                if passport.get("configuration_hash") is not None and model["configuration_hash"]!=passport["configuration_hash"]:
                    _fail("source_identity_mismatch")
        source_uuid=_source_child_uuid(child,source_attempt,plan_node.uuid)
        status={k:row.get(k) for k in ("status","quality","converged")}
        status["scientific_assessment_source_path"]="series_result.json#points/"+point_id+"/observables/scientific_assessment"
        if source_uuid is None: status["source_calcjob_reason"]="explicit_recovery_relation_absent_or_ambiguous"
        points.append({"point_id":point_id,"row_attempt":row_attempt,"source_attempt":source_attempt,
            "source_calcjob_uuid":source_uuid,"commit_path":reference,"commit_sha256":commit_digest,
            "state_id":state_id,"state_sequence":sequence,"receipt_sha256":_sha(metadata[receipt_path]),
            "storage_class":commit.get("storage_class"),"native_identity":identity,
            "declared_analysis":analysis,"recorded_statuses":status})
    for point in points:
        _check_point_pin(pin,point,selection)
    # Acknowledgements cross-check the same anchored generation/result only.
    for name in ("stop-receipt.json","pause-receipt.json"):
        try: acknowledgement=capture(name)
        except FileNotFoundError: continue
        _check_ack(name,acknowledgement,series,points,selection)
        if name=="stop-receipt.json" and acknowledgement.get("result_sha256")!=expected_result_sha256:
            _fail("source_identity_mismatch")
    # Stable metadata readback: immutable hold is a trusted admission predicate,
    # not a claim of defeating a hostile storage administrator/renamer.
    for name,raw in metadata.items():
        if _nfs_read(root,name,maximum,budget)!=raw: _fail("source_identity_mismatch")
    if _node_bytes(result,maximum,budget)!=retained: _fail("source_identity_mismatch")
    if _node_bytes(plan_node,maximum,budget)!=plan_raw: _fail("source_identity_mismatch")
    metadata[PIN_PATH]=pin_raw
    if sum(map(len,metadata.values()))>maximum: _fail("budget_exceeded")
    record={"schema":SCHEMA,"selection":selection,
        "plan":{"input_node_uuid":plan_node.uuid,"sha256":_sha(plan_raw),"plan_fingerprint":plan["fingerprint"],
                "scientific_fingerprint":plan.get("scientific_fingerprint")},
        "storage":{"namespace_id":namespace_id,"storage_identity":binding["storage_identity"],"computer_uuid":remote.computer.uuid,
            "remote_folder_uuid":remote.uuid,"relative_result_root":relative,"retention_policy_id":retention_policy_id,
            "retention_owner_id":admission["retention_owner_id"],"hold_ref":admission["hold_ref"],"access_binding_ref":admission["access_binding_ref"]},
        "snapshot":{"result_sha256":expected_result_sha256,"result_bytes":expected_result_bytes,
            "metadata_set_sha256":None,"complete":len(points)==len(frozen),"publication_state":"closed_registered"},
        "points":points,"metadata":[],"integrity":{"metadata":"verified","payload":"not_measured","payload_receipt_ref":None},
        "copy_status":"unknown",
        "request_uuid":None,"request_sha256":None,"registration_source_id":binding["registration_source_id"]}
    record["metadata"]=[{"path":PIN_PATH,"retained_node_uuid":pin_node.uuid}]
    return record,metadata,plan_node.uuid,result.uuid,plan_raw


def register_saved_source(run_uuid, execution_id, *, attempt, calcjob_uuid, request_uuid, namespace_id,
                          retention_policy_id, expected_result_sha256, expected_result_bytes, maximum_metadata_bytes):
    """Administrative L1 operation; call on the sole loaded-profile owner only."""
    try:
        _uuid(request_uuid); _uuid(run_uuid); _uuid(calcjob_uuid); _positive(attempt)
        maximum=_positive(maximum_metadata_bytes,MAX_METADATA)
        _hash(expected_result_sha256); _positive(expected_result_bytes,maximum)
        for value in (namespace_id,retention_policy_id):
            if type(value) is not str or not _owner().IDENTIFIER.fullmatch(value): _fail("invalid_binding")
        digest=_meaning(run_uuid=run_uuid,execution_id=execution_id,attempt=attempt,calcjob_uuid=calcjob_uuid,
            namespace_id=namespace_id,retention_policy_id=retention_policy_id,
            expected_result_sha256=expected_result_sha256,expected_result_bytes=expected_result_bytes,
            maximum_metadata_bytes=maximum_metadata_bytes)
        replay=_replay(request_uuid,digest)
        if replay is not None: return replay
        record,metadata,plan_uuid,result_uuid,plan_raw=_capture(run_uuid,execution_id,attempt,calcjob_uuid,namespace_id,
            retention_policy_id,expected_result_sha256,expected_result_bytes,maximum)
        # Exact finite envelope admission uses a canonical-width self UUID
        # placeholder before ANY constructor; real UUID replacement is same-size.
        _uuid(plan_uuid); _uuid(result_uuid)
        self_uuid="00000000-0000-0000-0000-000000000000"
        record["request_uuid"],record["request_sha256"]=request_uuid,digest
        special={item["path"]:item["retained_node_uuid"] for item in record["metadata"]}
        record["metadata"]=[{"path":name,"bytes":len(raw),"sha256":_sha(raw),
            "retained_node_uuid":result_uuid if name=="series_result.json" else special.get(name,self_uuid)}
            for name,raw in sorted(metadata.items())]
        record["snapshot"]["metadata_set_sha256"]=_sha(_canonical(record["metadata"]))
        envelope={"qcl_saved_source_kind":SCHEMA,"request_uuid":request_uuid,"request_sha256":digest,
            **record["selection"],"record_json":_canonical(record).decode(),
            "retained_metadata":{name:raw.decode("utf-8") for name,raw in metadata.items()}}
        if len(_canonical(envelope))>MAX_RESPONSE: _fail("budget_exceeded")
        _validate_record(record,envelope["retained_metadata"],request_uuid=request_uuid,request_sha256=digest,
                         selection=record["selection"])
        # Pre-admit the complete public response, not only the stored envelope.
        if len(_canonical({"status":"available","source_uuid":self_uuid,"source_sha256":"f"*64,
            "selection":record["selection"],"record":record,"metadata":envelope["retained_metadata"],
            "plan":plan_raw.decode(),
            "metadata_status":"verified","copy_status":record["copy_status"],"access_status":"not_checked"}))>MAX_RESPONSE:
            _fail("budget_exceeded")
    except SourceError as error:
        return {"status":"rejected","reason":error.code,**({"detail":error.detail} if error.detail else {})}
    except (ValueError,TypeError,KeyError,AttributeError):
        return {"status":"rejected","reason":"corrupt_result"}
    except OSError:
        return {"status":"rejected","reason":"source_unavailable"}
    node=orm.Dict()
    for item in record["metadata"]:
        if item["path"] not in ("series_result.json",PIN_PATH): item["retained_node_uuid"]=node.uuid
    record["snapshot"]["metadata_set_sha256"]=_sha(_canonical(record["metadata"]))
    envelope["record_json"]=_canonical(record).decode()
    node.set_dict(envelope)
    _pending[request_uuid]={"digest":digest,"source_uuid":node.uuid}
    try:
        node.store()
    except Exception:
        # Outcome may already be committed. Never construct/store a replacement
        # in this owner; same request reconciles immutable node evidence first.
        return {"status":"unknown","reason":"registration_outcome_unknown",**_pending[request_uuid]}
    _pending.pop(request_uuid,None)
    return _receipt(node)


def get_saved_sources(run_uuid, execution_id, *, attempt, calcjob_uuid):
    """Detached catalogue of exact selections; no NFS/mount/HDF operations."""
    selection={"run_uuid":run_uuid,"execution_id":execution_id,"attempt":attempt,"calcjob_uuid":calcjob_uuid}
    _exact_child(**selection)
    rows=_query(selection)
    if len(rows)>256: _fail("budget_exceeded")
    budget=_Budget(8*MAX_METADATA)
    result=[_receipt(node,"available",budget) for node in rows]
    if len(_canonical(result))>MAX_RESPONSE: _fail("budget_exceeded")
    return {"sources":result,"selection":selection,"complete":True}


def get_saved_source(run_uuid, execution_id, *, attempt, calcjob_uuid, source_uuid=None, expected_source_sha256=None):
    selection={"run_uuid":run_uuid,"execution_id":execution_id,"attempt":attempt,"calcjob_uuid":calcjob_uuid}
    _exact_child(**selection)
    if source_uuid is None:
        rows=_query(selection,2)
        if not rows: _fail("source_unavailable")
        if len(rows)>1: _fail("selection_required")
        node=rows[0]
    else:
        _uuid(source_uuid)
        try: node=orm.load_node(source_uuid)
        except (KeyError,LookupError,NotExistent) as error: raise SourceError("source_unavailable") from error
    record,metadata,plan_raw=_load(node)
    if not _same(record["selection"],selection): _fail("source_identity_mismatch")
    digest=_sha(_canonical(record))
    if expected_source_sha256 is not None and _hash(expected_source_sha256)!=digest: _fail("source_identity_mismatch")
    result={"status":"available","source_uuid":node.uuid,"source_sha256":digest,"selection":selection,
        "record":record,"metadata":metadata,"plan":plan_raw.decode("utf-8"),"metadata_status":"verified","copy_status":record["copy_status"],
        "access_status":"not_checked"}
    if len(_canonical(result))>MAX_RESPONSE: _fail("budget_exceeded")
    return result
