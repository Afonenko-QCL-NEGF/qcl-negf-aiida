"""L1 historical saved-source registration on the existing AiiDA owner.

Historical L1 registration does not change CalcJob outputs or retrieval. Site
bindings/admissions remain administrative inputs. Native B helpers below own an
additive parser/output boundary; retained reads use only ORM/repository metadata.
"""
from __future__ import annotations

from copy import deepcopy
from contextlib import contextmanager
from contextvars import ContextVar
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
PERFORMANCE_PATH = "_aiida/performance-binding.json"
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
        if not source_attempt<=row_attempt: _fail("corrupt_result")
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
    if PERFORMANCE_PATH in metadata:
        from .performance_binding import retained_source
        size=len(metadata[PERFORMANCE_PATH].encode()); budget.charge(size)
        retained=retained_source(child,maximum=size)
        if (retained["status"]!="available" or references[PERFORMANCE_PATH]!=retained["sidecar_uuid"]
                or metadata[PERFORMANCE_PATH]!=retained["sidecar_utf8"]): _fail("source_identity_mismatch")
    if any(owner_uuid!=node.uuid for name,owner_uuid in references.items() if name not in ("series_result.json",PIN_PATH,PERFORMANCE_PATH)):
        _fail("source_identity_mismatch")
    if metadata.get("series_result.json","").encode()!=series_raw: _fail("source_identity_mismatch")
    series=_json(series_raw,MAX_METADATA)
    from .science_association import validate_native_collisions
    validate_native_collisions(series["points"],pin["commits"],input_attempt=record["selection"]["attempt"],
        execution_id=record["selection"]["execution_id"],plan_fingerprint=record["plan"]["plan_fingerprint"],calcjob_uuid=child.uuid,
        scientific_fingerprint=record["plan"]["scientific_fingerprint"],result_scientific_fingerprint=series.get("plan_scientific_fingerprint"))
    source_proofs={child.uuid:(series,pin)}
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
        if _source_child_uuid(child,point["source_attempt"],plan_node.uuid,point_id=point["point_id"],
                commit_sha256=point["commit_sha256"],identity=point["native_identity"],proofs=source_proofs,budget=budget,
                scientific_fingerprint=record["plan"]["scientific_fingerprint"])!=point["source_calcjob_uuid"]:
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


def _source_child_uuid(child, source_attempt, plan_uuid, *, point_id, commit_sha256, identity,
                       proofs=None,budget=None,scientific_fingerprint=None):
    from .science_association import source_point_owner
    return source_point_owner(child,source_attempt,plan_uuid,point_id=point_id,commit_sha256=commit_sha256,
                              identity=identity,proofs=proofs,budget=budget,scientific_fingerprint=scientific_fingerprint)


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
    points=[]; seen=set(); source_proofs={child.uuid:(series,pin)}
    for row in rows:
        if not isinstance(row,dict): _fail("corrupt_result")
        if row.get("execution_id")!=execution_id: _fail("source_identity_mismatch")
        point_id=row.get("id")
        if point_id not in frozen or point_id in seen: _fail("source_identity_mismatch")
        seen.add(point_id)
        row_attempt=_positive(row.get("attempt"))
        coordinates=row.get("coordinates",{})
        for key in ("temperature_K","voltage_per_period_V","branch","order"):
            if key not in coordinates or not _same(coordinates[key],frozen[point_id].get(key)): _fail("source_identity_mismatch")
        data=row.get("data") or {}
        reference=data.get("result_commit")
        if row_attempt>attempt and reference is None: _fail("source_identity_mismatch")
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
        from .science_association import validate_native_collisions
        validate_native_collisions([row],pin["commits"],input_attempt=attempt,execution_id=execution_id,
            plan_fingerprint=plan["fingerprint"],calcjob_uuid=child.uuid,
            scientific_fingerprint=plan.get("scientific_fingerprint"),result_scientific_fingerprint=series.get("plan_scientific_fingerprint"))
        source_uuid=_source_child_uuid(child,source_attempt,plan_node.uuid,point_id=point_id,
            commit_sha256=commit_digest,identity=identity,proofs=source_proofs,budget=budget,
            scientific_fingerprint=plan.get("scientific_fingerprint"))
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
    performance_uuid=None
    reference=pin.get("performance_binding")
    if reference is not None:
        # Optional sidecar shares the existing retained metadata allowance.
        # If it cannot fit, retain science as before; performance is unavailable here.
        size=reference.get("sidecar_bytes")
        if type(size)is int and 0<size<=min(maximum-sum(map(len,metadata.values())),budget.remaining):
            from .performance_binding import retained_source
            performance=retained_source(child,maximum=min(maximum,budget.remaining))
            if performance["status"]!="available": _fail("source_identity_mismatch")
            encoded=performance["sidecar_utf8"].encode(); budget.charge(len(encoded))
            metadata[PERFORMANCE_PATH]=encoded; performance_uuid=performance["sidecar_uuid"]
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
    if performance_uuid is not None:
        record["metadata"].append({"path":PERFORMANCE_PATH,"retained_node_uuid":performance_uuid})
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
        if item["path"] not in ("series_result.json",PIN_PATH,PERFORMANCE_PATH): item["retained_node_uuid"]=node.uuid
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


# Native B consumer. This is separate from the historical L1 registrar above:
# a selected access context never certifies the scientific invocation's creator.
_NATIVE_METADATA = 32 * 1024 * 1024
_NATIVE_IO = 256 * 1024 * 1024
_NATIVE_OPERATION_METADATA = 2 * 1024 * 1024
_NATIVE_OPERATION_IO = 16 * 1024 * 1024
_NATIVE_INTEGRITY = dict(scope='selected_metadata', native_contexts='verified',
                         transport_owner='verified', full_core_payloads='not_assessed',
                         current_access='not_checked')


class _NativeLedger:
    """One nonrefundable SOURCE/L allowance, including failures and copies.

    Full foreign operation caps are prepaid. These reservations are an admission
    policy, not a measured RSS bound or an installed native qualification.
    """
    def __init__(self, maximum_metadata_bytes, maximum_io_bytes):
        if (type(maximum_metadata_bytes) is not int or not 0 < maximum_metadata_bytes <= _NATIVE_METADATA
                or type(maximum_io_bytes) is not int or not 0 < maximum_io_bytes <= _NATIVE_IO):
            _fail('budget_exceeded', 'native_outer_cap')
        self.metadata, self.io = maximum_metadata_bytes, maximum_io_bytes
        self.reservations = []

    def reserve(self, operation, *, metadata=0, io=0):
        if (type(metadata) is not int or type(io) is not int or metadata < 0 or io < 0
                or metadata > self.metadata or io > self.io):
            _fail('budget_exceeded', operation)
        self.metadata -= metadata
        self.io -= io
        self.reservations.append((operation, metadata, io))

    def results(self, operation):
        self.reserve(operation, metadata=_NATIVE_OPERATION_METADATA, io=_NATIVE_OPERATION_IO)


_native_credit_scope = ContextVar('qcl_native_consumer_credit', default=None)


@contextmanager
def _native_operation(maximum_metadata_bytes, maximum_io_bytes):
    active = _native_credit_scope.get()
    if active is not None:
        yield active
        return
    ledger = _NativeLedger(maximum_metadata_bytes,maximum_io_bytes)
    token = _native_credit_scope.set(ledger)
    try: yield ledger
    finally: _native_credit_scope.reset(token)


def _native_credit(maximum_metadata_bytes, maximum_io_bytes):
    if (type(maximum_metadata_bytes) is not int or not 0 < maximum_metadata_bytes <= _NATIVE_METADATA
            or type(maximum_io_bytes) is not int or not 0 < maximum_io_bytes <= _NATIVE_IO):
        _fail('budget_exceeded','native_outer_cap')
    active = _native_credit_scope.get()
    if active is None:return _NativeLedger(maximum_metadata_bytes,maximum_io_bytes)
    # Nested calls cannot enlarge or ignore an explicitly tightened allowance.
    # Burn excess remaining credit rather than replacing/resetting the ledger.
    active.reserve('caller tightened remaining allowance',
        metadata=max(0,active.metadata-maximum_metadata_bytes),io=max(0,active.io-maximum_io_bytes))
    return active


def _native_weight(value, *, depth=0, active=None):
    """Conservative detached object/encoding credit without allocating encoding."""
    if depth > 64: _fail('budget_exceeded', 'native_metadata_depth')
    kind = type(value)
    if value is None or kind in (bool, int, float): return 64
    if kind in (str, bytes): return 128 + 6 * len(value)
    if kind not in (dict, list, tuple): _fail('corrupt_result', 'native_metadata_type')
    active = set() if active is None else active
    if id(value) in active: _fail('corrupt_result', 'native_metadata_cycle')
    active.add(id(value))
    try:
        values = value.items() if kind is dict else enumerate(value)
        return 128 + sum(_native_weight(k, depth=depth+1, active=active) +
                         _native_weight(v, depth=depth+1, active=active) for k,v in values)
    finally: active.remove(id(value))


def _native_decode(raw, maximum, ledger, operation):
    # Punctuation in strings only overestimates. Reserve decoded Python/JSON
    # workspace before invoking the decoder, including original object strings.
    workspace=512*(1+sum(raw.count(c) for c in (b'{',b'}',b'[',b']',b',',b':')))+4*len(raw)
    ledger.reserve(operation,metadata=workspace)
    return _json(raw,maximum)


def _native_related(node, kind, label, ledger, *, outgoing=False, optional=False):
    ledger.reserve('ORM typed link query', metadata=16384)
    query = orm.QueryBuilder().append(orm.Node, filters={'uuid':node.uuid}, tag='anchor')
    edge = {'type':kind.value}
    if label is not None: edge['label'] = label
    query.append(orm.Node, **{'with_incoming' if outgoing else 'with_outgoing':'anchor'},
                 edge_filters=edge, project='*')
    rows = query.limit(2).all(flat=True)
    if optional and not rows: return None
    if len(rows) != 1: _fail('source_identity_mismatch', 'missing_or_ambiguous_native_link')
    return rows[0]


def _native_file(node, ledger, *, maximum=65536):
    if not isinstance(node, orm.SinglefileData) or not node.is_stored:
        _fail('source_identity_mismatch', 'stored_native_singlefile_required')
    ledger.reserve('original ORM file handle/size',metadata=8192)
    with node.open(mode='rb') as stream:
        stream.seek(0,2);size=stream.tell()
        if size>maximum:_fail('budget_exceeded','native_file_bound')
        ledger.reserve('original ORM file/decoded workspace',metadata=8*(size+1),io=size+1)
        stream.seek(0);raw=stream.read(size+1)
    if len(raw)!=size: _fail('budget_exceeded', 'native_file_bound')
    return raw


def _native_pin(node, raw):
    return dict(node_uuid=node.uuid, bytes=str(len(raw)), sha256=_sha(raw))


def _native_inputs(child, ledger):
    """Re-read actual immutable typed input/call graph; no runtime actor lookup."""
    from aiida.common.links import LinkType
    from qcl_negf_contracts import schema_validator
    if (not isinstance(child, orm.CalcJobNode) or not child.is_stored
            or child.process_type != 'aiida.calculations:qcl_negf.execution'):
        _fail('source_identity_mismatch', 'native_calcjob_required')
    parent=_native_related(child,LinkType.CALL_CALC,None,ledger)
    processes=[child,parent]
    if parent.process_type=='aiida.workflows:qcl_negf.execution_restart':
        run=_native_related(parent,LinkType.CALL_WORK,None,ledger);processes.append(run)
    else: run=parent
    if not isinstance(run,orm.WorkChainNode) or run.process_type!='aiida.workflows:qcl_negf.plan':
        _fail('source_identity_mismatch','native_plan_call_required')
    labels=('plan','source_graph','native_execution','code','execution_id','attempt','release_id')
    inputs={name:_native_related(child,LinkType.INPUT_CALC,name,ledger) for name in labels}
    code=inputs['code'];attempt=inputs['attempt'];execution=inputs['execution_id'];release=inputs['release_id']
    if (type(attempt) is not orm.Int or type(attempt.value) is not int or attempt.value<1
            or type(execution) is not orm.Str or type(release) is not orm.Str
            or not isinstance(code,orm.InstalledCode) or code.computer is None
            or child.computer is None or child.computer.uuid!=code.computer.uuid):
        _fail('source_identity_mismatch','native_scalar_code_input')
    raw_plan,raw_graph,raw_native=(_native_file(inputs[k],ledger) for k in ('plan','source_graph','native_execution'))
    plan=scientific_plan(_native_decode(raw_plan,65536,ledger,'original plan decoding'))
    graph=_native_decode(raw_graph,65536,ledger,'original graph decoding')
    native=_native_decode(raw_native,65536,ledger,'original execution decoding')
    schema_validator('scientific-plan.schema.json').validate(plan)
    validator=schema_validator('model-help.schema.json')
    validator.evolve(schema={'$ref':'#/$defs/SourceGraph','$defs':validator.schema['$defs']}).validate(graph)
    if (native.get('schema')!='qcl-negf.aiida-native-execution.v1'
            or native.get('input_plan')!=_native_pin(inputs['plan'],raw_plan)
            or native.get('source_graph')!=_native_pin(inputs['source_graph'],raw_graph)
            or native.get('execution_id')!=execution.value
            or native.get('storage',{}).get('execution_binding_uuid')!=inputs['native_execution'].uuid):
        _fail('source_identity_mismatch','native_original_inputs')
    declared=native.get('code',{})
    if (declared.get('code_uuid')!=code.uuid or declared.get('computer_uuid')!=code.computer.uuid
            or native['storage'].get('computer_uuid')!=code.computer.uuid
            or declared.get('solver_executable')!=str(code.filepath_executable)
            or declared.get('release_id')!=release.value):
        _fail('source_identity_mismatch','native_original_code_release')
    for process in processes:
        kind=LinkType.INPUT_CALC if isinstance(process,orm.CalcJobNode) else LinkType.INPUT_WORK
        for name in ('plan','source_graph','code','release_id'):
            if _native_related(process,kind,name,ledger).uuid!=inputs[name].uuid:
                _fail('source_identity_mismatch','native_call_original_input')
        if process.uuid!=run.uuid:
            for name in ('native_execution','execution_id','attempt'):
                if _native_related(process,kind,name,ledger).uuid!=inputs[name].uuid:
                    _fail('source_identity_mismatch','native_call_scalar_input')
    identifiers=[e['id'] for e in plan['executions']]
    if identifiers.count(execution.value)!=1: _fail('source_identity_mismatch','native_execution_inventory')
    name='native_executions__execution_'+str(identifiers.index(execution.value))
    if _native_related(run,LinkType.INPUT_WORK,name,ledger).uuid!=inputs['native_execution'].uuid:
        _fail('source_identity_mismatch','native_plan_execution_input')
    return dict(selection=dict(run_uuid=run.uuid,execution_id=execution.value,attempt=attempt.value,calcjob_uuid=child.uuid),
        frozen_plan=raw_plan,plan_node_uuid=inputs['plan'].uuid,source_graph=dict(value=graph,raw=raw_graph),
        source_graph_node_uuid=inputs['source_graph'].uuid,execution_binding=native)


def _native_result(child, ledger):
    from aiida.common.links import LinkType
    retained=_native_related(child,LinkType.CREATE,'retrieved',ledger,outgoing=True)
    if not isinstance(retained,orm.FolderData):_fail('source_identity_mismatch','native_retrieved_type')
    maximum=min(1024*1024,ledger.metadata//8)
    if maximum<1:_fail('budget_exceeded','native_result_workspace')
    ledger.reserve('retrieved result handle/size',metadata=8192)
    with retained.base.repository.open('series_result.json','rb') as stream:
        stream.seek(0,2);size=stream.tell()
        if size>maximum:_fail('budget_exceeded','native_result_bound')
        ledger.reserve('original retrieved result/workspace',metadata=8*(size+1),io=size+1)
        stream.seek(0);raw=stream.read(size+1)
    if len(raw)!=size:_fail('budget_exceeded','native_result_bound')
    return raw


def _native_manifest(root, reference, ledger):
    from qcl_negf_contracts.results import validate_manifest
    ledger.results('original manifest full FileRef')
    size=int(reference['bytes'])
    if size>_NATIVE_OPERATION_METADATA//8:_fail('budget_exceeded','native_manifest_bound')
    # Open every component relative to the previous directory FD. No path
    # component (including ancestry) may be replaced by a followed symlink.
    parts=Path(root).parts
    if not Path(root).is_absolute():_fail('source_identity_mismatch','native_root_absolute')
    directory=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        for part in (*parts[1:],*_relative(reference['path']).split('/')[:-1]):
            nxt=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=directory)
            os.close(directory);directory=nxt
        fd=os.open(reference['path'].split('/')[-1],os.O_RDONLY|os.O_NOFOLLOW,dir_fd=directory)
        with os.fdopen(fd,'rb') as stream:
            before=os.fstat(stream.fileno())
            if not stat.S_ISREG(before.st_mode) or before.st_size!=size:_fail('source_identity_mismatch','native_manifest_stat')
            raw=stream.read(size+1);after=os.fstat(stream.fileno())
            current=os.stat(reference['path'].split('/')[-1],dir_fd=directory,follow_symlinks=False)
            stamp=lambda s:(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
            if stamp(before)!=stamp(after) or stamp(before)!=stamp(current) or len(raw)!=size or _sha(raw)!=reference['sha256']:
                _fail('source_identity_mismatch','native_manifest_original_bytes')
    finally:os.close(directory)
    return validate_manifest(_native_decode(raw,_NATIVE_OPERATION_METADATA,ledger,'original manifest decoding'))


@contextmanager
def _native_final_reader(reader):
    """Close a public final reader once without replacing a primary error."""
    try:
        yield reader
    except BaseException:
        try:reader.close()
        except BaseException:pass
        raise
    else:reader.close()


def _native_contexts(root, source, plan_ref, model_ref, args, ledger):
    from qcl_negf_results.state import StateReader,ResultReadLimits
    from qcl_negf_results.final_observations import describe_checkpoint_inputs,FinalObservationsReader
    from qcl_negf_results.final_diagnostics import FinalDiagnosticsReader
    from qcl_negf_contracts.outward import validate_outward_native_source
    state=source['selected_state'];manifest=_native_manifest(root,state['manifest'],ledger)
    limits=ResultReadLimits(maximum_metadata_bytes=_NATIVE_OPERATION_METADATA,maximum_io_bytes=_NATIVE_OPERATION_IO,
        maximum_catalog_pages=16,maximum_catalog_entries=256,maximum_catalog_bytes=_NATIVE_OPERATION_METADATA,
        maximum_open_files=4,maximum_record_bytes=65536,maximum_batch_bytes=65536,maximum_batch_records=16,
        maximum_hdf_objects=4096,maximum_hdf_depth=32,maximum_array_bytes=65536)
    ledger.results('StateReader construction')
    reader=StateReader(root,state_ref=state,expected_identity=state['identity'],limits=limits)
    # Preserve a primary exception/cancellation if close also fails.
    try:
        ledger.results('StateReader source')
        selected=reader.source
        ledger.results('StateReader checkpoint')
        checkpoint=reader.checkpoint()
        ledger.results('describe_checkpoint_inputs')
        cp=describe_checkpoint_inputs(reader,plan_ref=plan_ref,model_ref=model_ref)
        if (cp.plan_raw!=args['frozen_plan'] or cp.plan_ref!=plan_ref or cp.model_ref!=model_ref
                or manifest['provenance']!=args['source_graph']['value']
                or cp.model['configuration_hash']!=state['identity']['model_sha256']):
            _fail('source_identity_mismatch','native_original_plan_model_graph')
        plan=scientific_plan(_native_decode(args['frozen_plan'],65536,ledger,'selected original plan decoding'))
        points=[p for p in plan['points'] if p['id']==state['identity']['point_id'] and p['execution_id']==args['selection']['execution_id']]
        if len(points)!=1 or points[0]['effective_inputs'].get('model_sha256')!=cp.model['configuration_hash']:
            _fail('source_identity_mismatch','native_effective_model')
        diagnostics=observations=final_plan=final_model=None
        if source['kind']=='final':
            ledger.results('FinalDiagnosticsReader construction')
            with _native_final_reader(FinalDiagnosticsReader(reader,artifact_ref=source['diagnostics'])) as final:
                ledger.results('FinalDiagnosticsReader describe')
                diagnostic=final.describe()
            ledger.results('FinalObservationsReader construction')
            with _native_final_reader(FinalObservationsReader(reader,artifact_ref=source['observations'])) as final:
                ledger.results('FinalObservationsReader describe')
                observation=final.describe()
            diagnostics=diagnostic.declaration;observations=observation.declaration
            final_plan=observation.plan;final_model=observation.model
            if observation.invocation!=selected.invocation or diagnostic.invocation!=selected.invocation:
                _fail('source_identity_mismatch','native_final_invocation')
        if observations is not None and observations.get('source_mode')=='restored':
            _fail('source_unavailable','qualified_immediate_parent_context')
        context=dict(source=source,manifest=manifest,manifest_ref=state['manifest'],checkpoint=checkpoint,
            diagnostics=diagnostics,observations=observations,plan=final_plan,model=final_model,parent_manifest=None,parent_checkpoint=None)
        validate_outward_native_source(source,**{k:v for k,v in context.items() if k!='source'})
        ledger.reserve('detached selected context',metadata=_native_weight(context))
        detached=deepcopy(context)
    except BaseException:
        try:reader.close()
        except BaseException:pass
        raise
    else:reader.close()
    return detached


def _load_native_binding_inputs(child: orm.CalcJobNode, raw_result: bytes, *,
                                maximum_metadata_bytes: int, maximum_io_bytes: int) -> dict:
    ledger=_native_credit(maximum_metadata_bytes,maximum_io_bytes)
    return _load_native_binding_inputs_with_ledger(child,raw_result,ledger)


def _load_native_binding_inputs_with_ledger(child,raw_result,ledger):
    from qcl_negf_contracts.outward import validate_outward_result
    from .native_backend import open_native_selected_sources
    if type(raw_result) is not bytes:_fail('corrupt_result','native_raw_bytes_required')
    ledger.reserve('borrowed original result/decoded workspace',metadata=8*len(raw_result))
    if _native_result(child,ledger)!=raw_result:_fail('source_identity_mismatch','native_retrieved_raw')
    args=_native_inputs(child,ledger)
    result=validate_outward_result(_native_decode(raw_result,1024*1024,ledger,'outward result decoding'))
    rows=result['points']+result.get('attempt_history',[])
    ledger.reserve('A selected access full lifetime',metadata=8*1024*1024,io=64*1024*1024)
    contexts=[];missing=[]
    with open_native_selected_sources(child.uuid,maximum_metadata_bytes=8*1024*1024,maximum_io_bytes=64*1024*1024) as (original,sources,technical):
        if original!=raw_result or technical is not None:_fail('source_identity_mismatch','native_access_result_or_technical')
        # First-seen selected root/StateRef order is mandatory; no digest search,
        # newest lookup or coverage loss. Relation is not a creator selector.
        unique=[]
        for row in rows:
            source=row['data']['native_source']
            if source['kind']=='absent':continue
            if not any(source['artifact_root']==s['artifact_root'] and source['selected_state']==s['selected_state'] for s in unique):unique.append(source)
        if len(sources)!=len(unique):_fail('source_identity_mismatch','native_source_coverage')
        for provided,source in zip(sources,unique):
            root,actual,plan_ref,model_ref=provided
            if actual!=source:_fail('source_identity_mismatch','native_source_order_or_ref')
            contexts.append(_native_contexts(root,source,plan_ref,model_ref,args,ledger))
        if unique:
            # Original current INPUT/CALL and selected byte access are verified
            # above, but none carries an immutable invocation -> CalcJob origin
            # relation. A row relation/counter/publisher cannot fill that gap.
            # parent_binding has no owning public input in the current frontier;
            # do not invent one or drop this row to make the binder succeed.
            for row in rows:
                if row['data']['native_source']['kind']!='absent':
                    missing.append(dict(execution_id=row['execution_id'],point_id=row['id'],point_attempt=row['attempt'],
                        operand='original_invocation_calcjob_origin',invocation=next(c['manifest']['invocation'] for c in contexts
                            if c['source']['selected_state']==row['data']['native_source']['selected_state'])))
    # A must finish its successful close/readback before any output constructor.
    if missing:_fail('creator_proof_unavailable',missing)
    args.update(raw_result=raw_result,source_contexts=(),creator_proofs=(),maximum_metadata_bytes=_NATIVE_OPERATION_METADATA)
    ledger.reserve('detached binder arguments',metadata=_native_weight(args))
    return deepcopy(args)


def _prepare_native_outputs(child: orm.CalcJobNode, raw_result: bytes, *,
                            maximum_metadata_bytes: int, maximum_io_bytes: int) -> tuple[orm.SinglefileData,orm.SinglefileData]:
    import io
    from .science_association import bind_native_result
    from aiida.common.links import LinkType
    ledger=_native_credit(maximum_metadata_bytes,maximum_io_bytes)
    for name in ('result','native_binding','selection'):
        if _native_related(child,LinkType.CREATE,name,ledger,outgoing=True,optional=True) is not None:
            _fail('source_identity_mismatch','native_outputs_already_exist')
    args=_load_native_binding_inputs_with_ledger(child,raw_result,ledger)
    ledger.reserve('pure binder full allowance',metadata=_NATIVE_OPERATION_METADATA)
    binding=bind_native_result(**args)
    binding.update(schema='qcl-negf.aiida-native-binding.v1',integrity=dict(_NATIVE_INTEGRITY))
    ledger.reserve('binding encoding/output constructors/readback',metadata=3*_native_weight(binding)+4*len(raw_result),io=2*_NATIVE_OPERATION_METADATA)
    binding_raw=_canonical(binding)
    return (orm.SinglefileData(file=io.BytesIO(raw_result),filename='series_result.json'),
            orm.SinglefileData(file=io.BytesIO(binding_raw),filename='native-binding.json'))


def _native_readback(child,expected_result,expected_binding,ledger):
    from aiida.common.links import LinkType
    from .science_association import bind_native_result
    args=_native_inputs(child,ledger)
    originals={}
    for name,expected in (('result',expected_result),('native_binding',expected_binding)):
        node=_native_related(child,LinkType.CREATE,name,ledger,outgoing=True)
        raw=_native_file(node,ledger,maximum=min(1024*1024,ledger.metadata//8))
        if _native_pin(node,raw)!=expected:_fail('source_identity_mismatch','native_pair_original_node_bytes')
        originals[name]=(node,raw)
    raw=originals['result'][1];binding=_native_decode(originals['native_binding'][1],1024*1024,ledger,'retained binding decoding')
    if _native_result(child,ledger)!=raw:_fail('source_identity_mismatch','native_pair_retrieved_raw')
    # Repeated offline verification cannot renew source access or invocation
    # authority. Only absent-source projection has a complete current proof.
    from qcl_negf_contracts.outward import validate_outward_result
    result=validate_outward_result(_native_decode(raw,1024*1024,ledger,'retained original result decoding'))
    if any(r['data']['native_source']['kind']!='absent' for r in result['points']+result.get('attempt_history',[])):
        _fail('creator_proof_unavailable','original_invocation_calcjob_origin')
    args.update(raw_result=raw,source_contexts=(),creator_proofs=(),maximum_metadata_bytes=_NATIVE_OPERATION_METADATA)
    ledger.reserve('offline pure binder full allowance',metadata=_NATIVE_OPERATION_METADATA)
    expected=bind_native_result(**args)
    expected.update(schema='qcl-negf.aiida-native-binding.v1',integrity=dict(_NATIVE_INTEGRITY))
    ledger.reserve('offline comparison encodings',metadata=2*_native_weight(expected))
    if not _same(binding,expected):_fail('source_identity_mismatch','native_binding_original_projection')
    selection=dict(calcjob_uuid=child.uuid,execution_id=args['selection']['execution_id'],attempt=args['selection']['attempt'],
        result_sha256=_sha(raw),commits={},recovery_commit_sha256=None,native_result=expected_result,native_binding=expected_binding,
        performance_status='unavailable',performance_reason='native_technical_context_unavailable')
    return selection,binding,raw,args['selection']


def _readback_native_outputs(child: orm.CalcJobNode, *, expected_result: dict,
                             expected_binding: dict, maximum_metadata_bytes: int) -> dict:
    ledger=_native_credit(maximum_metadata_bytes,_NATIVE_IO)
    selection,_,_,_=_native_readback(child,expected_result,expected_binding,ledger)
    ledger.reserve('detached scalar selection',metadata=_native_weight(selection))
    return deepcopy(selection)


def _retained_native_result_source(child: orm.CalcJobNode, *, point_id: str,
                                    binding_uuid: str | None = None, maximum_metadata_bytes: int) -> dict:
    from aiida.common.links import LinkType
    ledger=_native_credit(maximum_metadata_bytes,_NATIVE_IO)
    node=_native_related(child,LinkType.CREATE,'selection',ledger,outgoing=True)
    if not isinstance(node,orm.Dict):_fail('source_identity_mismatch','native_scalar_selection_type')
    ledger.reserve('selection Dict decoding',metadata=_NATIVE_OPERATION_METADATA)
    pin=node.get_dict()
    if binding_uuid is not None and pin.get('native_binding',{}).get('node_uuid')!=binding_uuid:
        _fail('source_identity_mismatch','native_binding_expectation')
    selection,binding,raw,_=_native_readback(child,pin.get('native_result'),pin.get('native_binding'),ledger)
    if pin!=selection:_fail('source_identity_mismatch','native_scalar_selection_changed')
    rows=[r for r in binding['points'] if r['point_id']==point_id]
    if len(rows)!=1:_fail('source_identity_mismatch','native_exact_selected_point')
    ledger.reserve('offline response encoding/copies',metadata=2*_native_weight(binding)+8*len(raw))
    return dict(status='available',point=deepcopy(rows[0]),binding=deepcopy(binding),result_utf8=raw.decode('utf-8'),
        native_result=deepcopy(selection['native_result']),native_binding=deepcopy(selection['native_binding']),
        integrity=dict(native_contexts='not_reverified',current_access='not_checked',scientific_acceptance='not_assessed'))
