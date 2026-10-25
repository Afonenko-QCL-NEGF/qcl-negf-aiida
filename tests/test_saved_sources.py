"""Literal L1 provenance/store controls; no profile, transport or numerical solve."""
from io import BytesIO
import hashlib
import json
from types import SimpleNamespace as NS
from uuid import UUID

import pytest
from aiida_qcl_negf import saved_sources as saved
from aiida_qcl_negf import service

RUN, CHILD, PLAN, RESULT, REMOTE, COMPUTER, REQUEST = [str(UUID(int=i)) for i in range(1, 8)]
PIN = str(UUID(int=9))
FP = "a"*64

def raw(value): return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
def digest(value): return hashlib.sha256(value).hexdigest()


class Outputs(dict):
    def __getattr__(self, key):
        try: return self[key]
        except KeyError as error: raise AttributeError(key) from error


@pytest.fixture
def source(tmp_path, monkeypatch):
    nodes, constructed, stores = {}, [], []
    fault = {"after_store": False, "before_store": False}
    class Dict:
        def __init__(self):
            self.uuid = str(UUID(int=100+len(constructed))); self.values = {}; constructed.append(self)
        def set_dict(self, value): self.values = value
        def get_dict(self): return self.values.copy()
        def store(self):
            stores.append(self.uuid)
            if fault["before_store"]: raise RuntimeError("unknown before commit")
            nodes[self.uuid] = self
            if fault["after_store"]: raise RuntimeError("unknown after commit")
            return self
    class Query:
        def append(self, cls, filters): self.filters=filters; return self
        def limit(self, count): self.count=count; return self
        def all(self, flat):
            return [n for n in nodes.values() if all(n.values.get(k.removeprefix("attributes."))==v for k,v in self.filters.items())][:self.count]
    monkeypatch.setattr(saved.orm, "Dict", Dict)
    monkeypatch.setattr(saved.orm, "QueryBuilder", Query)
    monkeypatch.setattr(saved.orm, "load_node", lambda uuid: nodes[uuid])
    saved._pending.clear()
    class File:
        def __init__(self, uuid, payload): self.uuid=uuid; self.payload=payload
        def open(self, mode): assert mode=="rb"; return BytesIO(self.payload)
    plan={"schema":"qcl-negf-scientific-plan-v2","root_kind":"study","root_definition_id":"D","name":"D",
          "fingerprint":FP,"scientific_fingerprint":"b"*64,"computation_count":1,"inclusions":[],
          "executions":[{"id":"E","definition_id":"D","variant_id":"base","point_ids":["P"]}],
          "points":[{"id":"P","execution_id":"E","temperature_K":70.,"voltage_per_period_V":0.,"branch":"B","order":1}]}
    identity={"point_id":"P","execution_id":"E","attempt":2,"plan_fingerprint":FP,"state_id":"state-P","state_sequence":1}
    commit={"schema":"qcl-negf.artifact-commit.v2","contract_set":"qcl-negf.results.v1","generation":1,
            "identity":identity,"state_id":"state-P","state_sequence":1,"storage_class":"archive",
            "quality":"unconverged","terminal_status":"failed","artifacts":[
                {"path":"analysis.h5","role":"physics.analysis","schema":"qcl-negf-physics-analysis-v4","media_type":"application/x-hdf5",
                 "profile":"science","dependencies":[],"sha256":"c"*64,"bytes":37}]}
    commit_bytes=raw(commit)
    receipt={"schema":"qcl-negf-recovery-receipt-v1","status":"verified","publication_scope":"local_filesystem",
             "commit_sha256":digest(commit_bytes),"identity":identity,"state_id":"state-P","state_sequence":1}
    series={"schema":"qcl-negf-series-result-v3","contract_set":"qcl-negf.results.v1","plan_fingerprint":FP,
            "selected_execution_id":"E","status":"failed","points":[{"id":"P","execution_id":"E","attempt":2,
                "coordinates":{"temperature_K":70.,"voltage_per_period_V":0.,"branch":"B","order":1},
                "status":"failed","quality":"unconverged","converged":False,"observables":{},
                "data":{"result_commit":"archive/E/P/final/commit.json"}}]}
    folder=tmp_path/"mount"; root=folder/"opaque-worker"/"result"; (root/"archive/E/P/final").mkdir(parents=True)
    payloads={"series_result.json":raw(series),"scientific_plan.json":raw(plan),"archive/E/P/final/commit.json":commit_bytes,
              "archive/E/P/final/receipt.json":raw(receipt)}
    for name,payload in payloads.items(): (root/name).write_bytes(payload)
    plan_node=File(PLAN,raw(plan)); result_node=File(RESULT,payloads["series_result.json"])
    child=NS(uuid=CHILD,is_terminated=True,inputs=NS(plan=plan_node,execution_id=NS(value="E"),attempt=NS(value=2),code=NS(computer=NS(uuid=COMPUTER))),outputs=Outputs())
    remote=NS(uuid=REMOTE,computer=NS(uuid=COMPUTER),get_remote_path=lambda:"/srv/jobs/opaque-worker",creator=child)
    pin={"calcjob_uuid":CHILD,"execution_id":"E","attempt":2,"result_sha256":digest(payloads["series_result.json"]),
         "commits":{"P":{"path":"archive/E/P/final/commit.json","commit_sha256":digest(commit_bytes),
             "identity":identity,"state_id":"state-P","state_sequence":1,"receipt_status":"verified"}},
         "recovery_commit_sha256":None}
    pin_node=NS(uuid=PIN,creator=child,get_dict=lambda:json.loads(json.dumps(pin)))
    result_node.creator=child; child.outputs.update(remote_folder=remote,result=result_node,selection=pin_node)
    run=NS(uuid=RUN,inputs=NS(plan=plan_node))
    monkeypatch.setattr(service,"_node",lambda uuid: run)
    def selected(uuid,execution,*,attempt,calcjob_uuid,**kwargs):
        if (uuid,execution,attempt,calcjob_uuid)!=(RUN,"E",2,CHILD): raise ValueError("foreign graph")
        return child,2,CHILD
    monkeypatch.setattr(service,"_export_child",selected)
    binding={"root":str(folder),"remote_root":"/srv/jobs","storage_identity":"measured-disk-A","computer_uuid":COMPUTER,
             "registration_source_id":"admin-source-1","admissions":{CHILD:{"run_uuid":RUN,"execution_id":"E","attempt":2,
             "remote_folder_uuid":REMOTE,"relative_result_root":"opaque-worker/result","scheduler_stopped":True,"writers_stopped":True,
             "hold_ref":"hold-A","access_binding_ref":"access-A","retention_policy_id":"indefinite-A","retention_owner_id":"owner-A",
             "storage_identity":"measured-disk-A","maximum_read_bytes":1024*1024}}}
    saved.configure_saved_sources({"jobs":binding})
    kwargs=dict(attempt=2,calcjob_uuid=CHILD,request_uuid=REQUEST,namespace_id="jobs",retention_policy_id="indefinite-A",
                expected_result_sha256=digest(payloads["series_result.json"]),expected_result_bytes=len(payloads["series_result.json"]),maximum_metadata_bytes=1024*1024)
    return NS(nodes=nodes,constructed=constructed,stores=stores,fault=fault,root=root,child=child,run=run,binding=binding,kwargs=kwargs,payloads=payloads,series=series,pin=pin)


def register(source, **change): return saved.register_saved_source(RUN,"E",**{**source.kwargs,**change})


def test_closed_negative_exact_series_and_retained_metadata_survive_storage_outage(source, monkeypatch):
    receipt=register(source)
    assert receipt["status"]=="stored" and len(source.constructed)==len(source.stores)==1
    assert receipt["selection"]=={"run_uuid":RUN,"execution_id":"E","attempt":2,"calcjob_uuid":CHILD}
    monkeypatch.setattr(saved,"_nfs_read",lambda *a,**k:pytest.fail("brief touched NFS"))
    data=saved.get_saved_source(RUN,"E",attempt=2,calcjob_uuid=CHILD,source_uuid=receipt["source_uuid"],expected_source_sha256=receipt["source_sha256"])
    assert data["record"]["snapshot"]["result_sha256"]==source.kwargs["expected_result_sha256"]
    assert data["metadata"]["series_result.json"].encode()==source.payloads["series_result.json"]
    point=data["record"]["points"][0]
    assert point["commit_sha256"]==digest(source.payloads["archive/E/P/final/commit.json"])
    assert point["recorded_statuses"]["status"]=="failed"
    assert data["record"]["integrity"]["payload"]=="not_measured"
    assert next(m for m in data["record"]["metadata"] if m["path"]=="series_result.json")["retained_node_uuid"]==RESULT


@pytest.mark.parametrize("missing",["retained","nfs"])
def test_no_series_before_any_data_constructor_even_with_valid_commit(source, missing):
    # A valid closed stop acknowledgement still cannot replace series bytes.
    (source.root/"stop-receipt.json").write_bytes(raw({"schema":"qcl-negf-stop-receipt-v1","status":"failed",
        "execution_id":"E","attempt":2,"result_sha256":source.kwargs["expected_result_sha256"],"publication_scope":"local_filesystem"}))
    if missing=="retained": del source.child.outputs["result"]
    else: (source.root/"series_result.json").unlink()
    result=register(source)
    assert result["reason"]=="source_series_required"
    assert result["detail"]=="no_series_registration_deferred"
    assert source.constructed==source.stores==[]


@pytest.mark.parametrize("change",[{"expected_result_sha256":"d"*64},{"expected_result_bytes":1}])
def test_digest_conflict_not_missing_fallback(source,change):
    assert register(source,**change)["reason"]=="source_identity_mismatch"
    assert not source.constructed


def test_present_corrupt_series_and_symlink_refuse_without_store(source):
    (source.root/"series_result.json").write_bytes(b"{")
    source.child.outputs.result.payload=b"{"
    assert register(source,expected_result_sha256=digest(b"{"),expected_result_bytes=1)["reason"]=="corrupt_result"
    assert not source.constructed
    (source.root/"series_result.json").unlink()
    (source.root/"series_result.json").symlink_to(source.root/"scientific_plan.json")
    assert register(source)["status"]=="rejected"
    assert not source.constructed


def test_replay_conflict_and_unknown_store_fence(source):
    source.fault["after_store"]=True
    first=register(source)
    assert first["status"]=="unknown"
    assert register(source)["status"]=="replayed"
    assert len(source.constructed)==len(source.stores)==1
    assert register(source,retention_policy_id="other")["reason"]=="request_conflict"


def test_unknown_unstored_does_not_retry_constructor(source):
    source.fault["before_store"]=True
    assert register(source)["status"]=="unknown"
    assert register(source)["status"]=="unknown"
    assert len(source.constructed)==len(source.stores)==1


def test_foreign_mapping_and_quiescence_fail_before_reads_and_store(source):
    source.binding["admissions"][CHILD]["scheduler_stopped"]=False
    saved.configure_saved_sources({"jobs":source.binding})
    assert register(source)["reason"]=="source_not_quiescent"
    assert not source.constructed


def test_explicit_source_stays_pinned_and_ambiguity_requires_selection(source):
    first=register(source)
    second=register(source,request_uuid=str(UUID(int=8)))
    assert first["source_uuid"]!=second["source_uuid"]
    with pytest.raises(saved.SourceError) as failure:
        saved.get_saved_source(RUN,"E",attempt=2,calcjob_uuid=CHILD)
    assert failure.value.code=="selection_required"
    pinned=saved.get_saved_source(RUN,"E",attempt=2,calcjob_uuid=CHILD,source_uuid=first["source_uuid"],expected_source_sha256=first["source_sha256"])
    assert pinned["source_uuid"]==first["source_uuid"]
    assert saved.get_saved_sources(RUN,"E",attempt=2,calcjob_uuid=CHILD)["complete"] is True


def test_changed_retained_plan_or_remote_graph_refuses_source_read(source):
    first=register(source)
    source.child.inputs.plan.payload=b"{}"
    with pytest.raises(saved.SourceError):
        saved.get_saved_source(RUN,"E",attempt=2,calcjob_uuid=CHILD,source_uuid=first["source_uuid"])


def test_active_child_and_missing_hold_refuse_before_constructor(source):
    source.child.is_terminated=False
    assert register(source)["reason"]=="source_not_quiescent"
    assert not source.constructed
    source.child.is_terminated=True
    source.binding["admissions"][CHILD]["hold_ref"]=""
    saved.configure_saved_sources({"jobs":source.binding})
    assert register(source)["status"]=="rejected"
    assert not source.constructed


def test_borrowed_resource_pause_keeps_source_attempt_and_null_legacy_relation(source):
    path="archive/E/P/final/commit.json"
    commit=json.loads(source.payloads[path]); commit["identity"]["attempt"]=1
    commit["storage_class"]="recovery"
    commit_bytes=raw(commit); (source.root/path).write_bytes(commit_bytes)
    receipt_path="archive/E/P/final/receipt.json"
    receipt=json.loads(source.payloads[receipt_path]); receipt["identity"]["attempt"]=1
    receipt["commit_sha256"]=digest(commit_bytes); (source.root/receipt_path).write_bytes(raw(receipt))
    series=source.series; point=series["points"][0]; point["status"]="paused"
    point["data"].update(checkpoint_source_attempt=1,pause_reason="resource_pressure",resume_kind="checkpoint",recovery_origin="last_committed_before_resource_pause")
    payload=raw(series); source.child.outputs.result.payload=payload; (source.root/"series_result.json").write_bytes(payload)
    source.pin["result_sha256"]=digest(payload)
    source.pin["commits"]["P"].update(commit_sha256=digest(commit_bytes),identity=commit["identity"])
    registration=register(source,expected_result_sha256=digest(payload),expected_result_bytes=len(payload))
    assert registration["status"]=="stored"
    result=saved.get_saved_source(RUN,"E",attempt=2,calcjob_uuid=CHILD,source_uuid=registration["source_uuid"])
    record=result["record"]["points"][0]
    assert (record["row_attempt"],record["source_attempt"],record["native_identity"]["attempt"])==(2,1,1)
    assert record["source_calcjob_uuid"] is None
    assert record["recorded_statuses"]["source_calcjob_reason"]=="explicit_recovery_relation_absent_or_ambiguous"


@pytest.mark.parametrize("field,value",[("storage_identity","other-disk"),("computer_uuid",str(UUID(int=50)))])
def test_foreign_namespace_identity_does_not_reach_constructor(source,field,value):
    source.binding[field]=value
    saved.configure_saved_sources({"jobs":source.binding})
    assert register(source)["reason"]=="source_identity_mismatch"
    assert not source.constructed


def test_committed_metadata_corruption_and_foreign_selectors_do_not_store(source):
    path=source.root/"archive/E/P/final/receipt.json"
    receipt=json.loads(path.read_bytes()); receipt["commit_sha256"]="e"*64; path.write_bytes(raw(receipt))
    assert register(source)["reason"]=="source_identity_mismatch"
    assert not source.constructed
    assert register(source,calcjob_uuid=str(UUID(int=51)))["reason"]=="source_identity_mismatch"
    assert not source.constructed


def test_configured_root_symlink_rejected_before_retention(source):
    root=source.root.parent.parent
    real=root.with_name("real-mount")
    root.rename(real)
    root.symlink_to(real,target_is_directory=True)
    assert register(source)["reason"]=="source_identity_mismatch"
    assert not source.constructed


def rewrite_commit_and_receipt(source, mutate, *, update_pin=False):
    path="archive/E/P/final/commit.json"
    commit=json.loads(source.payloads[path]); mutate(commit)
    encoded=raw(commit); (source.root/path).write_bytes(encoded)
    receipt=json.loads(source.payloads["archive/E/P/final/receipt.json"])
    receipt.update(commit_sha256=digest(encoded),identity=commit["identity"],state_id=commit.get("state_id"),state_sequence=commit.get("state_sequence"))
    (source.root/"archive/E/P/final/receipt.json").write_bytes(raw(receipt))
    if update_pin:
        source.pin["commits"]["P"].update(commit_sha256=digest(encoded),identity=commit["identity"],
            state_id=commit.get("state_id"),state_sequence=commit.get("state_sequence"))


def test_coherent_replacement_B_rejected_against_independent_retained_pin_A(source):
    original_pin=source.pin["commits"]["P"]["commit_sha256"]
    def change(commit):
        commit["state_id"]=commit["identity"]["state_id"]="state-B"
        commit["artifacts"][0]["sha256"]="d"*64
    rewrite_commit_and_receipt(source,change)
    assert source.pin["commits"]["P"]["commit_sha256"]==original_pin
    assert register(source)["reason"]=="source_identity_mismatch"
    assert source.constructed==source.stores==[]


def test_explicit_foreign_native_calcjob_cannot_be_stamped_current(source):
    rewrite_commit_and_receipt(source,lambda commit:commit["identity"].update(calcjob_uuid=str(UUID(int=999))),update_pin=True)
    assert register(source)["reason"]=="source_identity_mismatch"
    assert source.constructed==source.stores==[]


def test_missing_retained_selection_refuses_without_constructor(source):
    del source.child.outputs["selection"]
    result=register(source)
    assert result["reason"]=="source_selection_required"
    assert source.constructed==source.stores==[]


@pytest.mark.parametrize("storage",[None,"other"])
def test_invalid_storage_class_refuses_before_constructor_even_with_coherent_pin(source,storage):
    def change(commit):
        if storage is None: commit.pop("storage_class")
        else: commit["storage_class"]=storage
    rewrite_commit_and_receipt(source,change,update_pin=True)
    assert register(source)["reason"]=="corrupt_result"
    assert source.constructed==source.stores==[]


def test_overcount_tiny_metadata_admission_precedes_constructor(source,monkeypatch):
    capture=saved._capture
    def extra(*args,**kwargs):
        record,metadata,plan_uuid,result_uuid,plan_raw=capture(*args,**kwargs)
        metadata.update({f"tiny/{i}.json":b"{}" for i in range(1025)})
        return record,metadata,plan_uuid,result_uuid,plan_raw
    monkeypatch.setattr(saved,"_capture",extra)
    assert register(source)["reason"]=="budget_exceeded"
    assert source.constructed==source.stores==[]


def test_saved_selection_pin_rechecked_on_get_without_nfs(source,monkeypatch):
    receipt=register(source)
    source.pin["commits"]["P"]["commit_sha256"]="f"*64
    monkeypatch.setattr(saved,"_nfs_read",lambda *a,**k:pytest.fail("retained GET touched NFS"))
    with pytest.raises(saved.SourceError) as error:
        saved.get_saved_source(RUN,"E",attempt=2,calcjob_uuid=CHILD,source_uuid=receipt["source_uuid"])
    assert error.value.code=="source_identity_mismatch"


@pytest.mark.parametrize("status",["failed","completed"])
def test_stop_status_exactly_matches_failed_series(source,status):
    (source.root/"stop-receipt.json").write_bytes(raw({"schema":"qcl-negf-stop-receipt-v1","status":status,
        "execution_id":"E","attempt":2,"result_sha256":source.kwargs["expected_result_sha256"],"publication_scope":"local_filesystem"}))
    result=register(source)
    assert result["status"]==("stored" if status=="failed" else "rejected")
    assert len(source.constructed)==len(source.stores)==(1 if status=="failed" else 0)


@pytest.mark.parametrize("point,status",[("P","paused"),("Q","paused"),("P","completed")])
def test_pause_exact_point_status_and_source_identity(source,point,status):
    series=source.series; series["status"]="paused"; series["points"][0]["status"]="paused"
    encoded=raw(series); source.child.outputs.result.payload=encoded; (source.root/"series_result.json").write_bytes(encoded)
    source.pin["result_sha256"]=digest(encoded)
    # Current source attempt2 coherent paused state; avoid any source-attempt rewrite.
    (source.root/"pause-receipt.json").write_bytes(raw({"schema":"qcl-negf-pause-receipt-v1","status":status,
        "execution_id":"E","attempt":2,"point_id":point,"commit_path":"archive/E/P/final/commit.json",
        "commit_sha256":source.pin["commits"]["P"]["commit_sha256"],"state_id":"state-P","state_sequence":1,"publication_scope":"local_filesystem"}))
    result=register(source,expected_result_sha256=digest(encoded),expected_result_bytes=len(encoded))
    valid=point=="P" and status=="paused"
    assert result["status"]==("stored" if valid else "rejected")
    assert len(source.constructed)==len(source.stores)==(1 if valid else 0)


@pytest.mark.parametrize("change",[{"result_sha256":"d"*64},{"calcjob_uuid":str(UUID(int=999))},{"attempt":1}])
def test_retained_selection_header_cannot_describe_other_series_or_child(source,change):
    source.pin.update(change)
    assert register(source)["reason"]=="source_identity_mismatch"
    assert source.constructed==source.stores==[]


def test_pause_publication_scope_is_not_inferred(source):
    (source.root/"stop-receipt.json").write_bytes(raw({"schema":"qcl-negf-stop-receipt-v1","status":"failed",
        "execution_id":"E","attempt":2,"result_sha256":source.kwargs["expected_result_sha256"],"publication_scope":"remote_guess"}))
    assert register(source)["reason"]=="source_identity_mismatch"
    assert source.constructed==source.stores==[]
