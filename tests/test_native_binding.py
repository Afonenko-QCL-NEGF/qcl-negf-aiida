"""PA01–PA16: pure declaration joins; fabricated data never grants provenance.

Fixture seed is literal data from published Contracts8d6b6b7 formats, retained
from the accepted P07 S1 design. No private test/package import or profile.
Changing the binder to infer creator from counters, accept RETURN, ignore full
refs/raw bytes, or issue integrity authority must break these tests.
"""
from copy import deepcopy
import hashlib
import json
import struct
from importlib.resources import files

import pytest
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.messages import ContractError, decode, encode
from qcl_negf_contracts.outward import validate_outward_result, validate_outward_native_source
from aiida_qcl_negf import science_association as association


def uid(n):
    return f"00000000-0000-4000-8000-{n:012d}"


def binding(args):
    function = getattr(association, "bind_native_result", None)
    assert callable(function), "missing pure native binder"
    return function(**args)


def node_file(node_uuid, raw):
    return dict(node_uuid=node_uuid, bytes=str(len(raw)), sha256=hashlib.sha256(raw).hexdigest())


def fileref(path, schema, raw=b"literal", media="application/json"):
    return dict(path=path, schema=schema, bytes=str(len(raw)), sha256=hashlib.sha256(raw).hexdigest(), media_type=media)


def fixture(kind="checkpoint", *, native=3, row_attempt=None):
    plan = deepcopy(SEED["plan"])
    plan["points"][0]["effective_inputs"]["model_sha256"] = "a" * 64
    frozen = encode(plan)
    graph = deepcopy(SEED["graph"])
    graph_raw = b" \n" + encode(graph) + b"\n"
    execution = deepcopy(SEED["execution_binding"])
    execution["input_plan"] = node_file(uid(10), frozen)
    execution["source_graph"] = node_file(uid(11), graph_raw)
    args = dict(raw_result=b"", selection=dict(run_uuid=uid(1), execution_id="execution", attempt=2, calcjob_uuid=uid(3)),
        frozen_plan=frozen, plan_node_uuid=uid(10), source_graph=dict(value=graph, raw=graph_raw),
        source_graph_node_uuid=uid(11), execution_binding=execution,
        source_contexts=(), creator_proofs=(), maximum_metadata_bytes=8*1024*1024)
    result = deepcopy(SEED["series"])
    row = result["points"][0]
    row["attempt"] = native if row_attempt is None else row_attempt
    row["data"]["phase_source"] = dict(status="unavailable", session_id=None, current_path=None, reason="not_recorded")
    if kind != "absent":
        ctx = deepcopy(SEED["checkpoint"])
        cp, manifest = ctx["checkpoint"], ctx["manifest"]
        for obj in (cp, manifest):
            obj["identity"].update(attempt=str(native), plan_sha256=hashlib.sha256(frozen).hexdigest())
            obj["invocation"]["source_epoch"] = f"epoch{native}"
        for target in cp["witness_targets"]:
            target["locator"]["source_identity"] = deepcopy(cp["identity"])
            target["locator"]["source_epoch"] = cp["invocation"]["source_epoch"]
        cpref = fileref("checkpoint/current.json", cp["schema"], encode(cp))
        manifest["checkpoint"] = cpref
        for item in manifest["artifacts"]:
            if item["schema"] == cp["schema"]: item.update(cpref)
        manifest["provenance"] = graph
        source = ctx["source"]
        source["selected_state"] = dict(identity=deepcopy(cp["identity"]), state_id=cp["state_id"], state_sequence=cp["state_sequence"],
            manifest=fileref("manifests/current.json", manifest["schema"], encode(manifest)))
        source["relation"] = "current_point_attempt" if row["attempt"] == native else "retained_source"
        source["reason"] = "final_not_published" if source["relation"] == "current_point_attempt" else "last_committed_before_pause"
        ctx["manifest_ref"] = deepcopy(source["selected_state"]["manifest"])
        ctx["row_key"] = dict(execution_id="execution", point_id="point", point_attempt=row["attempt"])
        row["data"]["native_source"] = deepcopy(source)
        proof = deepcopy(SEED["proof"])
        proof["row_key"] = deepcopy(ctx["row_key"])
        proof["origin_row_key"] = deepcopy(ctx["row_key"])
        proof["files"] = [dict(node_uuid=uid(10), raw=frozen), dict(node_uuid=uid(11), raw=graph_raw),
            dict(node_uuid=uid(12), raw=encode(execution))]
        args["source_contexts"], args["creator_proofs"] = (ctx,), (proof,)
    else:
        row["data"]["native_source"] = deepcopy(SEED["series"]["points"][0]["data"]["native_source"])
    result["attempt_history"] = [deepcopy(row)]
    args["raw_result"] = encode(result)
    return args


def validate_context(args):
    """Independent public dependency control: fixtures must be real joined inputs."""
    validate_outward_result(decode(args["raw_result"]))
    for ctx in args["source_contexts"]:
        validate_outward_native_source(ctx["source"], **{k:v for k,v in ctx.items() if k not in ("row_key", "source")})


def test_PA01_absent_returns_detached_draft_without_any_authority():
    args = fixture("absent")
    before = deepcopy(args)
    draft = binding(args)
    assert set(draft) == {"selection", "input_plan", "source_graph", "storage", "raw_result", "points", "attempt_history"}
    assert draft["points"][0]["creator"] is None and draft["points"][0]["source_invocation"] is None
    assert args == before
    draft["storage"]["hold_ref"] = "changed"
    assert args["execution_binding"]["storage"]["hold_ref"] == "fixture-hold"


@pytest.mark.parametrize("native,row_attempt", [(3,3), (1,2)])
def test_PA02_PA04_current_creator_is_not_native_or_row_counter(native, row_attempt):
    args = fixture(native=native, row_attempt=row_attempt)
    validate_context(args)
    draft = binding(args)
    assert draft["selection"]["attempt"] == 2
    assert draft["points"][0]["creator"] == {"calcjob_uuid":uid(3), "binding_uuid":None}
    assert draft["points"][0]["source"]["selected_state"]["identity"]["attempt"] == str(native)


@pytest.mark.parametrize("mutation", ["extra_context", "missing_context", "extra_proof", "verified", "capability", "cp_final", "graph_raw", "plan_raw", "bool", "native_int", "label", "direction", "creator_uuid", "call_label_duplicate", "uuid_conflict", "foreign_epoch", "coordinate", "model", "invocation"])
def test_PA05_to_PA15_refuse_distinguishing_unbound_data(mutation):
    args = fixture()
    ctx, proof = args["source_contexts"][0], args["creator_proofs"][0]
    if mutation == "extra_context": args["source_contexts"] += (deepcopy(ctx),)
    elif mutation == "missing_context": args["source_contexts"] = ()
    elif mutation == "extra_proof": args["creator_proofs"] += (deepcopy(proof),)
    elif mutation == "verified": ctx["verified"] = True
    elif mutation == "capability": proof["capability"] = True
    elif mutation == "cp_final": ctx["diagnostics"] = {}
    elif mutation == "graph_raw": args["source_graph"]["raw"] += b" "
    elif mutation == "plan_raw": args["frozen_plan"] += b" "
    elif mutation == "bool": args["selection"]["attempt"] = True
    elif mutation == "native_int": ctx["source"]["selected_state"]["identity"]["attempt"] = 3
    elif mutation == "label": proof["links"][0]["label"] = "_invalid"
    elif mutation == "direction": proof["links"][2]["source_uuid"], proof["links"][2]["target_uuid"] = proof["links"][2]["target_uuid"],proof["links"][2]["source_uuid"]
    elif mutation == "creator_uuid": proof["creator_calcjob_uuid"] = uid(4)
    elif mutation == "call_label_duplicate": proof["links"].append(deepcopy(proof["links"][0]))
    elif mutation == "uuid_conflict": proof["files"].append(dict(node_uuid=uid(10), raw=b"other"))
    elif mutation == "foreign_epoch": ctx["checkpoint"]["invocation"]["source_epoch"] = "foreign"
    elif mutation == "coordinate":
        result=decode(args["raw_result"]);result["points"][0]["coordinates"]["temperature_K"]=71.;result["attempt_history"]=deepcopy(result["points"]);args["raw_result"]=encode(result)
    elif mutation == "model": ctx["manifest"]["identity"]["model_sha256"] = "f"*64
    elif mutation == "invocation": ctx["source"]["selected_state"]["identity"]["branch_id"] = "foreign"
    with pytest.raises(ContractError): binding(args)


@pytest.mark.parametrize("field", ["path", "bytes", "sha256", "schema", "media_type"])
def test_PA07_full_manifest_ref_is_not_path_or_digest_only(field):
    args=fixture();ref=args["source_contexts"][0]["manifest_ref"]
    ref[field] = {"path":"elsewhere.json","bytes":"1","sha256":"f"*64,"schema":"foreign","media_type":"text/plain"}[field]
    with pytest.raises(ContractError): binding(args)


@pytest.mark.parametrize("bad", [True, 1.0, 0, -1, 1])
def test_PA16_budget_rejects_before_decode(bad):
    args=fixture("absent");args["raw_result"]=b"{";args["maximum_metadata_bytes"]=bad
    with pytest.raises(ContractError): binding(args)


@pytest.mark.parametrize("field", ["selection", "source_contexts", "creator_proofs", "cycle", "depth", "utf8", "nan"])
def test_PA16_exact_types_and_portable_metadata_admission(field):
    args=fixture("absent")
    if field == "selection": args[field] = type("CustomDict", (dict,), {})(args[field])
    elif field in ("source_contexts", "creator_proofs"): args[field] = []
    elif field == "cycle": args["execution_binding"]["storage"]["cycle"] = args["execution_binding"]
    elif field == "depth":
        v={};args["execution_binding"]["storage"]["nested"]=v
        for _ in range(65):v["x"]={};v=v["x"]
    elif field == "utf8": args["execution_binding"]["access_receipt_ref"]="\ud800"
    else: args["execution_binding"]["storage"]["extra"] = float("nan")
    with pytest.raises(ContractError): binding(args)


def ieee(value=0.0):
    return dict(kind="ieee", type="f64", bits=struct.pack(">d", value).hex())


def sym(value):
    return dict(kind="symbol", value=value)


def integer(value):
    return dict(kind="integer", type="Int64", decimal=str(value))


def final_fixture(native=3):
    args=fixture(native=native)
    ctx=args["source_contexts"][0];cp=ctx["checkpoint"];m=ctx["manifest"]
    state=cp["state"]["fields"];scba=state["scba"]["fields"]
    state.update(boundary=sym("final"), continuation=None, status=sym("invalid"),
        final_evidence={"type":"FinalEvidenceSnapshot","fields":dict(
            report={"type":"ConvergenceReport","fields":dict(passed=False,
                metrics={"kind":"dict","entries":[]},messages={"kind":"array","eltype":"String","shape":["1"],"order":"F","values":["manufactured failure"]})},
            original_converged=False,original_status=sym("invalid"),observations={"type":"CoreRecord","fields":{"entries":[]}})})
    cp["history"]={"head":None, **{k:"0" for k in ("entries","records","scba_rows","outer_rows","witness_updates","last_record")}}
    cp["witness_targets"]=[];cp["payloads"]=cp["payloads"][:1]
    def payload_schema(v):
        if type(v) is dict:
            if v.get("kind")=="array" and "file" in v:v["file"]["schema"]="qcl-negf-physics-v5"
            for item in v.values():payload_schema(item)
        elif type(v) is list:
            for item in v:payload_schema(item)
    payload_schema(state);cp["payloads"][0]["schema"]="qcl-negf-physics-v5"
    for name,v in scba["green"]["fields"].items():
        if v.get("kind")=="array":v["shape"]=["3","2","2","2"] if name in ("Gᴿ","Gˡ","Gᵍ","A") else ["3","2"]
    for name in ("published_U","published_n"):state[name]["shape"]=["5"]
    family=deepcopy(scba["embedding"])
    for a in family["fields"].values():a["shape"]=["3","2","2","2"]
    for name in ("embedding","embedding_plus","embedding_minus"):scba[name]=deepcopy(family)
    scba["scattering"]={"kind":"dict","entries":[dict(key=sym("electron_electron"),value=deepcopy(family))]}
    state["family_order"]=[sym("electron_electron"),sym("embedding_plus"),sym("embedding_minus")]
    m["history"]=deepcopy(cp["history"]);m["closed"]=True;m["status"]["process"]=dict(state="failed",exit_code=1,reason="manufactured failure")
    cpref=fileref("checkpoint/final.json",cp["schema"],encode(cp));m["checkpoint"]=cpref
    dr=fileref("analysis/diagnostics.h5","qcl-negf-physics-analysis-v5",media="application/x-hdf5")
    ore=fileref("analysis/observations.h5","qcl-negf-physics-analysis-v5",b"other",media="application/x-hdf5")
    diag=dict(schema="qcl-negf.final-diagnostics.v1",contract_set="qcl-negf.results.v2",
        **{k:deepcopy(cp[k]) for k in ("identity","invocation","state_id","state_sequence")},checkpoint=cpref,
        dimensions=dict(energy="3",momentum="2",basis="2"),solution_metrics=dict(summary_metrics=[],strict_limits=[],approximate_limits=None),
        positivity=dict(status="not_measured",reason="positivity_measurement_failed: manufactured",data=None))
    flags=dict(LO=True,acoustic=True,impurity=True,IFR=True,alloy=False)
    layout=json.loads(files("qcl_negf_contracts").joinpath("core_layout.json").read_text())["graph"]["nodes"]
    def literal_option(name):
        node=layout[name];kind=node["kind"]
        if kind=="bool":return False
        if kind=="symbol":return sym("")
        if kind=="ieee":return ieee()
        if kind=="integer":return dict(kind="integer",type=node["type"],decimal="0")
        if kind=="nothing":return None
        if kind=="union":return literal_option(node["variants"][0])
        if kind=="struct":return dict(type=name,fields={f["name"]:literal_option(f["type"]) for f in node["fields"]})
        raise AssertionError((name,kind))
    context=dict(dimensions=dict(Nz="5",NE="3",Nk="2",Nb="2"),domain_revision=integer(0),discretization_id="a"*64,basis_id="b"*64,
        scales={n:ieee(v) for n,v in [("E0_eV",0.1),("L0_m",1e-9),("J0_A_per_m2",2.)]},
        inputs={n:ieee(v) for n,v in [("F_bias_V_per_m",5e6),("V_period_V",.05),("T_L_K",70.),("T_LO_K",70.),("E_ref_eV",0.),("N_dop_2D_per_m2",2e14),("f_ion",1.),("z0_m",0.),("period_length_m",1e-8)]},
        problem_scattering=dict(type="ScatteringOptions",fields=flags),enabled_scattering=[],selfenergy_families=["electron_electron"],
        physical_models=literal_option("PhysicalModelOptions"),algorithms=deepcopy(state["measurement_context"]["fields"]["algorithms"]),
        energy_shift_discretization="nodal_linear",localization="pzp",boundary_condition="stationary_field_periodic_embedding",
        basis_contract="orthonormal_finite_projected",energy_reference="scaled_E0_plus_E_ref",effective_gauge="k0_ascending_spatial_first_max_real_positive")
    context["inputs"]["g_s"]=integer(2)
    views=dict(current_density_A_per_m2=ieee(-0.),sheet_density_per_m2=ieee(2e14))
    arrays=[("context","x_scaled","Float64",["z"],"1",["z_nm"],["wX"]),
        ("context","wX","Float64",["z"],"1",["z_nm"],["wX"]),
        ("context","energy_scaled","Float64",["E"],"1",["energy_eV"],["wE"]),
        ("context","wE","Float64",["E"],"1",["energy_eV"],["wE"]),
        ("context","momentum_scaled","Float64",["k"],"1",["k_per_nm"],["wk"]),
        ("context","wk","Float64",["k"],"1",["k_per_nm"],["wk"]),
        ("context","z_nm","Float64",["z"],"nm",["z_nm"],["wX"]),
        ("context","energy_eV","Float64",["E"],"eV",["energy_eV"],["wE"]),
        ("context","k_per_nm","Float64",["k"],"nm^-1",["k_per_nm"],["wk"]),
        ("context","state_index","Int64",["state"],"1",["state_index"],[None]),
        ("context","trusted_energy","Bool",["E"],"1",["energy_eV"],["wE"])]
    arrays += [("views",n,"Float64",["z"],"eV",["z_nm"],["wX"]) for n in ("potential_structure_eV","potential_external_eV","potential_hartree_eV","potential_total_eV")]
    arrays += [("views","density_per_m3","Float64",["z"],"m^-3",["z_nm"],["wX"]),
        ("views","spatial_energy_density_per_eV_m3","Float64",["E","z"],"eV^-1 m^-3",["energy_eV","z_nm"],["wE","wX"]),
        ("views","spectral_trace_per_eV","Float64",["E","k"],"eV^-1",["energy_eV","k_per_nm"],["wE","wk"]),
        ("views","occupied_trace_per_eV","Float64",["E","k"],"eV^-1",["energy_eV","k_per_nm"],["wE","wk"])]
    arrays += [("views",n,"Float64",["state"],"eV",["state_index"],[None]) for n in ("localized_levels_eV","effective_levels_eV")]
    arrays += [("views",n,"ComplexF64",["z","state"],"nm^-1/2",["z_nm","state_index"],["wX",None]) for n in ("localized_wavefunctions_per_sqrt_nm","effective_wavefunctions_per_sqrt_nm")]
    arrays += [("views",n,"ComplexF64",["state","state_prime"],"1" if n=="effective_transform" else "m^-2",["state_index","state_index"],[None,None]) for n in ("effective_transform","sheet_density_matrix_per_m2","effective_density_matrix_per_m2")]
    sizes=dict(z="5",E="3",k="2",state="2",state_prime="2")
    for owner,name,eltype,axes,units,coords,weights in arrays:
        (context if owner=="context" else views)[name]=dict(dataset="/final_observations/arrays/"+name,eltype=eltype,shape=[sizes[a] for a in axes],order="F",axes=axes,units=units,coordinate_fields=coords,weight_fields=weights)
    model=dict(schema="qcl-negf-resolved-configuration-v3",contract_set="qcl-negf.results.v2",configuration_hash="a"*64,hash_encoding="qcl-negf-canonical-bytes-v1",
        configuration=dict(scope="stationary_transport_inputs_v1",inputs={n:{} for n in ("physical","numerical","scales","solver","kernel_construction","algorithms","physical_models","domain_adaptation")},derived=dict(effective_seed_eV=.01)))
    model["configuration"]["inputs"].update(scattering=flags,production=dict(physics_markers={},verify_fft_roundoff=False))
    pr=fileref("plans/frozen.json","qcl-negf-scientific-plan-v2",args["frozen_plan"]);mr=fileref("models/actual.json",model["schema"],encode(model))
    def components(f):return {a:deepcopy(f["fields"][b]) for a,b in [("retarded","Σᴿ"),("lesser","Σˡ"),("greater","Σᵍ")]}
    obs=dict(schema="qcl-negf.final-observations.v1",contract_set="qcl-negf.results.v2",**{k:deepcopy(cp[k]) for k in ("identity","invocation","state_id","state_sequence")},
        checkpoint=cpref,diagnostics=dr,source_mode="produced",binding=dict(identity=deepcopy(state["identity"]),terminal_integrity=deepcopy(state["integrity"])),context=context,
        request=dict(kind="scientific_plan_v2",plan=pr,model=mr,temperature_K=ieee(70.),voltage_per_period_V=ieee(.05),scattering_requested=flags,
            coordinate_checks={n:dict(status="verified",reason=None,requested=ieee(v),actual=ieee(v)) for n,v in [("temperature",70.),("voltage",.05)]}),
        state_bindings=dict(green={a:deepcopy(scba["green"]["fields"][b]) for a,b in [("retarded","Gᴿ"),("lesser","Gˡ"),("greater","Gᵍ"),("spectral","A")]},
            scattering=[dict(family="electron_electron",**components(family))],embedding={a:components(scba[b]) for a,b in [("total","embedding"),("plus","embedding_plus"),("minus","embedding_minus")]}),views=views,
        summary=dict(status="invalid",quality="invalid",converged=False,registry_version="literal-registry",iterative_converged=False,fixed_hartree_converged=False,physical_gates_passed=False,stationary_candidate_accepted=False,scientific_accepted=None,discretization_verified="not_measured",scientific_acceptance_reason="No independent discretization evidence",outer_confirmation_count=integer(0)),restoration=None,
        issues=[dict(target="positivity",status="not_measured",reason="positivity_measurement_failed",detail="manufactured")])
    m["artifacts"]=[dict(cp["payloads"][0],role="physics.full",profile="full-state",dependencies=[]),dict(cpref,role="recovery",profile="full-state",dependencies=[cp["payloads"][0]["path"]]),
        dict(dr,role="physics.analysis",profile="science",dependencies=[cpref["path"]]),dict(pr,role="plan",profile="science",dependencies=[]),dict(mr,role="model",profile="science",dependencies=[]),
        dict(ore,role="physics.analysis",profile="science",dependencies=[r["path"] for r in (cpref,dr,pr,mr)])]
    for name in ("convergence","physics"):m["status"][name]=dict(state="recorded",reason=None,evidence=ore)
    source=ctx["source"];source.update(kind="final",diagnostics=dr,observations=ore,reason=None)
    ctx.update(diagnostics=diag,observations=obs,plan=decode(args["frozen_plan"]),model=model)
    source["selected_state"]["manifest"]=fileref("manifests/final.json",m["schema"],encode(m));ctx["manifest_ref"]=deepcopy(source["selected_state"]["manifest"])
    result=decode(args["raw_result"])
    for col in ("points","attempt_history"):result[col][0]["data"]["native_source"]=deepcopy(source)
    args["raw_result"]=encode(result)
    return args


def retained_fixture(*, final=False):
    args=final_fixture(1) if final else fixture(native=1,row_attempt=1)
    result=decode(args["raw_result"])
    if final:
        for col in ("points","attempt_history"):result[col][0]["status"]="completed_with_warnings"
        args["raw_result"]=encode(result)
    proof=args["creator_proofs"][0]
    origin_selection=dict(args["selection"],calcjob_uuid=uid(4),attempt=1)
    raw=args["raw_result"];source=result["points"][0]["data"]["native_source"]
    # Independent expected producer declaration, never derived with binder helpers.
    br=dict(point_id="point",point_attempt=1,source=deepcopy(source),source_invocation=dict(source_epoch="epoch1"),
        creator=dict(calcjob_uuid=uid(4),binding_uuid=None),plan_point_id="point",coordinates=deepcopy(result["points"][0]["coordinates"]))
    stored=dict(schema="qcl-negf.aiida-native-binding.v1",selection=origin_selection,input_plan=deepcopy(args["execution_binding"]["input_plan"]),
        source_graph=deepcopy(args["execution_binding"]["source_graph"]),storage=deepcopy(args["execution_binding"]["storage"]),
        raw_result={k:v for k,v in node_file(uid(40),raw).items() if k!="node_uuid"},points=[br],attempt_history=[deepcopy(br)],
        integrity=dict(scope="selected_metadata",native_contexts="verified",transport_owner="verified",full_core_payloads="not_assessed",current_access="not_checked"))
    old=deepcopy(proof["processes"][-1]);old.update(uuid=uid(4),attempt_input=dict(node_uuid=uid(34),value=1));proof["processes"].append(old)
    origin_links=[]
    for link in proof["links"]:
        if link["target_uuid"]==uid(3):
            item=deepcopy(link);item["target_uuid"]=uid(4)
            if item["source_uuid"]==uid(32):item["source_uuid"]=uid(34)
            if item["type"]=="call_calc":item["label"]="iteration_01"
            origin_links.append(item)
    proof["links"]+=origin_links+[dict(source_uuid=uid(4),target_uuid=uid(40),type="create",label="result"),
        dict(source_uuid=uid(4),target_uuid=uid(41),type="create",label="native_binding"),dict(source_uuid=uid(41),target_uuid=uid(3),type="input_calc",label="parent_binding")]
    proof.update(mode="retained",creator_calcjob_uuid=uid(4),origin_result_uuid=uid(40),origin_binding_uuid=uid(41),hops=[dict(child_calcjob_uuid=uid(3),parent_binding_uuid=uid(41))])
    proof["files"] += [dict(node_uuid=uid(40),raw=raw),dict(node_uuid=uid(41),raw=encode(stored))]
    return args


@pytest.mark.parametrize("kind", ["final", "retained_checkpoint", "completed_skip"])
def test_PA02_PA03_full_joined_final_and_retained_preserve_negative_science(kind):
    args=final_fixture() if kind=="final" else retained_fixture(final=kind=="completed_skip")
    validate_context(args)
    before=deepcopy(args);draft=binding(args)
    assert args==before and set(draft)=={"selection","input_plan","source_graph","storage","raw_result","points","attempt_history"}
    assert draft["points"][0]["creator"] == {"calcjob_uuid":uid(3) if kind=="final" else uid(4),"binding_uuid":None if kind=="final" else uid(41)}
    if kind=="completed_skip":assert draft["points"][0]["source"]["relation"]=="current_point_attempt" and draft["selection"]["attempt"]==2
    if kind!="retained_checkpoint":assert args["source_contexts"][0]["observations"]["summary"]["scientific_accepted"] is None


@pytest.mark.parametrize("mutation", ["return", "hop", "origin_bytes", "binding_bytes", "origin_owner", "origin_source", "integrity_extra", "missing_parent", "cycle", "foreign_plan", "final_missing", "final_plan_bytes"])
def test_PA08_to_PA14_origin_proof_and_final_context_are_not_authority(mutation):
    args=retained_fixture(final=True);proof=args["creator_proofs"][0]
    if mutation=="return":next(x for x in proof["links"] if x["type"]=="create" and x["label"]=="native_binding")["type"]="return"
    elif mutation=="hop":proof["hops"][0]["child_calcjob_uuid"]=uid(4)
    elif mutation=="origin_bytes":proof["files"][-2]["raw"]+=b" "
    elif mutation=="binding_bytes":proof["files"][-1]["raw"]=b"{}"
    elif mutation in ("origin_owner","origin_source","integrity_extra","foreign_plan"):
        x=decode(proof["files"][-1]["raw"])
        if mutation=="origin_owner":x["selection"]["calcjob_uuid"]=uid(3)
        elif mutation=="origin_source":x["points"][0]["source"]["artifact_root"]="foreign"
        elif mutation=="integrity_extra":x["integrity"]["authority"]=True
        else:x["input_plan"]["node_uuid"]=uid(99)
        proof["files"][-1]["raw"]=encode(x)
    elif mutation=="missing_parent":proof["links"]=[x for x in proof["links"] if x["label"]!="parent_binding"]
    elif mutation=="cycle":proof["hops"]*=2
    elif mutation=="final_missing":args["source_contexts"][0]["observations"]=None
    else:args["source_contexts"][0]["observations"]["request"]["plan"]["bytes"]="1"
    with pytest.raises(ContractError):binding(args)


def completed_restore_fixture():
    args=final_fixture(2);ctx=args["source_contexts"][0];cp=ctx["checkpoint"];m=ctx["manifest"];obs=ctx["observations"]
    parent=deepcopy(cp);pm=deepcopy(m)
    for obj in (parent,pm):
        obj["identity"].update(attempt="1",branch_id="parent-branch");obj["invocation"]["source_epoch"]="epoch-parent"
    pr=fileref("parents/checkpoint.json",parent["schema"],encode(parent));old=pm["checkpoint"]["path"];pm["checkpoint"]=pr
    for item in pm["artifacts"]:
        if item["path"]==old:item.update(pr)
        item["dependencies"]=[pr["path"] if path==old else path for path in item["dependencies"]]
    lineage=dict(identity=deepcopy(parent["identity"]),state_id=parent["state_id"],state_sequence=parent["state_sequence"],manifest=fileref("parents/manifest.json",pm["schema"],encode(pm)))
    for obj in (cp,m):
        obj["lineage"]={"parent_state":deepcopy(lineage)};obj["state_id"]="child-state";obj["state_sequence"]="7"
    for obj in (obs,ctx["diagnostics"]):
        for name in ("identity","invocation","state_id","state_sequence"):obj[name]=deepcopy(cp[name])
    obs["source_mode"]="completed_restore";obs["summary"]["status"]="validation_failed"
    measurement=deepcopy(cp["state"]["fields"]["measurement_context"])
    measurement["fields"].update(backend=sym("production"),map_kernels_shared_with_solver=True,final_state_alignment=sym("restored_aligned"))
    report=deepcopy(parent["state"]["fields"]["final_evidence"]["fields"]["report"])
    obs["restoration"]=dict(parent_state=lineage,parent_checkpoint=pr,history=deepcopy(parent["history"]),
        original=dict(checkpoint=deepcopy(obs["checkpoint"]),field="final_evidence",integrity=deepcopy(obs["binding"]["terminal_integrity"])),
        revalidation=dict(type="RevalidationReceipt",fields=dict(status=sym("measured"),reason=None,detail="",context=measurement,report=report)),
        independent_oracle=dict(type="IndependentOracleReceipt",fields=dict(status=sym("unsupported"),reason=sym("independent_map_not_qualified"),detail="literal no oracle",context=None,report=None)))
    ctx.update(parent_manifest=pm,parent_checkpoint=parent)
    ctx["source"]["selected_state"].update({k:deepcopy(cp[k]) for k in ("identity","state_id","state_sequence")})
    ctx["source"]["selected_state"]["manifest"]=fileref("manifests/child.json",m["schema"],encode(m));ctx["manifest_ref"]=deepcopy(ctx["source"]["selected_state"]["manifest"])
    raw=decode(args["raw_result"])
    for col in ("points","attempt_history"):raw[col][0]["data"]["native_source"]=deepcopy(ctx["source"])
    args["raw_result"]=encode(raw)
    return args


def multihop_fixture():
    args=retained_fixture(final=True);proof=args["creator_proofs"][0]
    args["selection"]["attempt"]=3;proof["processes"][2]["attempt_input"]["value"]=3
    middle=deepcopy(proof["processes"][2]);middle.update(uuid=uid(5),attempt_input=dict(node_uuid=uid(35),value=2));proof["processes"].append(middle)
    extra=[]
    for link in proof["links"]:
        if link["target_uuid"]==uid(3) and link["label"]!="parent_binding":
            item=deepcopy(link);item["target_uuid"]=uid(5)
            if item["source_uuid"]==uid(32):item["source_uuid"]=uid(35)
            extra.append(item)
    proof["links"]+=extra
    next(x for x in proof["links"] if x["label"]=="parent_binding")["target_uuid"]=uid(5)
    proof["links"] += [dict(source_uuid=uid(5),target_uuid=uid(42),type="create",label="result"),dict(source_uuid=uid(5),target_uuid=uid(43),type="create",label="native_binding"),dict(source_uuid=uid(43),target_uuid=uid(3),type="input_calc",label="parent_binding")]
    stored=decode(proof["files"][-1]["raw"]);stored["selection"].update(calcjob_uuid=uid(5),attempt=2)
    for col in ("points","attempt_history"):stored[col][0]["creator"]["binding_uuid"]=uid(41)
    proof["files"] += [dict(node_uuid=uid(42),raw=args["raw_result"]),dict(node_uuid=uid(43),raw=encode(stored))]
    proof["hops"]=[dict(child_calcjob_uuid=uid(3),parent_binding_uuid=uid(43)),dict(child_calcjob_uuid=uid(5),parent_binding_uuid=uid(41))]
    return args


def test_completed_restore_preserves_original_failed_report_and_independent_oracle():
    args=completed_restore_fixture();validate_context(args);out=binding(args)
    assert out["points"][0]["creator"]["calcjob_uuid"]==uid(3)
    obs=args["source_contexts"][0]["observations"]
    assert obs["restoration"]["revalidation"]["fields"]["report"]["fields"]["passed"] is False
    assert obs["restoration"]["independent_oracle"]["fields"]["status"]==sym("unsupported")
    assert obs["summary"]["scientific_accepted"] is None


def test_multihop_parent_chain_retains_original_creator_not_immediate_parent():
    args=multihop_fixture();validate_context(args);out=binding(args)
    assert out["selection"]["attempt"]==3 and out["points"][0]["creator"]==dict(calcjob_uuid=uid(4),binding_uuid=uid(41))


def test_checkpoint_can_conservatively_select_original_final_without_final_authority():
    args=retained_fixture(final=True);ctx=args["source_contexts"][0]
    ctx["source"].update(kind="checkpoint",diagnostics=None,observations=None,reason="final_not_published")
    for k in ("diagnostics","observations","plan","model","parent_manifest","parent_checkpoint"):ctx[k]=None
    result=decode(args["raw_result"])
    for col in ("points","attempt_history"):
        result[col][0]["status"]="failed";result[col][0]["data"]["native_source"]=deepcopy(ctx["source"])
    args["raw_result"]=encode(result);validate_context(args);out=binding(args)
    assert out["points"][0]["source"]["kind"]=="checkpoint" and out["points"][0]["source"]["observations"] is None


@pytest.mark.parametrize("parent", ["missing", "latest", "epoch"])
def test_restoration_requires_exact_original_parent_context(parent):
    args=completed_restore_fixture();ctx=args["source_contexts"][0]
    if parent=="missing":ctx["parent_checkpoint"]=None
    elif parent=="latest":ctx["parent_manifest"]["state_id"]="latest"
    else:ctx["parent_checkpoint"]["invocation"]["source_epoch"]="foreign"
    with pytest.raises(ContractError):binding(args)


def metadata_allowance(args):
    # Independent oracle: actual public encode, not the implementation counter.
    raw_total=0
    def project(v):
        nonlocal raw_total
        if type(v) is bytes:raw_total+=len(v);return None
        if type(v) is dict:return {k:project(x) for k,x in v.items()}
        if type(v) in (tuple,list):return [project(x) for x in v]
        return v
    projection=project(args)
    return raw_total+len(encode(projection))


def test_PA16_exact_utf8_escaping_raw_bytes_and_repeated_snapshots_allowance():
    args=fixture();graph=args["source_graph"]["value"]
    graph["components"][0]["url"]='е́🙂\\"\b\f\n\r\t'
    args["source_graph"]["raw"]=b" \n"+encode(graph)
    args["execution_binding"]["source_graph"]=node_file(uid(11),args["source_graph"]["raw"])
    proof=args["creator_proofs"][0];proof["files"][1]["raw"]=args["source_graph"]["raw"]
    proof["files"][-1]["raw"]=encode(args["execution_binding"])
    ctx=args["source_contexts"][0];ctx["manifest"]["provenance"]=deepcopy(graph)
    ctx["manifest_ref"]=fileref("manifests/current.json",ctx["manifest"]["schema"],encode(ctx["manifest"]))
    ctx["source"]["selected_state"]["manifest"]=deepcopy(ctx["manifest_ref"])
    raw=decode(args["raw_result"])
    for col in ("points","attempt_history"):raw[col][0]["data"]["native_source"]=deepcopy(ctx["source"])
    args["raw_result"]=encode(raw)
    # The budget's own decimal width participates in the projection; stabilize it.
    for _ in range(3):args["maximum_metadata_bytes"]=metadata_allowance(args)
    assert metadata_allowance(args)==args["maximum_metadata_bytes"]
    binding(args)
    args["maximum_metadata_bytes"]-=1
    with pytest.raises(ContractError,match="allowance"):binding(args)


@pytest.mark.parametrize("mutation", ["result_version", "result_extra_execution", "origin_relation", "raw_node", "null_binding", "unrelated_process", "unused_file"])
def test_closed_source_graph_cannot_ignore_extra_or_mismatched_claims(mutation):
    args=retained_fixture();proof=args["creator_proofs"][0]
    if mutation in ("result_version","result_extra_execution"):
        raw=decode(args["raw_result"])
        if mutation=="result_version":raw["schema"]="qcl-negf-series-result-v3"
        else:raw["executions"].append(dict(raw["executions"][0],id="foreign"))
        args["raw_result"]=encode(raw)
    elif mutation=="raw_node":proof["files"][0]["node_uuid"]=uid(99)
    elif mutation=="null_binding":proof["origin_binding_uuid"]=None
    elif mutation=="unrelated_process":proof["processes"].append(dict(proof["processes"][-1],uuid=uid(9)))
    elif mutation=="unused_file":proof["files"].append(dict(node_uuid=uid(99),raw=b"unused"))
    else:
        stored=decode(proof["files"][-1]["raw"])
        for col in ("points","attempt_history"):stored[col][0]["source"]["relation"]="retained_source"
        proof["files"][-1]["raw"]=encode(stored)
    with pytest.raises(ContractError):binding(args)


def test_signed_zero_coordinate_join_uses_original_binary64_not_numeric_equality():
    args=fixture("absent");plan=decode(args["frozen_plan"])
    plan["points"][0]["voltage_per_period_V"]=0.
    plan["points"][0]["effective_inputs"]["voltage_per_period_V"]=0.
    args["frozen_plan"]=encode(plan);args["execution_binding"]["input_plan"]=node_file(uid(10),args["frozen_plan"])
    result=decode(args["raw_result"])
    for col in ("points","attempt_history"):result[col][0]["coordinates"]["voltage_per_period_V"]=-0.
    args["raw_result"]=encode(result)
    with pytest.raises(ContractError,match="binary64"):binding(args)


def test_preflight_refuses_before_decode_or_detach(monkeypatch):
    args=fixture();args["maximum_metadata_bytes"]=1
    def unexpected(*a, **kw):raise AssertionError("decode/copy preceded admission")
    monkeypatch.setattr(association,"decode",unexpected);monkeypatch.setattr(association,"deepcopy",unexpected)
    with pytest.raises(ContractError,match="allowance"):binding(args)


def history_fixture():
    args=fixture();old=fixture(native=1,row_attempt=1);ctx=old["source_contexts"][0]
    ctx["source"]["selected_state"]["manifest"]=fileref("manifests/old.json",ctx["manifest"]["schema"],encode(ctx["manifest"]))
    ctx["manifest_ref"]=deepcopy(ctx["source"]["selected_state"]["manifest"])
    row=decode(old["raw_result"])["points"][0];row["data"]["native_source"]=deepcopy(ctx["source"])
    result=decode(args["raw_result"]);result["attempt_history"].insert(0,row);args["raw_result"]=encode(result)
    args["source_contexts"]=(ctx,)+args["source_contexts"];args["creator_proofs"]+=old["creator_proofs"]
    return args


def test_history_preserves_all_exact_native_sources_under_one_current_creator():
    args=history_fixture();validate_context(args);out=binding(args)
    assert [r["point_attempt"] for r in out["attempt_history"]]==[1,3]
    assert all(r["creator"]==dict(calcjob_uuid=uid(3),binding_uuid=None) for r in out["attempt_history"])


def test_repeated_process_uuid_cannot_have_different_call_snapshot_per_row():
    args=history_fixture();validate_context(args)
    args["creator_proofs"][1]["links"][1]["label"]="different_observed_call"
    with pytest.raises(ContractError):binding(args)


@pytest.mark.parametrize("kind", ["absent", "checkpoint"])
def test_explicit_graph_allowance_does_not_acquire_unrelated_message_default_limit(kind):
    args=fixture(kind)
    args["source_graph"]["raw"]=b" "*(9*1024*1024+1)+encode(args["source_graph"]["value"])
    args["execution_binding"]["source_graph"]=node_file(uid(11),args["source_graph"]["raw"])
    if kind!="absent":
        args["creator_proofs"][0]["files"][1]["raw"]=args["source_graph"]["raw"]
        args["creator_proofs"][0]["files"][-1]["raw"]=encode(args["execution_binding"])
    args["maximum_metadata_bytes"]=24*1024*1024
    out=binding(args)
    assert out["source_graph"]["bytes"]==str(len(args["source_graph"]["raw"]))


# Self-contained manufactured data; no runtime dependency on private design artifacts.
SEED = json.loads(r'''
{
  "plan": {
    "campaign": null,
    "computation_count": 1,
    "executions": [
      {
        "definition_id": "study",
        "effective_inputs": {
          "energy_max_eV": 1.0,
          "energy_min_eV": -1.0,
          "energy_nodes": 3,
          "energy_shift": "none",
          "seed": {}
        },
        "estimated_peak_bytes": 256,
        "id": "execution",
        "label": "",
        "method_id": "method",
        "operation": "stationary",
        "output": {
          "archive": {
            "full_final": true,
            "intermediate_history": 16,
            "optical": false,
            "projections": true
          },
          "recovery": {
            "byte_budget": 8589934592,
            "enabled": true,
            "interval_seconds": 1800.0,
            "reserve_bytes": 67108864,
            "retain_generations": 2
          },
          "telemetry": {
            "buffer_events": 256,
            "enabled": true
          }
        },
        "point_ids": [
          "point"
        ],
        "policies": {},
        "provenance": {},
        "purpose": "diagnostic",
        "repetition": 1,
        "resolved_configuration": {
          "algorithms": {
            "anderson": {
              "damping": 1.0,
              "history_depth": 6,
              "regularization": 1e-12
            },
            "contraction": "dense_blas",
            "energy_shift": "sparse_plan",
            "hilbert": "direct",
            "kernel_build": "direct",
            "localization": "pzp",
            "low_rank": {
              "maximum_rank": 0,
              "relative_tolerance": 0.0
            },
            "mixing": "linear",
            "retarded_real_part": "kramers_kronig",
            "self_energy_structure": "full",
            "transverse_momentum": "resolved"
          },
          "execution": {
            "auto": {
              "blocks_per_worker": 4,
              "energy_chunk_alignment": 8,
              "energy_jobs_per_worker": 2,
              "hilbert_columns_per_worker": 8,
              "large_basis_threshold": 32,
              "memory_reserve_fraction": 0.125,
              "minimum_energy_chunk": 8,
              "minimum_memory_reserve_bytes": 2147483648,
              "minimum_outer_parallel_threads": 4,
              "workspace_budget_fraction": 0.05,
              "workspace_complex_arrays_per_energy_block": 12
            },
            "blas_threads": 1,
            "fail_on_thread_mismatch": false,
            "julia_threads": 0,
            "solver_backend": "production",
            "strategy": "auto_exact"
          },
          "kernel_construction": {
            "angular_validation_pairs": 3,
            "impurity_angular_tolerance": 1e-05,
            "initial_lookup_nodes": 65,
            "lookup_power": 2.0,
            "maximum_lookup_nodes": 513,
            "relative_tolerance": 0.0002,
            "strict": true,
            "validation_fractions": [
              0.25,
              0.5,
              0.75
            ]
          },
          "numerical": {
            "angular_nodes": 8,
            "basis_periods": 1,
            "basis_states": 3,
            "energy_max": "0.45 eV",
            "energy_min": "-0.15 eV",
            "energy_nodes": 49,
            "energy_trust_margin": "80 meV",
            "longitudinal_momentum_max": "10.0 nm^-1",
            "longitudinal_momentum_nodes": 25,
            "momentum_max": "0.60 nm^-1",
            "momentum_nodes": 5,
            "seed_broadening": "0.5 meV",
            "spatial_nodes": 48
          },
          "output": {
            "checkpoint_prefix": "state",
            "debug_hdf5": false,
            "device_geometry": {
              "cavity_length": "1.8 mm",
              "periods": 405,
              "ridge_width": "150 μm"
            },
            "directory": "results/base",
            "fail_fast": false,
            "light_max_energy_points": 512,
            "light_max_momentum_points": 64,
            "light_max_snapshots": 16,
            "light_max_space_points": 2048,
            "live_visualization": false,
            "progress": {
              "dashboard_file": null,
              "enabled": true,
              "event_log_file": "progress/events.csv",
              "human_every": 5,
              "latest_snapshot_file": "progress/latest.csv",
              "significant_digits": 4,
              "terminal": false
            },
            "report_directory": "expert_report",
            "resume": false,
            "save_comparison_csv": true,
            "save_comparison_plots": true,
            "save_csv": true,
            "save_expert_markdown": true,
            "save_full_state": true,
            "save_plots": false,
            "snapshot_every_outer": 1,
            "snapshot_every_scba": 25
          },
          "physical": {
            "acoustic_deformation_potential": "7.0 eV",
            "alloy_potential": null,
            "donor_sheet_density": "4.5e14 m^-2",
            "field_bias": null,
            "high_frequency_relative_permittivity": 10.89,
            "impurity_screening_wavenumber": "0.20 nm^-1",
            "interface_correlation_length": "9.0 nm",
            "interface_roughness_height": "0.10 nm",
            "interfaces": [
              "0 nm",
              "3.26 nm",
              "11.25 nm",
              "13.15 nm"
            ],
            "ionization_fraction": 1.0,
            "lattice_temperature": "70.0 K",
            "layers": [
              {
                "aluminium_fraction": 0.25,
                "conduction_band_edge": "0.20775 eV",
                "doped": false,
                "longitudinal_relative_mass": 0.08775,
                "material": "AlGaAs",
                "relative_permittivity": 12.9,
                "thickness": "3.26 nm",
                "transverse_relative_mass": 0.08775
              },
              {
                "aluminium_fraction": 0.0,
                "conduction_band_edge": "0 eV",
                "doped": false,
                "longitudinal_relative_mass": 0.067,
                "material": "GaAs",
                "relative_permittivity": 12.9,
                "thickness": "7.99 nm",
                "transverse_relative_mass": 0.067
              },
              {
                "aluminium_fraction": 0.25,
                "conduction_band_edge": "0.20775 eV",
                "doped": false,
                "longitudinal_relative_mass": 0.08775,
                "material": "AlGaAs",
                "relative_permittivity": 12.9,
                "thickness": "1.90 nm",
                "transverse_relative_mass": 0.08775
              },
              {
                "aluminium_fraction": 0.0,
                "conduction_band_edge": "0 eV",
                "doped": false,
                "longitudinal_relative_mass": 0.067,
                "material": "GaAs",
                "relative_permittivity": 12.9,
                "thickness": "8.40 nm",
                "transverse_relative_mass": 0.067
              },
              {
                "aluminium_fraction": 0.0,
                "conduction_band_edge": "0 eV",
                "doped": true,
                "longitudinal_relative_mass": 0.067,
                "material": "GaAs",
                "relative_permittivity": 12.9,
                "thickness": "2.90 nm",
                "transverse_relative_mass": 0.067
              },
              {
                "aluminium_fraction": 0.0,
                "conduction_band_edge": "0 eV",
                "doped": false,
                "longitudinal_relative_mass": 0.067,
                "material": "GaAs",
                "relative_permittivity": 12.9,
                "thickness": "5.16 nm",
                "transverse_relative_mass": 0.067
              }
            ],
            "lo_phonon_energy": "36.7 meV",
            "lo_screening_wavenumber": "0.20 nm^-1",
            "lo_temperature": "70.0 K",
            "mass_density": "5317 kg/m^3",
            "position_origin": "0 nm",
            "primitive_cell_volume": null,
            "reference_energy": "0 eV",
            "sound_velocity": "5240 m/s",
            "spin_degeneracy": 2,
            "static_relative_permittivity": 12.9,
            "voltage_per_period": "56 mV"
          },
          "production": {
            "checkpoint_every_outer": 0,
            "checkpoint_every_scba": 0,
            "energy_chunk": 64,
            "hilbert_columns": 32,
            "parallel_backend": "threads",
            "phase_timing": false,
            "progress_every_outer": 1,
            "progress_every_scba": 5,
            "residual_chunk": 256,
            "verify_fft_roundoff": true,
            "worker_count": 0
          },
          "run": {
            "classification": "reference",
            "description": "Канонический паспорт физической модели QCL; запуск задаётся научным исследованием.",
            "name": "resource-proposal-fixture-baseline"
          },
          "scales": {
            "energy": "0.1 eV",
            "length": "10 nm",
            "reference_mass_ratio": 0.067
          },
          "scattering": {
            "acoustic_phonon": true,
            "alloy_disorder": false,
            "interface_roughness": true,
            "ionized_impurity": true,
            "lo_phonon": true
          },
          "solver": {
            "convergence": {
              "diagnostic_quality": {
                "causality": 1e-10,
                "coarse_wait_iterations": 64,
                "enabled": true,
                "keldysh": 0.001,
                "normalization": 0.0001,
                "observables": 0.001,
                "outer_threshold": 0.0001,
                "positivity": 1e-06,
                "required_consecutive_passes": 3,
                "self_energy": 0.001,
                "strict_attempt_iterations": 8,
                "target_fixed_point": 0.0001
              },
              "minimum_poisson_iterations": 2,
              "minimum_scba_iterations": 2,
              "mode": "strict_fail_fast",
              "required_consecutive_poisson_passes": 2,
              "required_consecutive_scba_passes": 3,
              "stagnation_relative_improvement": 0.01,
              "stagnation_window": 200,
              "trend": {
                "max_oscillation": 10.0,
                "max_regression": 2.0,
                "minimum_reduction": 0.5,
                "promising_threshold": 0.0001,
                "window": 32
              }
            },
            "maximum_poisson_iterations": 100,
            "maximum_scba_iterations": 2000,
            "poisson_mixing": 0.15,
            "self_energy_mixing": 0.1,
            "tolerances": {
              "causality": 1e-10,
              "collision": 1e-08,
              "current_continuity": 1e-06,
              "density": 1e-07,
              "dyson": 1e-11,
              "energy_edge": 1e-06,
              "fft_roundoff": 1e-10,
              "hartree": 1e-07,
              "imaginary_part": 1e-10,
              "keldysh": 1e-08,
              "neutrality": 1e-10,
              "normalization": 1e-08,
              "observables": 0.0001,
              "poisson": 1e-11,
              "poisson_gauge": 1e-10,
              "positivity": 1e-10,
              "power": 0.0001,
              "self_energy": 1e-08,
              "spectral_identity": 1e-09,
              "sum_rule": 0.001,
              "tail": 1e-06
            },
            "validation_windows": {
              "energy_tail_fraction": 0.05,
              "momentum_tail_fraction": 0.1
            }
          },
          "study": {
            "calculate_optical_response": false,
            "comparison_profiles": [],
            "convergence": {
              "angular_nodes": [],
              "energy_nodes": [],
              "momentum_nodes": [],
              "spatial_nodes": []
            },
            "methods": [],
            "mode": "single",
            "optical_edge_tolerance": 0.0001,
            "photon_energy_max": "80 meV",
            "photon_energy_min": "1 meV",
            "photon_energy_points": 321,
            "reference_profile": null,
            "repetitions": 1,
            "temperatures": [
              "70.0 K"
            ],
            "voltages_per_period": [
              "0.0 V"
            ]
          }
        },
        "resolved_snapshot": {
          "default_rule_sources": {
            "fixture-rule": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
          },
          "derived": {
            "effective_seed": {
              "clamped": false,
              "effective_eV": 0.001,
              "effective_scaled": 0.01,
              "grid_floor_eV": 0.0,
              "grid_floor_scaled": 0.0,
              "requested_eV": 0.001,
              "requested_scaled": 0.01,
              "rule": "manufactured",
              "working_eta_eV": 0.0
            }
          },
          "model_binding": {
            "encoding": "qcl-negf-canonical-bytes-v1",
            "scope": "stationary_transport_inputs_v1",
            "sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
          },
          "phase": "resolved_declarative_inputs",
          "raw_configuration": {
            "algorithms": {
              "anderson": {
                "damping": 1.0,
                "history_depth": 6,
                "regularization": 1e-12
              },
              "contraction": "dense_blas",
              "energy_shift": "sparse_plan",
              "hilbert": "direct",
              "kernel_build": "direct",
              "localization": "pzp",
              "low_rank": {
                "maximum_rank": 0,
                "relative_tolerance": 0.0
              },
              "mixing": "linear",
              "retarded_real_part": "kramers_kronig",
              "self_energy_structure": "full",
              "transverse_momentum": "resolved"
            },
            "execution": {
              "auto": {
                "blocks_per_worker": 4,
                "energy_chunk_alignment": 8,
                "energy_jobs_per_worker": 2,
                "hilbert_columns_per_worker": 8,
                "large_basis_threshold": 32,
                "memory_reserve_fraction": 0.125,
                "minimum_energy_chunk": 8,
                "minimum_memory_reserve_bytes": 2147483648,
                "minimum_outer_parallel_threads": 4,
                "workspace_budget_fraction": 0.05,
                "workspace_complex_arrays_per_energy_block": 12
              },
              "blas_threads": 1,
              "fail_on_thread_mismatch": false,
              "julia_threads": 0,
              "solver_backend": "production",
              "strategy": "auto_exact"
            },
            "kernel_construction": {
              "angular_validation_pairs": 3,
              "impurity_angular_tolerance": 1e-05,
              "initial_lookup_nodes": 65,
              "lookup_power": 2.0,
              "maximum_lookup_nodes": 513,
              "relative_tolerance": 0.0002,
              "strict": true,
              "validation_fractions": [
                0.25,
                0.5,
                0.75
              ]
            },
            "numerical": {
              "angular_nodes": 8,
              "basis_periods": 1,
              "basis_states": 3,
              "energy_max": "0.45 eV",
              "energy_min": "-0.15 eV",
              "energy_nodes": 49,
              "energy_trust_margin": "80 meV",
              "longitudinal_momentum_max": "10.0 nm^-1",
              "longitudinal_momentum_nodes": 25,
              "momentum_max": "0.60 nm^-1",
              "momentum_nodes": 5,
              "seed_broadening": "0.5 meV",
              "spatial_nodes": 48
            },
            "output": {
              "checkpoint_prefix": "state",
              "debug_hdf5": false,
              "device_geometry": {
                "cavity_length": "1.8 mm",
                "periods": 405,
                "ridge_width": "150 μm"
              },
              "directory": "results/base",
              "fail_fast": false,
              "light_max_energy_points": 512,
              "light_max_momentum_points": 64,
              "light_max_snapshots": 16,
              "light_max_space_points": 2048,
              "live_visualization": false,
              "progress": {
                "dashboard_file": null,
                "enabled": true,
                "event_log_file": "progress/events.csv",
                "human_every": 5,
                "latest_snapshot_file": "progress/latest.csv",
                "significant_digits": 4,
                "terminal": false
              },
              "report_directory": "expert_report",
              "resume": false,
              "save_comparison_csv": true,
              "save_comparison_plots": true,
              "save_csv": true,
              "save_expert_markdown": true,
              "save_full_state": true,
              "save_plots": false,
              "snapshot_every_outer": 1,
              "snapshot_every_scba": 25
            },
            "physical": {
              "acoustic_deformation_potential": "7.0 eV",
              "alloy_potential": null,
              "donor_sheet_density": "4.5e14 m^-2",
              "field_bias": null,
              "high_frequency_relative_permittivity": 10.89,
              "impurity_screening_wavenumber": "0.20 nm^-1",
              "interface_correlation_length": "9.0 nm",
              "interface_roughness_height": "0.10 nm",
              "interfaces": [
                "0 nm",
                "3.26 nm",
                "11.25 nm",
                "13.15 nm"
              ],
              "ionization_fraction": 1.0,
              "lattice_temperature": "70.0 K",
              "layers": [
                {
                  "aluminium_fraction": 0.25,
                  "conduction_band_edge": "0.20775 eV",
                  "doped": false,
                  "longitudinal_relative_mass": 0.08775,
                  "material": "AlGaAs",
                  "relative_permittivity": 12.9,
                  "thickness": "3.26 nm",
                  "transverse_relative_mass": 0.08775
                },
                {
                  "aluminium_fraction": 0.0,
                  "conduction_band_edge": "0 eV",
                  "doped": false,
                  "longitudinal_relative_mass": 0.067,
                  "material": "GaAs",
                  "relative_permittivity": 12.9,
                  "thickness": "7.99 nm",
                  "transverse_relative_mass": 0.067
                },
                {
                  "aluminium_fraction": 0.25,
                  "conduction_band_edge": "0.20775 eV",
                  "doped": false,
                  "longitudinal_relative_mass": 0.08775,
                  "material": "AlGaAs",
                  "relative_permittivity": 12.9,
                  "thickness": "1.90 nm",
                  "transverse_relative_mass": 0.08775
                },
                {
                  "aluminium_fraction": 0.0,
                  "conduction_band_edge": "0 eV",
                  "doped": false,
                  "longitudinal_relative_mass": 0.067,
                  "material": "GaAs",
                  "relative_permittivity": 12.9,
                  "thickness": "8.40 nm",
                  "transverse_relative_mass": 0.067
                },
                {
                  "aluminium_fraction": 0.0,
                  "conduction_band_edge": "0 eV",
                  "doped": true,
                  "longitudinal_relative_mass": 0.067,
                  "material": "GaAs",
                  "relative_permittivity": 12.9,
                  "thickness": "2.90 nm",
                  "transverse_relative_mass": 0.067
                },
                {
                  "aluminium_fraction": 0.0,
                  "conduction_band_edge": "0 eV",
                  "doped": false,
                  "longitudinal_relative_mass": 0.067,
                  "material": "GaAs",
                  "relative_permittivity": 12.9,
                  "thickness": "5.16 nm",
                  "transverse_relative_mass": 0.067
                }
              ],
              "lo_phonon_energy": "36.7 meV",
              "lo_screening_wavenumber": "0.20 nm^-1",
              "lo_temperature": "70.0 K",
              "mass_density": "5317 kg/m^3",
              "position_origin": "0 nm",
              "primitive_cell_volume": null,
              "reference_energy": "0 eV",
              "sound_velocity": "5240 m/s",
              "spin_degeneracy": 2,
              "static_relative_permittivity": 12.9,
              "voltage_per_period": "56 mV"
            },
            "production": {
              "checkpoint_every_outer": 0,
              "checkpoint_every_scba": 0,
              "energy_chunk": 64,
              "hilbert_columns": 32,
              "parallel_backend": "threads",
              "phase_timing": false,
              "progress_every_outer": 1,
              "progress_every_scba": 5,
              "residual_chunk": 256,
              "verify_fft_roundoff": true,
              "worker_count": 0
            },
            "run": {
              "classification": "reference",
              "description": "Канонический паспорт физической модели QCL; запуск задаётся научным исследованием.",
              "name": "resource-proposal-fixture-baseline"
            },
            "scales": {
              "energy": "0.1 eV",
              "length": "10 nm",
              "reference_mass_ratio": 0.067
            },
            "scattering": {
              "acoustic_phonon": true,
              "alloy_disorder": false,
              "interface_roughness": true,
              "ionized_impurity": true,
              "lo_phonon": true
            },
            "solver": {
              "convergence": {
                "diagnostic_quality": {
                  "causality": 1e-10,
                  "coarse_wait_iterations": 64,
                  "enabled": true,
                  "keldysh": 0.001,
                  "normalization": 0.0001,
                  "observables": 0.001,
                  "outer_threshold": 0.0001,
                  "positivity": 1e-06,
                  "required_consecutive_passes": 3,
                  "self_energy": 0.001,
                  "strict_attempt_iterations": 8,
                  "target_fixed_point": 0.0001
                },
                "minimum_poisson_iterations": 2,
                "minimum_scba_iterations": 2,
                "mode": "strict_fail_fast",
                "required_consecutive_poisson_passes": 2,
                "required_consecutive_scba_passes": 3,
                "stagnation_relative_improvement": 0.01,
                "stagnation_window": 200,
                "trend": {
                  "max_oscillation": 10.0,
                  "max_regression": 2.0,
                  "minimum_reduction": 0.5,
                  "promising_threshold": 0.0001,
                  "window": 32
                }
              },
              "maximum_poisson_iterations": 100,
              "maximum_scba_iterations": 2000,
              "poisson_mixing": 0.15,
              "self_energy_mixing": 0.1,
              "tolerances": {
                "causality": 1e-10,
                "collision": 1e-08,
                "current_continuity": 1e-06,
                "density": 1e-07,
                "dyson": 1e-11,
                "energy_edge": 1e-06,
                "fft_roundoff": 1e-10,
                "hartree": 1e-07,
                "imaginary_part": 1e-10,
                "keldysh": 1e-08,
                "neutrality": 1e-10,
                "normalization": 1e-08,
                "observables": 0.0001,
                "poisson": 1e-11,
                "poisson_gauge": 1e-10,
                "positivity": 1e-10,
                "power": 0.0001,
                "self_energy": 1e-08,
                "spectral_identity": 1e-09,
                "sum_rule": 0.001,
                "tail": 1e-06
              },
              "validation_windows": {
                "energy_tail_fraction": 0.05,
                "momentum_tail_fraction": 0.1
              }
            },
            "study": {
              "calculate_optical_response": false,
              "comparison_profiles": [],
              "convergence": {
                "angular_nodes": [],
                "energy_nodes": [],
                "momentum_nodes": [],
                "spatial_nodes": []
              },
              "methods": [],
              "mode": "single",
              "optical_edge_tolerance": 0.0001,
              "photon_energy_max": "80 meV",
              "photon_energy_min": "1 meV",
              "photon_energy_points": 321,
              "reference_profile": null,
              "repetitions": 1,
              "temperatures": [
                "70.0 K"
              ],
              "voltages_per_period": [
                "0.0 V"
              ]
            }
          },
          "schema": "qcl-negf-resolved-configuration-v1",
          "sections": {
            "algorithms": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "domain_adaptation": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "execution": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "kernel_construction": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "numerical": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "output": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "physical": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "physical_models": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "production": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "scales": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "scattering": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "solver": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            },
            "study": {
              "input_assigning_histories": {},
              "origin": "owning_resolution_rule",
              "rule_source": "fixture-rule#parse",
              "used": {
                "example": null
              }
            }
          }
        },
        "resource_estimate": {
          "adaptation_enabled": false,
          "admitted_memory_bytes": null,
          "allocator_overhead_bytes": null,
          "basis": {
            "core_estimator_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "runner_formula_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
          },
          "confidence": "analytical_conservative_uncalibrated",
          "decision_kind": "planning_estimate",
          "declared_cap": {
            "breakdown": {
              "manufactured": 128
            },
            "peak_bytes": 256,
            "resident_bytes": 128
          },
          "declared_maximum_energy_nodes": 3,
          "estimated_numerical_peak_bytes": 256,
          "initial": {
            "breakdown": {
              "manufactured": 128
            },
            "peak_bytes": 256,
            "resident_bytes": 128
          },
          "initial_energy_nodes": 3,
          "measured_rss_bytes": null,
          "retained_resident_bytes": 0,
          "runtime_overhead_bytes": null,
          "schema": "qcl-negf-resource-estimate-v1",
          "status": "available",
          "total_memory_request_bytes": null,
          "unknown": [
            "runtime_overhead",
            "allocator_overhead",
            "native_grant",
            "measured_rss"
          ],
          "worker_capacity": 2
        },
        "variant_id": "variant"
      }
    ],
    "fingerprint": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "inclusions": [],
    "maximum_solver_runs": 1,
    "model_revision": "transport-contract-v2",
    "name": "P07 manufactured declaration only",
    "nodes": [],
    "points": [
      {
        "branch": "forward",
        "effective_inputs": {
          "axes_applied": true,
          "configuration_hash": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
          "hash_encoding": "qcl-negf-canonical-bytes-v1",
          "model_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
          "temperature_K": 70.0,
          "voltage_per_period_V": 0.05
        },
        "execution_id": "execution",
        "id": "point",
        "initialization": "cold",
        "order": 1,
        "predecessor_id": null,
        "temperature_K": 70.0,
        "voltage_per_period_V": 0.05
      }
    ],
    "resolved_snapshot_schema": "qcl-negf-resolved-configuration-v1",
    "resources": {
      "estimated_peak_bytes": 256,
      "maximum_active_executions": 1,
      "minimum_output_bytes": 0
    },
    "root_definition_id": "study",
    "root_kind": "study",
    "schema": "qcl-negf-scientific-plan-v2",
    "science_archive_preflight": null,
    "scientific_fingerprint": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  },
  "series": {
    "attempt_history": [
      {
        "attempt": 1,
        "converged": false,
        "coordinates": {
          "branch": "forward",
          "order": 1,
          "temperature_K": 70.0,
          "voltage_per_period_V": 0.05
        },
        "data": {
          "literal_scalar_extension": "kept",
          "native_source": {
            "artifact_root": ".",
            "diagnostics": null,
            "kind": "absent",
            "observations": null,
            "reason": "solver_not_run",
            "relation": null,
            "schema": "qcl-negf.native-point-source.v1",
            "selected_state": null
          },
          "optical_attachment": null,
          "phase_source": {
            "current_path": "executions/exec/point/attempt-1/phase/phase-current.json",
            "reason": null,
            "session_id": "phase-session",
            "status": "available"
          },
          "progress_events": "executions/exec/point/progress/events.jsonl"
        },
        "execution_id": "execution",
        "id": "point",
        "initialization": {
          "checkpoint": null,
          "fallback_reason": null,
          "kind": "cold_start",
          "source_point_id": null
        },
        "observables": {
          "current_density_A_per_m2": 0,
          "scientific_assessment": {
            "scientific_accepted": false
          }
        },
        "postprocessing": {
          "optical": {
            "error": null,
            "status": "not_requested"
          }
        },
        "quality": "invalid",
        "status": "failed",
        "warnings": []
      }
    ],
    "available_point_count": 1,
    "contract_set": "qcl-negf.results.v2",
    "evidence": [],
    "executions": [
      {
        "definition_id": "study",
        "id": "execution",
        "label": "",
        "method_id": "method",
        "operation": "stationary",
        "point_ids": [
          "point"
        ],
        "purpose": "diagnostic",
        "repetition": 1,
        "variant_id": "variant"
      }
    ],
    "expected_point_count": 1,
    "inclusions": [],
    "model_version": "manufactured",
    "name": "P07 manufactured declaration only",
    "plan_fingerprint": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "plan_scientific_fingerprint": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "points": [
      {
        "attempt": 1,
        "converged": false,
        "coordinates": {
          "branch": "forward",
          "order": 1,
          "temperature_K": 70.0,
          "voltage_per_period_V": 0.05
        },
        "data": {
          "literal_scalar_extension": "kept",
          "native_source": {
            "artifact_root": ".",
            "diagnostics": null,
            "kind": "absent",
            "observations": null,
            "reason": "solver_not_run",
            "relation": null,
            "schema": "qcl-negf.native-point-source.v1",
            "selected_state": null
          },
          "optical_attachment": null,
          "phase_source": {
            "current_path": "executions/exec/point/attempt-1/phase/phase-current.json",
            "reason": null,
            "session_id": "phase-session",
            "status": "available"
          },
          "progress_events": "executions/exec/point/progress/events.jsonl"
        },
        "execution_id": "execution",
        "id": "point",
        "initialization": {
          "checkpoint": null,
          "fallback_reason": null,
          "kind": "cold_start",
          "source_point_id": null
        },
        "observables": {
          "current_density_A_per_m2": 0,
          "scientific_assessment": {
            "scientific_accepted": false
          }
        },
        "postprocessing": {
          "optical": {
            "error": null,
            "status": "not_requested"
          }
        },
        "quality": "invalid",
        "status": "failed",
        "warnings": []
      }
    ],
    "root_definition_id": "study",
    "schema": "qcl-negf-series-result-v4",
    "selected_execution_id": "execution",
    "status": "failed"
  },
  "checkpoint": {
    "row_key": {
      "execution_id": "execution",
      "point_id": "point",
      "point_attempt": 3
    },
    "source": {
      "schema": "qcl-negf.native-point-source.v1",
      "kind": "checkpoint",
      "artifact_root": ".",
      "relation": "current_point_attempt",
      "selected_state": {
        "identity": {
          "run_id": "run",
          "execution_id": "execution",
          "point_id": "point",
          "attempt": "3",
          "branch_id": "branch",
          "model_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "plan_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        },
        "state_id": "state3",
        "state_sequence": "1",
        "manifest": {
          "path": "manifests/current.json",
          "bytes": "8058",
          "sha256": "a638d77b13c876f847d957e9325298809795cdda1047bf48029432c09bd448a9",
          "media_type": "application/json",
          "schema": "qcl-negf.result-manifest.v1"
        }
      },
      "diagnostics": null,
      "observations": null,
      "reason": "final_not_published"
    },
    "manifest": {
      "schema": "qcl-negf.result-manifest.v1",
      "contract_set": "qcl-negf.results.v2",
      "identity": {
        "run_id": "run",
        "execution_id": "execution",
        "point_id": "point",
        "attempt": "3",
        "branch_id": "branch",
        "model_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "plan_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      },
      "lineage": {
        "parent_state": null
      },
      "invocation": {
        "source_epoch": "epoch3"
      },
      "numerical_binding": {
        "identity": {
          "fields": {
            "contract_version": {
              "decimal": "1",
              "kind": "integer",
              "type": "UInt16"
            },
            "execution": [
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              }
            ],
            "policy": [
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              }
            ],
            "problem": [
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              }
            ]
          },
          "type": "ExactRestartIdentity"
        },
        "execution": {
          "fields": {
            "blas": "manufactured-blas",
            "blas_threads": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            },
            "core_code": "manufactured-code",
            "energy_chunk": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            },
            "fftw": "manufactured-fftw",
            "hilbert_columns": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            },
            "index_type": "Int64",
            "julia": "1.13.0",
            "parallel_backend": {
              "kind": "symbol",
              "value": "threads"
            },
            "reduction": "static_chunks_v1",
            "residual_chunk": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            },
            "scalar_type": "Float64/ComplexF64",
            "worker_count": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            }
          },
          "type": "ExactExecutionIdentity"
        }
      },
      "state_id": "state3",
      "state_sequence": "1",
      "history": {
        "head": {
          "path": "catalog/one.json",
          "bytes": "40",
          "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
          "media_type": "application/json",
          "schema": "qcl-negf.scientific-catalog.v1"
        },
        "entries": "1",
        "records": "1",
        "scba_rows": "1",
        "outer_rows": "0",
        "witness_updates": "0",
        "last_record": "1"
      },
      "provenance": {
        "format": "qcl-negf.git-graph.v1",
        "revision": "1111111111111111111111111111111111111111",
        "components": [
          {
            "path": "components/QCLNEGF.jl",
            "revision": "2222222222222222222222222222222222222222",
            "url": "https://github.com/Afonenko-QCL-NEGF/QCLNEGF.jl"
          },
          {
            "path": "components/QCLNEGFRunner.jl",
            "revision": "3333333333333333333333333333333333333333",
            "url": "https://github.com/Afonenko-QCL-NEGF/QCLNEGFRunner.jl"
          }
        ]
      },
      "generation": "1",
      "closed": false,
      "status": {
        "process": {
          "state": "running",
          "exit_code": null,
          "reason": null
        },
        "convergence": {
          "state": "not_measured",
          "reason": "not_requested",
          "evidence": null
        },
        "physics": {
          "state": "not_measured",
          "reason": "not_requested",
          "evidence": null
        },
        "discretization": {
          "state": "not_measured",
          "reason": "not_requested",
          "evidence": null
        },
        "experimental": {
          "state": "not_measured",
          "reason": "not_requested",
          "evidence": null
        }
      },
      "checkpoint": {
        "path": "checkpoint/current.json",
        "bytes": "29188",
        "sha256": "4c6918aabb1515018338306c12ed5d8e28f4b8a8264cd974601a9223b4a19ebc",
        "media_type": "application/json",
        "schema": "qcl-negf.checkpoint-declaration.v1"
      },
      "artifacts": [
        {
          "bytes": "0",
          "media_type": "application/x-hdf5",
          "path": "manufactured.h5",
          "schema": "qcl-negf-checkpoint-v5",
          "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
          "role": "recovery",
          "profile": "full-state",
          "dependencies": []
        },
        {
          "path": "checkpoint/current.json",
          "bytes": "29188",
          "sha256": "4c6918aabb1515018338306c12ed5d8e28f4b8a8264cd974601a9223b4a19ebc",
          "media_type": "application/json",
          "schema": "qcl-negf.checkpoint-declaration.v1",
          "role": "recovery",
          "profile": "full-state",
          "dependencies": [
            "manufactured.h5",
            "history/original.h5"
          ]
        },
        {
          "path": "history/original.h5",
          "bytes": "40",
          "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
          "media_type": "application/x-hdf5",
          "schema": "qcl-negf-scientific-history-v5",
          "role": "science.history",
          "profile": "science",
          "dependencies": []
        }
      ]
    },
    "manifest_ref": {
      "path": "manifests/current.json",
      "bytes": "8058",
      "sha256": "a638d77b13c876f847d957e9325298809795cdda1047bf48029432c09bd448a9",
      "media_type": "application/json",
      "schema": "qcl-negf.result-manifest.v1"
    },
    "checkpoint": {
      "schema": "qcl-negf.checkpoint-declaration.v1",
      "contract_set": "qcl-negf.results.v2",
      "identity": {
        "run_id": "run",
        "execution_id": "execution",
        "point_id": "point",
        "attempt": "3",
        "branch_id": "branch",
        "model_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "plan_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      },
      "lineage": {
        "parent_state": null
      },
      "invocation": {
        "source_epoch": "epoch3"
      },
      "state_id": "state3",
      "state_sequence": "1",
      "core_layout_sha256": "94561e874a62ad0749be8a13645ef0142994e074b33d271cbd57807f2cf63600",
      "numerical_binding": {
        "identity": {
          "fields": {
            "contract_version": {
              "decimal": "1",
              "kind": "integer",
              "type": "UInt16"
            },
            "execution": [
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "2",
                "kind": "integer",
                "type": "UInt8"
              }
            ],
            "policy": [
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt8"
              }
            ],
            "problem": [
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              },
              {
                "decimal": "0",
                "kind": "integer",
                "type": "UInt8"
              }
            ]
          },
          "type": "ExactRestartIdentity"
        },
        "execution": {
          "fields": {
            "blas": "manufactured-blas",
            "blas_threads": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            },
            "core_code": "manufactured-code",
            "energy_chunk": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            },
            "fftw": "manufactured-fftw",
            "hilbert_columns": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            },
            "index_type": "Int64",
            "julia": "1.13.0",
            "parallel_backend": {
              "kind": "symbol",
              "value": "threads"
            },
            "reduction": "static_chunks_v1",
            "residual_chunk": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            },
            "scalar_type": "Float64/ComplexF64",
            "worker_count": {
              "decimal": "1",
              "kind": "integer",
              "type": "Int64"
            }
          },
          "type": "ExactExecutionIdentity"
        }
      },
      "history": {
        "head": {
          "path": "catalog/one.json",
          "bytes": "40",
          "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
          "media_type": "application/json",
          "schema": "qcl-negf.scientific-catalog.v1"
        },
        "entries": "1",
        "records": "1",
        "scba_rows": "1",
        "outer_rows": "0",
        "witness_updates": "0",
        "last_record": "1"
      },
      "state": {
        "fields": {
          "adaptation": null,
          "boundary": {
            "kind": "symbol",
            "value": "scba_accepted"
          },
          "continuation": {
            "type": "SCBAContinuation",
            "fields": {
              "decision": {
                "type": "SCBAStopDecision",
                "fields": {
                  "terminal": false,
                  "converged": false,
                  "status": {
                    "kind": "symbol",
                    "value": "running"
                  },
                  "quality": {
                    "kind": "symbol",
                    "value": "unresolved"
                  },
                  "reason": "manufactured"
                }
              },
              "candidate": {
                "type": "FixedPointAuditCandidate",
                "fields": {
                  "scattering": {
                    "entries": [],
                    "kind": "dict"
                  },
                  "embedding": {
                    "fields": {
                      "Σˡ": {
                        "dataset": "/manufactured/ComplexF64/3x2x1x1",
                        "eltype": "ComplexF64",
                        "file": {
                          "bytes": "0",
                          "media_type": "application/x-hdf5",
                          "path": "manufactured.h5",
                          "schema": "qcl-negf-checkpoint-v5",
                          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                        },
                        "kind": "array",
                        "order": "F",
                        "shape": [
                          "3",
                          "2",
                          "1",
                          "1"
                        ]
                      },
                      "Σᴿ": {
                        "dataset": "/manufactured/ComplexF64/3x2x1x1",
                        "eltype": "ComplexF64",
                        "file": {
                          "bytes": "0",
                          "media_type": "application/x-hdf5",
                          "path": "manufactured.h5",
                          "schema": "qcl-negf-checkpoint-v5",
                          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                        },
                        "kind": "array",
                        "order": "F",
                        "shape": [
                          "3",
                          "2",
                          "1",
                          "1"
                        ]
                      },
                      "Σᵍ": {
                        "dataset": "/manufactured/ComplexF64/3x2x1x1",
                        "eltype": "ComplexF64",
                        "file": {
                          "bytes": "0",
                          "media_type": "application/x-hdf5",
                          "path": "manufactured.h5",
                          "schema": "qcl-negf-checkpoint-v5",
                          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                        },
                        "kind": "array",
                        "order": "F",
                        "shape": [
                          "3",
                          "2",
                          "1",
                          "1"
                        ]
                      }
                    },
                    "type": "SelfEnergyFamily"
                  },
                  "plus": {
                    "fields": {
                      "Σˡ": {
                        "dataset": "/manufactured/ComplexF64/3x2x1x1",
                        "eltype": "ComplexF64",
                        "file": {
                          "bytes": "0",
                          "media_type": "application/x-hdf5",
                          "path": "manufactured.h5",
                          "schema": "qcl-negf-checkpoint-v5",
                          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                        },
                        "kind": "array",
                        "order": "F",
                        "shape": [
                          "3",
                          "2",
                          "1",
                          "1"
                        ]
                      },
                      "Σᴿ": {
                        "dataset": "/manufactured/ComplexF64/3x2x1x1",
                        "eltype": "ComplexF64",
                        "file": {
                          "bytes": "0",
                          "media_type": "application/x-hdf5",
                          "path": "manufactured.h5",
                          "schema": "qcl-negf-checkpoint-v5",
                          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                        },
                        "kind": "array",
                        "order": "F",
                        "shape": [
                          "3",
                          "2",
                          "1",
                          "1"
                        ]
                      },
                      "Σᵍ": {
                        "dataset": "/manufactured/ComplexF64/3x2x1x1",
                        "eltype": "ComplexF64",
                        "file": {
                          "bytes": "0",
                          "media_type": "application/x-hdf5",
                          "path": "manufactured.h5",
                          "schema": "qcl-negf-checkpoint-v5",
                          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                        },
                        "kind": "array",
                        "order": "F",
                        "shape": [
                          "3",
                          "2",
                          "1",
                          "1"
                        ]
                      }
                    },
                    "type": "SelfEnergyFamily"
                  },
                  "minus": {
                    "fields": {
                      "Σˡ": {
                        "dataset": "/manufactured/ComplexF64/3x2x1x1",
                        "eltype": "ComplexF64",
                        "file": {
                          "bytes": "0",
                          "media_type": "application/x-hdf5",
                          "path": "manufactured.h5",
                          "schema": "qcl-negf-checkpoint-v5",
                          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                        },
                        "kind": "array",
                        "order": "F",
                        "shape": [
                          "3",
                          "2",
                          "1",
                          "1"
                        ]
                      },
                      "Σᴿ": {
                        "dataset": "/manufactured/ComplexF64/3x2x1x1",
                        "eltype": "ComplexF64",
                        "file": {
                          "bytes": "0",
                          "media_type": "application/x-hdf5",
                          "path": "manufactured.h5",
                          "schema": "qcl-negf-checkpoint-v5",
                          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                        },
                        "kind": "array",
                        "order": "F",
                        "shape": [
                          "3",
                          "2",
                          "1",
                          "1"
                        ]
                      },
                      "Σᵍ": {
                        "dataset": "/manufactured/ComplexF64/3x2x1x1",
                        "eltype": "ComplexF64",
                        "file": {
                          "bytes": "0",
                          "media_type": "application/x-hdf5",
                          "path": "manufactured.h5",
                          "schema": "qcl-negf-checkpoint-v5",
                          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                        },
                        "kind": "array",
                        "order": "F",
                        "shape": [
                          "3",
                          "2",
                          "1",
                          "1"
                        ]
                      }
                    },
                    "type": "SelfEnergyFamily"
                  },
                  "roundoff": {
                    "kind": "ieee",
                    "type": "f64",
                    "bits": "0000000000000000"
                  }
                }
              },
              "next_previous_current": {
                "kind": "ieee",
                "type": "f64",
                "bits": "0000000000000000"
              },
              "next_previous_populations": {
                "dataset": "/manufactured/Float64/2",
                "eltype": "Float64",
                "file": {
                  "bytes": "0",
                  "media_type": "application/x-hdf5",
                  "path": "manufactured.h5",
                  "schema": "qcl-negf-checkpoint-v5",
                  "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                },
                "kind": "array",
                "order": "F",
                "shape": [
                  "2"
                ]
              },
              "seed_mu_scaled": null,
              "seed_number_ratio": null,
              "witness_retention": [
                {
                  "kind": "integer",
                  "type": "Int64",
                  "decimal": "1"
                },
                {
                  "kind": "integer",
                  "type": "Int64",
                  "decimal": "1"
                },
                {
                  "kind": "integer",
                  "type": "Int64",
                  "decimal": "1"
                }
              ]
            }
          },
          "family_order": [
            {
              "kind": "symbol",
              "value": "embedding_plus"
            },
            {
              "kind": "symbol",
              "value": "embedding_minus"
            }
          ],
          "final_evidence": null,
          "identity": {
            "fields": {
              "contract_version": {
                "decimal": "1",
                "kind": "integer",
                "type": "UInt16"
              },
              "execution": [
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "2",
                  "kind": "integer",
                  "type": "UInt8"
                }
              ],
              "policy": [
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "1",
                  "kind": "integer",
                  "type": "UInt8"
                }
              ],
              "problem": [
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                },
                {
                  "decimal": "0",
                  "kind": "integer",
                  "type": "UInt8"
                }
              ]
            },
            "type": "ExactRestartIdentity"
          },
          "integrity": [
            {
              "decimal": "137",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "109",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "248",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "221",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "143",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "150",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "254",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "235",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "121",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "9",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "176",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "71",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "244",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "1",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "188",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "249",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "202",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "90",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "6",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "158",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "30",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "130",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "21",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "194",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "253",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "84",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "113",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "45",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "254",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "59",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "48",
              "kind": "integer",
              "type": "UInt8"
            },
            {
              "decimal": "37",
              "kind": "integer",
              "type": "UInt8"
            }
          ],
          "measurement_context": {
            "fields": {
              "algorithms": {
                "fields": {
                  "anderson_damping": {
                    "bits": "3ff0000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "anderson_history_depth": {
                    "decimal": "5",
                    "kind": "integer",
                    "type": "Int64"
                  },
                  "anderson_regularization": {
                    "bits": "3d719799812dea11",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "contraction": {
                    "kind": "symbol",
                    "value": "dense_blas"
                  },
                  "embedding": {
                    "kind": "symbol",
                    "value": "full_cell_resolvent"
                  },
                  "embedding_periods": {
                    "decimal": "8",
                    "kind": "integer",
                    "type": "Int64"
                  },
                  "energy_shift": {
                    "kind": "symbol",
                    "value": "sparse_plan"
                  },
                  "hilbert": {
                    "kind": "symbol",
                    "value": "fft"
                  },
                  "kernel_build": {
                    "kind": "symbol",
                    "value": "tabulated"
                  },
                  "localization": {
                    "kind": "symbol",
                    "value": "pzp"
                  },
                  "low_rank_maximum_rank": {
                    "decimal": "0",
                    "kind": "integer",
                    "type": "Int64"
                  },
                  "low_rank_relative_tolerance": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "mixing": {
                    "kind": "symbol",
                    "value": "linear"
                  },
                  "occupation_normalization": {
                    "kind": "symbol",
                    "value": "paired_convex"
                  },
                  "retarded_real_part": {
                    "kind": "symbol",
                    "value": "kramers_kronig"
                  },
                  "self_energy_structure": {
                    "kind": "symbol",
                    "value": "full"
                  },
                  "solver_backend": {
                    "kind": "symbol",
                    "value": "production"
                  },
                  "transverse_momentum": {
                    "kind": "symbol",
                    "value": "resolved"
                  }
                },
                "type": "AlgorithmOptions"
              },
              "backend": {
                "kind": "symbol",
                "value": "not_executed"
              },
              "final_state_alignment": {
                "kind": "symbol",
                "value": "producer_aligned"
              },
              "hilbert_roundoff_measured": null,
              "map_kernels_shared_with_solver": null
            },
            "type": "AcceptanceContext"
          },
          "mixer_position": {
            "fields": {
              "applied_mixes": {
                "decimal": "0",
                "kind": "integer",
                "type": "Int64"
              },
              "last_coefficient_fallback": {
                "kind": "symbol",
                "value": "none"
              },
              "last_reset_mix": {
                "decimal": "0",
                "kind": "integer",
                "type": "Int64"
              },
              "last_reset_reason": {
                "kind": "symbol",
                "value": "none"
              },
              "retained_mix_indices": []
            },
            "type": "MixerPosition"
          },
          "outer_history": {
            "eltype": "OuterIteration",
            "kind": "array",
            "order": "F",
            "shape": [
              "1"
            ],
            "values": [
              {
                "fields": {
                  "J": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "density": {
                    "dataset": "/manufactured/Float64/2",
                    "eltype": "Float64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "2"
                    ]
                  },
                  "populations": {
                    "dataset": "/manufactured/Float64/2",
                    "eltype": "Float64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "2"
                    ]
                  },
                  "r_J": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "r_Jchange": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "r_P": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "r_U": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "r_n": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "r_neutral": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "r_population": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "ζ": {
                    "bits": "0000000000000000",
                    "kind": "ieee",
                    "type": "f64"
                  },
                  "μ": {
                    "decimal": "1",
                    "kind": "integer",
                    "type": "Int64"
                  }
                },
                "type": "OuterIteration"
              }
            ]
          },
          "policy_evaluations": [],
          "progress": {
            "fields": {
              "accepted_scba": {
                "decimal": "1",
                "kind": "integer",
                "type": "Int64"
              },
              "completed_outer": {
                "decimal": "1",
                "kind": "integer",
                "type": "Int64"
              },
              "current_outer": {
                "decimal": "1",
                "kind": "integer",
                "type": "Int64"
              },
              "diagnostic_streak": {
                "decimal": "0",
                "kind": "integer",
                "type": "Int64"
              },
              "inner_strict_streak": {
                "decimal": "0",
                "kind": "integer",
                "type": "Int64"
              },
              "outer_strict_streak": {
                "decimal": "0",
                "kind": "integer",
                "type": "Int64"
              }
            },
            "type": "SolverProgress"
          },
          "published_U": {
            "dataset": "/manufactured/Float64/2",
            "eltype": "Float64",
            "file": {
              "bytes": "0",
              "media_type": "application/x-hdf5",
              "path": "manufactured.h5",
              "schema": "qcl-negf-checkpoint-v5",
              "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
            },
            "kind": "array",
            "order": "F",
            "shape": [
              "2"
            ]
          },
          "published_n": {
            "dataset": "/manufactured/Float64/2",
            "eltype": "Float64",
            "file": {
              "bytes": "0",
              "media_type": "application/x-hdf5",
              "path": "manufactured.h5",
              "schema": "qcl-negf-checkpoint-v5",
              "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
            },
            "kind": "array",
            "order": "F",
            "shape": [
              "2"
            ]
          },
          "scba": {
            "fields": {
              "converged": false,
              "embedding": {
                "fields": {
                  "Σˡ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "Σᴿ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "Σᵍ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  }
                },
                "type": "SelfEnergyFamily"
              },
              "embedding_minus": {
                "fields": {
                  "Σˡ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "Σᴿ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "Σᵍ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  }
                },
                "type": "SelfEnergyFamily"
              },
              "embedding_plus": {
                "fields": {
                  "Σˡ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "Σᴿ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "Σᵍ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  }
                },
                "type": "SelfEnergyFamily"
              },
              "green": {
                "fields": {
                  "A": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "Gˡ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "Gᴿ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "Gᵍ": {
                    "dataset": "/manufactured/ComplexF64/3x2x1x1",
                    "eltype": "ComplexF64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2",
                      "1",
                      "1"
                    ]
                  },
                  "condition_number": {
                    "dataset": "/manufactured/Float64/3x2",
                    "eltype": "Float64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2"
                    ]
                  },
                  "dyson_scale": {
                    "dataset": "/manufactured/Float64/3x2",
                    "eltype": "Float64",
                    "file": {
                      "bytes": "0",
                      "media_type": "application/x-hdf5",
                      "path": "manufactured.h5",
                      "schema": "qcl-negf-checkpoint-v5",
                      "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
                    },
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "3",
                      "2"
                    ]
                  }
                },
                "type": "GreenState"
              },
              "history": {
                "eltype": "SCBAIteration",
                "kind": "array",
                "order": "F",
                "shape": [
                  "1"
                ],
                "values": [
                  {
                    "fields": {
                      "J": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "lambda_change": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "normalized_charge": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "physical_markers": null,
                      "r_A": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "r_D": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "r_Jchange": {
                        "bits": "7ff8000000000001",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "r_K": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "r_PSD": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "r_caus": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "r_population": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "r_roundoff": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "r_Σ": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "r_λ": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "raw_charge": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "target_charge": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "witness": null,
                      "λ": {
                        "bits": "0000000000000000",
                        "kind": "ieee",
                        "type": "f64"
                      },
                      "ν": {
                        "decimal": "1",
                        "kind": "integer",
                        "type": "Int64"
                      }
                    },
                    "type": "SCBAIteration"
                  }
                ]
              },
              "mixer_state": {
                "fields": {
                  "method": {
                    "kind": "symbol",
                    "value": "linear"
                  },
                  "residuals": {
                    "eltype": "Vector{SelfEnergyFamily}",
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "0"
                    ],
                    "values": []
                  },
                  "states": {
                    "eltype": "Vector{SelfEnergyFamily}",
                    "kind": "array",
                    "order": "F",
                    "shape": [
                      "0"
                    ],
                    "values": []
                  }
                },
                "type": "SCBAMixerState"
              },
              "quality": {
                "kind": "symbol",
                "value": "unresolved"
              },
              "restart_contract": null,
              "scattering": {
                "entries": [],
                "kind": "dict"
              },
              "status": {
                "kind": "symbol",
                "value": "running"
              }
            },
            "type": "SCBAResult"
          },
          "status": {
            "kind": "symbol",
            "value": "running"
          },
          "warnings": []
        },
        "type": "ExactSolverState"
      },
      "payloads": [
        {
          "bytes": "0",
          "media_type": "application/x-hdf5",
          "path": "manufactured.h5",
          "schema": "qcl-negf-checkpoint-v5",
          "sha256": "0000000000000000000000000000000000000000000000000000000000000000"
        },
        {
          "path": "history/original.h5",
          "bytes": "40",
          "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
          "media_type": "application/x-hdf5",
          "schema": "qcl-negf-scientific-history-v5"
        }
      ],
      "witness_targets": [
        {
          "row_key": {
            "domain_revision": {
              "kind": "integer",
              "type": "Int64",
              "decimal": "0"
            },
            "discretization_id": "grid",
            "basis_id": "basis",
            "stage": {
              "kind": "symbol",
              "value": "scba"
            },
            "outer_iteration": {
              "kind": "integer",
              "type": "Int64",
              "decimal": "1"
            },
            "inner_iteration": {
              "kind": "integer",
              "type": "Int64",
              "decimal": "1"
            }
          },
          "locator": {
            "source_identity": {
              "run_id": "run",
              "execution_id": "execution",
              "point_id": "point",
              "attempt": "3",
              "branch_id": "branch",
              "model_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
              "plan_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            },
            "source_epoch": "epoch3",
            "append_ordinal": "1",
            "leaf": {
              "path": "history/original.h5",
              "bytes": "40",
              "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
              "media_type": "application/x-hdf5",
              "schema": "qcl-negf-scientific-history-v5"
            },
            "row_offset": "0"
          }
        }
      ]
    },
    "diagnostics": null,
    "observations": null,
    "plan": null,
    "model": null,
    "parent_manifest": null,
    "parent_checkpoint": null
  },
  "proof": {
    "row_key": {
      "execution_id": "execution",
      "point_id": "point",
      "point_attempt": 3
    },
    "mode": "current",
    "creator_calcjob_uuid": "00000000-0000-4000-8000-000000000003",
    "origin_row_key": {
      "execution_id": "execution",
      "point_id": "point",
      "point_attempt": 3
    },
    "origin_result_uuid": null,
    "origin_binding_uuid": null,
    "processes": [
      {
        "uuid": "00000000-0000-4000-8000-000000000001",
        "process_type": "aiida.workflows:qcl_negf.plan",
        "attempt_input": null,
        "execution_input": null
      },
      {
        "uuid": "00000000-0000-4000-8000-000000000002",
        "process_type": "aiida.workflows:qcl_negf.execution_restart",
        "attempt_input": {
          "node_uuid": "00000000-0000-4000-8000-000000000030",
          "value": 1
        },
        "execution_input": {
          "node_uuid": "00000000-0000-4000-8000-000000000031",
          "value": "execution"
        }
      },
      {
        "uuid": "00000000-0000-4000-8000-000000000003",
        "process_type": "aiida.calculations:qcl_negf.execution",
        "attempt_input": {
          "node_uuid": "00000000-0000-4000-8000-000000000032",
          "value": 2
        },
        "execution_input": {
          "node_uuid": "00000000-0000-4000-8000-000000000031",
          "value": "execution"
        }
      }
    ],
    "codes": [
      {
        "code_uuid": "00000000-0000-4000-8000-000000000013",
        "computer_uuid": "00000000-0000-4000-8000-000000000020",
        "solver_executable": "/nix/store/fixture-qcl/bin/qcl-negf",
        "release_id": "fixture-release",
        "build_receipt_ref": "fixture-build"
      }
    ],
    "links": [
      {
        "source_uuid": "00000000-0000-4000-8000-000000000001",
        "target_uuid": "00000000-0000-4000-8000-000000000002",
        "type": "call_work",
        "label": "execution_0"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000002",
        "target_uuid": "00000000-0000-4000-8000-000000000003",
        "type": "call_calc",
        "label": "iteration_02"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000010",
        "target_uuid": "00000000-0000-4000-8000-000000000001",
        "type": "input_work",
        "label": "plan"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000011",
        "target_uuid": "00000000-0000-4000-8000-000000000001",
        "type": "input_work",
        "label": "source_graph"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000013",
        "target_uuid": "00000000-0000-4000-8000-000000000001",
        "type": "input_work",
        "label": "code"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000010",
        "target_uuid": "00000000-0000-4000-8000-000000000002",
        "type": "input_work",
        "label": "plan"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000011",
        "target_uuid": "00000000-0000-4000-8000-000000000002",
        "type": "input_work",
        "label": "source_graph"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000013",
        "target_uuid": "00000000-0000-4000-8000-000000000002",
        "type": "input_work",
        "label": "code"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000010",
        "target_uuid": "00000000-0000-4000-8000-000000000003",
        "type": "input_calc",
        "label": "plan"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000011",
        "target_uuid": "00000000-0000-4000-8000-000000000003",
        "type": "input_calc",
        "label": "source_graph"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000013",
        "target_uuid": "00000000-0000-4000-8000-000000000003",
        "type": "input_calc",
        "label": "code"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000030",
        "target_uuid": "00000000-0000-4000-8000-000000000002",
        "type": "input_work",
        "label": "attempt"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000031",
        "target_uuid": "00000000-0000-4000-8000-000000000002",
        "type": "input_work",
        "label": "execution_id"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000012",
        "target_uuid": "00000000-0000-4000-8000-000000000002",
        "type": "input_work",
        "label": "native_execution"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000032",
        "target_uuid": "00000000-0000-4000-8000-000000000003",
        "type": "input_calc",
        "label": "attempt"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000031",
        "target_uuid": "00000000-0000-4000-8000-000000000003",
        "type": "input_calc",
        "label": "execution_id"
      },
      {
        "source_uuid": "00000000-0000-4000-8000-000000000012",
        "target_uuid": "00000000-0000-4000-8000-000000000003",
        "type": "input_calc",
        "label": "native_execution"
      }
    ],
    "hops": []
  },
  "execution_binding": {
    "schema": "qcl-negf.aiida-native-execution.v1",
    "request_id": "00000000-0000-4000-8000-000000000100",
    "execution_id": "execution",
    "storage": {
      "namespace_id": "fixture",
      "storage_identity": "fixture-nfs",
      "relative_execution_root": "runs/request/execution",
      "computer_uuid": "00000000-0000-4000-8000-000000000020",
      "execution_binding_uuid": "00000000-0000-4000-8000-000000000012",
      "retention_policy_id": "fixture-retention",
      "retention_owner_id": "fixture-owner",
      "hold_ref": "fixture-hold",
      "access_binding_ref": "fixture-access"
    },
    "source_graph": {
      "node_uuid": "00000000-0000-4000-8000-000000000011",
      "bytes": "401",
      "sha256": "a486d7d2bddf100552d178edceb561440894b6c8946e4ddaf7c552404611221a"
    },
    "code": {
      "code_uuid": "00000000-0000-4000-8000-000000000013",
      "computer_uuid": "00000000-0000-4000-8000-000000000020",
      "solver_executable": "/nix/store/fixture-qcl/bin/qcl-negf",
      "release_id": "fixture-release",
      "build_receipt_ref": "fixture-build"
    },
    "input_plan": {
      "node_uuid": "00000000-0000-4000-8000-000000000010",
      "bytes": "18414",
      "sha256": "90a4b841700186ad1ee18617764c3ee1ac9bf68c55606dfc5a6ac4c9c6abf32c"
    },
    "access_receipt_ref": "fixture-access"
  },
  "graph": {
    "format": "qcl-negf.git-graph.v1",
    "revision": "1111111111111111111111111111111111111111",
    "components": [
      {
        "path": "components/QCLNEGF.jl",
        "revision": "2222222222222222222222222222222222222222",
        "url": "https://github.com/Afonenko-QCL-NEGF/QCLNEGF.jl"
      },
      {
        "path": "components/QCLNEGFRunner.jl",
        "revision": "3333333333333333333333333333333333333333",
        "url": "https://github.com/Afonenko-QCL-NEGF/QCLNEGFRunner.jl"
      }
    ]
  }
}
''')
