# Pure native result association

`aiida_qcl_negf.science_association.bind_native_result` joins explicitly supplied
outward/native declarations and a closed AiiDA graph snapshot. It returns a
**draft**, even when every declaration agrees. A caller can fabricate a coherent
snapshot. The function authenticates neither nodes nor files, storage, installed
code, scheduler ownership, current access, numerical convergence or scientific
acceptance. It never creates/stores nodes, loads a profile, submits or retries.
Bundled schema resources are read through the public Contracts validators.

```python
bind_native_result(raw_result: bytes, *, selection: dict, frozen_plan: bytes,
                   plan_node_uuid: str, source_graph: dict,
                   source_graph_node_uuid: str, execution_binding: dict,
                   source_contexts: tuple[dict, ...],
                   creator_proofs: tuple[dict, ...],
                   maximum_metadata_bytes: int) -> dict
```

Every argument is required. Inputs use exact builtins, strict UTF-8 strings and
finite JSON values, with no cycles and at most 64 levels of nesting. The two
context/proof collections are exact tuples; nested arrays are lists. Bytes occur
only in `raw_result`, `frozen_plan`, `source_graph.raw` and `files[*].raw` within
creator proofs. Custom mappings/scalars/ORM objects are refused.

Local objects have exactly the keys below. All keys are required; `| None`
permits a null value, not an omitted key. UUID means the complete canonical
lowercase UUID string. `Id`, `Sha256`, `Count` and native `PositiveCount` retain
public `result-common.schema.json` definitions. In particular, native counters
and byte counts are **decimal strings**, while outward point and transport
attempts are exact Python positive integers up to UInt64 maximum (never bool).

| Input/type | Required fields |
| --- | --- |
| selection | `run_uuid:UUID, execution_id:Id, attempt:int, calcjob_uuid:UUID` |
| source_graph | `value:SourceGraph, raw:bytes`; SourceGraph is the published model-help definition |
| NodeFile | `node_uuid:UUID, bytes:Count, sha256:Sha256` |
| execution_binding | `schema,request_id,execution_id,storage,source_graph,code,input_plan,access_receipt_ref` |
| storage | `namespace_id,storage_identity,relative_execution_root,computer_uuid,execution_binding_uuid,retention_policy_id,retention_owner_id,hold_ref,access_binding_ref` |
| code | `code_uuid,computer_uuid,solver_executable,release_id,build_receipt_ref` |
| RowKey | `execution_id:Id, point_id:Id, point_attempt:int` |

Execution schema is `qcl-negf.aiida-native-execution.v1`; request/computer/code/
execution-binding identifiers are UUIDs. Execution ID, namespace and release ID
are public Ids. Plan and source-graph entries are NodeFiles. Root paths follow
public POSIX relative-path rules. Solver executable is a declared lexical
`/nix/store/<component>/bin/qcl-negf` path. Remaining storage/access/retention/build
references are opaque strings, length 1–256, without leading slash or C0/DEL.
Their presence gives no capability. Original plan and graph bytes are mandatory,
including an absent result with empty context/proof tuples. Hashing a re-encoded
decoded object cannot replace them.

Each nonabsent `(execution_id,point_id,point_attempt)` in points or history has
exactly one context and proof. Identical rows shared by those two collections
have one key. Missing, duplicate, extra and absent-source contexts/proofs refuse.

`SourceContext` keys are `row_key,source,manifest,manifest_ref,checkpoint,
diagnostics,observations,plan,model,parent_manifest,parent_checkpoint`. Source is
the unmodified public NativePointSource. All native scientific fields use whole
published Contracts definitions and the two public outward APIs, rather than a
local scientific schema. `manifest_ref` is the entire selected five-field
FileRef. Identity has seven fields; StateRef has `identity,state_id,state_sequence,
manifest`; Invocation has `source_epoch`.

Checkpoint contexts require all six final/parent fields null. Final contexts
require diagnostics, observations, scientific plan and ModelEnvelope; the parent
pair is nonnull exactly for completed restoration. Public
`validate_outward_native_source` checks the manifest/checkpoint/D1/O1/model/plan
and immediate-parent relations. It does not authenticate their files or calculate
scientific criteria. Original plan SHA/length and exact plan point/execution/
branch/order/T/V binary64 coordinates also join to the supplied result. Native
branch content IDs are preserved, not reconstructed by a guessed algorithm.

Creator proofs have exactly `row_key,mode,creator_calcjob_uuid,origin_row_key,
origin_result_uuid,origin_binding_uuid,processes,codes,files,links,hops`. Arrays are
lists; processes, codes and links are nonempty. File entries have `node_uuid,raw`.
Processes have `uuid,process_type,attempt_input,execution_input`. Only the Plan
process has null scalar inputs; others use `{node_uuid,value}`. Types are the
registered Plan/Restart/CalcJob entry points. Links have
`source_uuid,target_uuid,type,label`, where type is one of call_work/call_calc/
create/input_work/input_calc/return and label is a valid observed ASCII AiiDA
identifier. A call label is not a unique recovery event identifier.

The graph must close at every endpoint, with no unrelated or conflicting claims.
It has exactly one Plan→CalcJob or Plan→Restart→CalcJob path to each relevant
child and the declared original plan/graph/code/scalar/native_execution inputs.
Current creator means the selected CalcJob UUID, null origin file UUIDs and empty
hops. Its result/binding outputs do not exist at this draft stage. Retained
creator means a different CalcJob UUID, original raw result/binding files, typed
CREATE links and an ordered chain of `{child_calcjob_uuid,parent_binding_uuid}`
INPUT_CALC hops. Each parent binding joins its own original result bytes and
selection. RETURN, counters, time and “latest” never substitute for CREATE or the
explicit chain. Cycles and ambiguous creators refuse. Stored binding integrity
markers are checked only as structural data and convey no authority.

Native, outward-row and CalcJob attempts are separate domains. Native3/row3 can
have current creator input2. A completed skip row1/native1 inside child2 keeps
its original child1 creator, even with relation `current_point_attempt`.
Same-child row2/native1 may have current creator with relation `retained_source`.
A checkpoint selection may conservatively retain the same StateRef from a prior
final source, with null D/O and no final claim. Completed restoration preserves
its original final evidence and separate revalidation/independent-oracle records.

The result has exactly `selection,input_plan,source_graph,storage,raw_result,
points,attempt_history`. Raw result metadata is `{bytes,sha256}`. Projected rows
have `point_id,point_attempt,source,source_invocation,creator,plan_point_id,
coordinates`. Creator is null for absent, otherwise `{calcjob_uuid,binding_uuid}`;
current binding UUID is null, retained binding UUID is the original creator's.
The draft has no schema, integrity, verified, pass or grant field. Original raw
status/quality/converged/optical/scientific data are not rewritten. Returned data
are detached from caller objects.

Before decode or copying, S12 admission counts every provided raw byte occurrence
plus the exact public compact UTF-8 JSON encoding length of the argument
projection (bytes replaced by null, top tuples by arrays). Repeated snapshots
count again. Raw result is at most 32 MiB, plan at most 16 MiB; aggregate allowance
is caller supplied and exact, including its own encoded decimal width. Admission
is a metadata allowance, not an RSS/physical I/O certificate. Oversize data refuse
without truncation. Failures raise public `ContractError`; local type/budget
errors use invalid_input, joins corrupt_result and unsupported plan/result
versions incompatible_contract. No partial draft is returned.

Actor loading, authentic immutable node/file/model bytes, protected native root
access, build graph/code binding, final envelope construction and post-store
CREATE checks belong to the separate B stage. Recovery persistence/reconciliation
and native execution belong to C/D. A current parser is still PARSING and cannot
reuse a terminal-only source lookup unchanged. Runner frontier/crossroot import,
actual-build graph delivery and daemon storage/native admission bridges remain
required owner followups. Unknown submission outcome needs reconciliation;
scientific failure never implies automatic retry. This pure function grants
none of these operations.
