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

## Stored consumer and c1 origin operands

The B consumer in `saved_sources.py` obtains original manifest, checkpoint,
plan/model and final D/O declarations through public Results readers while the
public A selected-source context remains open. Each distinct nonabsent
point/history RowKey gets a context and a creator proof; a repeated identical
RowKey uses one proof. It preserves original result bytes, all rows, StateRefs,
FileRefs, binary64 values and native epochs. The public A tuple remains
`(original_result, selected_sources, None)` and supplies current byte access.

A new producer epoch has the form
`c1-<CalcJob UUID lowercase hex32>-<native_execution INPUT UUID hex32>-<UInt64 nonce>`.
The nonce, row attempt, transport attempt and relation are separate domains.
The consumer uses these tokens to select actual immutable ORM operands, then
repeats typed Plan/Restart/CalcJob CALL/INPUT joins, original plan/graph/native
file identities, Code/release/scalar inputs and genuine script CREATE/raw reads.
The public `read_native_origin_inputs` port returns original native input/script
bytes; its returned tuple is checked against B's own actual reads. The script's
original literal eleven Runner arguments must join the actual creator, input
file names, execution, Code and declared output suffix. This suffix does not
qualify a jobs root or grant filesystem access.

The same selected CalcJob can create a `retained_source`; it remains eligible
after the same joins. A different CalcJob, even with `current_point_attempt`,
requires existing actual `INPUT_CALC parent_binding` hops, genuine parent
`CREATE native_binding/result/selection`, original bytes and scalar pins. Each
hop must retain the same selected source/invocation and end at its original
creator. Missing or cyclic chains remain unavailable with the missing operand.
This consumer does not add a WorkChain input, retry or recovery frontier. Legacy
unmapped epochs remain `creator_proof_unavailable`. Restored final evidence
still requires an original immediate-parent manifest/checkpoint context; the
current consumer reports `qualified_immediate_parent_context` unavailable.

Retained reads repeat immutable ORM inputs, original result/binding/retrieved
bytes, scalar selection CREATE/pins and c1/input/script mapping, including an
existing retained chain. The existing owning `_binding_stored` helper checks
only the original structural projection. Its stored `verified` words convey no
renewed authority. The getter calls no A/native backend, NFS or Platform reader
and manufactures no checkpoint/manifest context. Its response explicitly has
`native_contexts=not_reverified`, `current_access=not_checked` and
`scientific_acceptance=not_assessed`.

One attempt-owned nonrefundable ledger covers parser, genuine CREATE pair,
immutable readback and scalar selection. The current operational allowance is
320 MiB metadata W and 256 MiB scoped I/O. It prepays reusable Results workspace
128 MiB once, separate binder/outward validation workspace 96 MiB once and each
8 MiB detached context slot **before** the public call. ResultReadLimits metadata
is 8 MiB per method: the unchanged 113111-byte plan's ordinary JSON workspace
forecast is 2839900 bytes, already above the former 2 MiB limit. These are
operational admissions, not physical/scientific criteria or total heap/RSS limits.
Every selected StateRef/kind and every point/history row remains mandatory;
insufficient aggregate credit HOLDs without filtering or shortening inputs.

The public `describe_native_metadata_context` operation returns the original
manifest/checkpoint and detached input/final Descriptions only after stability
and all closes. B passes both required `diagnostics_ref` and `observations_ref`
keywords explicitly (None for checkpoint). It passes Results workspace128MiB
and retained8MiB separately. B owns no StateReader/final-reader lifetime and
reuses workspace only after a successful closed operation. Its context projection
aliases exclusively owned detached objects; later binder arguments/copies and
output/readback materialization are paid separately. A failure/cancellation makes
the same ledger terminal: no refund, next-ref retry, reset or dependent reuse.
Primary exceptions/cancellation survive secondary close failures, and output
constructors remain after successful selected-access exit.

The initial e39/b665 qualification counted all three transitive cache domains: twelve
Results science validators, six fixed AiiDA binder resource/fragment keys and
five fixed public Contracts outward DTO roots. Controlled source census measures
binder6/outward5 W55168568 and all23 cached schema graphs W114914288, including
shared standard metaschemas. The 96 MiB owning reservation comprises own cache
contents bounded at32MiB before work, five outward bundles at6MiB each, three
simultaneous fresh factory bundles at8MiB each,8MiB inspection/traversal and2MiB
wrapper/builder bookkeeping. The fixed sorted36-schema factory's maximum prefix
plus JSON decode forecast is6951280 bytes before its bounded resource/map headers.
This accounts explicit metadata structures, not interpreter/allocator memory.
The earlier256/64 policy and its GREEN source receipts remain partial accounting
evidence; they do not demonstrate full transitive capacity.

Before relevant factory/consumer work and after closed work, B inspects its own
actual CPython C-cache layout, unique owning function target and six known keys,
cached execution/registry aliases and an incremental bounded builtin schema
forest. Unknown keys/layout or changed identities HOLD. Contracts private caches
are never imported/accessed in production: their five closed public entry roots
and pinned immutable packaged factory are the conservative source bound. This
assumes trusted CPython/stdlib/library/import machinery and immutable packaged
assets; it is not universal Python/VM/security attestation. Caches are neither
cleared nor prewarmed by production code; no Contracts API is changed.

Artifact and package-source scopes remain distinct. Each closed Results call
prepays the enforced artifact16MiB allowance plus the producer's enforced
identity-source6MiB credit across initialization/stability/close. Original ORM,
origin-port and selected-access I/O remains separately prepaid and nonrefundable.
`maximum_io_bytes` and B's256MiB scoped ledger cover these named operations;
public artifact `delivered_bytes` is not total process/package/filesystem I/O.

Immutable packaged schema/factory/vocabulary reads are **outside** that scoped
I/O upper bound. Their observed source asset bytes are qualification telemetry,
not another enforced/public credit or guaranteed whole-filesystem limit. In
particular the public catalog/depth32/pages16 paths can perform more repeated
factory reads than simple CP/final fixtures. The former14MiB package allowance
was not source-proven and is withdrawn; no unproved8MiB factory reservations are
charged. Package memory remains fully included in128+96/outer320 W accounting
under the pinned immutable package/source/runtime assumptions. Interpreter
imports and backend/query physical I/O are also outside the delivered-byte metric.

Original plan reads and original/selected/parent/offline plan projections permit
up to1MiB; graph, native execution, script and default files retain64KiB. Whole
original byte/UUID claims and actual typed INPUT/CALL/CREATE chains are checked
for each role. Before the origin port B reserves2.5MiB metadata and327680 payload
I/O bytes, exactly `floor(2621440/8)`. Its actual actor ledger prepays8 times every
requested payload chunk/EOF before delivery; queries/other charges are
nonnegative and cache hits deliver no bytes. The unchanged113111-byte plan fits
this narrower allowance; a1MiB role ceiling is not a whole-port fit guarantee.
Separate original reads, JSON workspace and returned comparisons remain charged.

SOURCE/L checks use genuine isolated ORM nodes/links and source-owned synthetic
CP/final fixtures around original plan/model bytes. Current, same-child retained,
existing foreign-parent binding, multiple StateRefs, parser→CREATE/readback/scalar
and offline joins are separate from native creator qualification. Offline output
explicitly says native_contexts=not_reverified/current_access=not_checked. A302
scientific failure can carry complete association outputs; it is not acceptance.
Missing actual parent/restore proof still HOLDs. Historical32MiB/4MiB failures and
all failed attempts remain preserved with their source identities. The selected
Results dependency's normal T0/phase shared-cache profile limitation is a separate
owning Results/installed gate; no unpublished dependency fix is consumed here.

Actual installed Code/APP/wrapper/profile/caller, original protected dispatch,
virgin/noReuse/writer-domain, RO/retention lifetime and selected-source birth
qualification remain mandatory Root native gates. A c1 token, coherent stored
projection or SOURCE/L fixture does not establish them. Plan-role metadata
eligibility is separate from the still-open combined parser credit/Results
bridge and installed composition gates. Scientific acceptance is separate.


## Bounded original scalar admission

Original offline and parent `selection` reads use one owning backend/UUID
helper. It never hydrates an ORM Dict or calls its `get_dict()`. Static
bound-parameter SQL first obtains at most two CREATE UUID/type rows. A second
statement repeats unique actual CREATE ownership, computes native UTF-8 JSON
byte length and returns complete attributes text only when at most4096 bytes.
Size, ownership and conditional text share that statement's snapshot; the first
lookup and later graph/files readback have separate snapshots. Stored AiiDA
operands/layout are qualified immutable source, not privileged SQL/VM attestation.

The current closed scalar maximum is835 ASCII JSON bytes / W8242: canonical
UUID36/SHA64/Id128/UInt64/Count20 and fixed empty/null/performance fields. The
4096-byte cap covers this entire valid shape. Before SQL/text/decode the ledger
prepays2228992W and8192 scoped I/O bytes: Q65536 for fixed queries, bounded rows,
expected copies and bookkeeping; worst decode2114048W; simultaneous text/raw
49408W. Oversize originals return only size/NULL and refuse without a free tree
copy. All prepaid credit remains spent, and failures make the operation terminal.
Full ordinary dict equality preserves reordered keys and rejects extra root or
nested fields. Expected pins come from unique genuine CREATE original bounded
result/binding files and are compared to the stored declarations before use.

Qualified matrix is CPython3.14.7/AiiDA2.9.2/SQLAlchemy2.1.1/main_0002:
SqliteDosStorage with sqlite/pysqlite/qmark, SQLite3.53.4 and UTF-8;
PsqlDosBackend with postgresql/psycopg/pyformat, psycopg3.3.6, PostgreSQL17.11 and
server/client UTF8. Unknown adapters/schema/encoding/version HOLD. Only builtin
Dict source is admitted. The helper receives an already qualified backend and
canonical creator UUID, with no creator-object refresh inside the SQL boundary.
It uses `no_autoflush`, borrows the session/connection, and owns only its cursors;
it adds no commit/rollback/isolation policy. Driver autobegin/read locks may occur.
Primary error/cancellation survives secondary cursor-close failure; successful
close failure remains visible.

This is client metadata/transfer/decode W. Shared SQLAlchemy/DBAPI heap, native
DB engine allocations, total process RSS and physical DB I/O are outside that
explicit allowance. SQLite's native engine is in the same process and may read
or allocate the whole large JSON internally even when no text is delivered.
This is not a universal process/database read-only guarantee. The selected
statements are SELECT/PRAGMA/SELECT-only CTE; fixture creation and Root database
qualification are separate operations. Package reads retain the explicit scoped
I/O limitation described above. Original scalar admission does not change
scientific values, full provenance joins, public APIs or native eligibility.

Original e39/science12 qualification is retained separately. Joined source tests
select frozen published Results37c3 and Contracts1c641, including all15 legitimate
science/technical cache kinds; live Results writer worktrees are not dependencies.
Publication/installed/native producer/scientific acceptance remain separate gates.

Current joined37c3/1c641 census after finite cache prewarm measures binder6 W31832886,
outward5 W26594348, their union W58025576 and all26 cached validators W136603646.
These are census observations before later public reader work, not a runtime peak.
The owning32MiB cache, five6MiB outward and three8MiB fresh allowances still cover
the pinned current domains. Current sorted36-schema raw bytes total531983;
maximum prior-prefix plus decode forecast is7982538W, below8MiB.
Prior e39 corpus/census numbers above remain historical, not current-source totals.

## Original execution catalog metadata

The existing owner operation keeps its required keyword and forwarding signature:

```python
get_native_result_source(run_uuid, execution_id, *, attempt, calcjob_uuid,
                         point_id, binding_uuid=None)
```

An explicit `point_id=None` returns the original execution catalog. Omitting
`point_id` is still an error. A string still selects exactly one recorded point,
with the existing seven response keys and source/creator checks. The catalog
has those same keys, with `point=None`, plus `declared_points`:

```text
{status: "available", point: null, binding: <original whole binding>,
 result_utf8: <exact original raw UTF-8>, native_result: <original NodePin>,
 native_binding: <original NodePin>, integrity: <readback markers>,
 declared_points: [{id, execution_id, temperature_K, voltage_per_period_V,
                    branch, order}, ...]}
```

`declared_points` contains only the selected execution, in the exact original
frozen-plan order. These are declarations, not a used snapshot or a result.
The original binding/raw result still contains only recorded rows. An
uncomputed declared point therefore has no corresponding row; it is not a
manufactured `absent` native source. A recorded `absent`, checkpoint or final
source keeps its exact source, StateRef, invocation and creator. Native point
attempts remain distinct from the integer transport attempt. The existing
`binding.input_plan` and `binding.source_graph` are the genuine original node
pins; the catalog does not mint new provenance identifiers.

Both modes use one complete original readback: scalar CREATE, whole
result/binding/retrieved raw bytes, original frozen plan/graph/inputs, selected
and history rows, and all original creator relations. Optional `binding_uuid`
is checked before that readback. A missing original pair remains unavailable;
no first-point or legacy fallback supplies a catalog. No native payload, current
NFS access, scientific revalidation or new storage authority is obtained.
`integrity.native_contexts=not_reverified`, `current_access=not_checked` and
`scientific_acceptance=not_assessed` retain their original scopes.

The already decoded original plan is reused inside the same operation. Before
constructing the projection, its count, field weights and UTF-8 validity are
checked. Large integer encoding is bounded by its bit length before creating
a decimal string; no new scientific/schema range is imposed. Three
projection/copy/encoding allowances are prepaid on the
existing nonrefundable ledger. The complete catalog JSON is bounded to 2 MiB
before its detached response copy; oversize returns `budget_exceeded` without
truncation. Original 1 MiB file bounds and 320 MiB metadata/256 MiB scoped I/O
allowances remain. The historical L1 limit of 256 points does not apply to this
native catalog. These are source/metadata controls, not measured RSS, physical
DB I/O, current RO access or installed producer/scientific qualification.

The consumer can reuse this one whole binding read for a finite page. Pagination,
J(V) brief reads, RO child lifetime and UI contracts belong to Portal and Results;
this operation does not read every point's scientific payload.
