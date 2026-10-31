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

# Retained optional model-help inputs

The optional `model_help_assets` input is existing immutable `FolderData`,
forwarded by the Plan and Restart workchains. It retains the original Root
`release.json`, `bundle.json` and selected `help/fragments/<id>.json` alongside
the original `source_graph` input. The trusted owning submitter supplies their
release/build association. That opaque source/review association is not yet
retained by this adapter: it creates no execution-specific release receipt and
mapping remains unavailable. The service compares exact bytes, graph, fragment
inventory and immutable INPUT links; it never reconstructs historical assets
from the current deployment. No full serving, source-review or physical-origin
authority is inferred from this minimal mapping. Full bound documentation still
requires the existing complete Contracts bundle/source/review/payload proof.

`get_model_help_input` keeps the context-only call when `fragment_id=None`.
An explicit requested fragment adds detached `retained` operands in the existing
Contracts validator layout. Original retrieved/index CREATE and INPUT/CALL joins
precede availability. Native capture uses the existing complete original result,
binding, scalar selection and source/creator/epoch readback. Its local point
attempt may differ from the child transport attempt: `selection.capture_context`
contains the LocalContext, while the View/release context keeps the selected
child transport identity. A prior source or a matching path alone cannot create
this association. Missing source/creator evidence leaves USED unavailable.

The parser emits metadata inventory and the optional help index before native
and ordinary scientific early returns, including missing or failed results.
Index `valid` describes Snapshot shape and its original path/plan passport
identity; it does not grant a creator association or scientific acceptance.
Invalid optional JSON remains raw in the original retrieved repository and
does not replace the scientific exit. Inventory sizes use repository metadata;
the inventory does not open HDF5. A complete index is required for availability.

Native retrieval adds only `('result/executions', '.', 2)`. The owning sandbox
must have no `result` namespace. Immediately before the existing submit action,
the scheduler checks the original workdir owner and namespace absence; unknown
or preexisting state removes only this optional tuple, preserving all five
scientific controls and the optional performance source. This does not replay
a submit or change scientific inputs. Runner independently requires a fresh
owned namespace and publishes exact immutable bytes. Tuple presence alone is
not transport authority: the adapter also requires the existing trusted original
launch/workdir/closed packet joins. All other tree retrieval remains HOLD.

The only admitted extra repository root is `result/executions/E/P/attempt-N/`
containing `model-help-inputs.json`. Every E/P belongs to the selected frozen
execution. The transport checks complete metadata membership, owners, regular
files, link count, no symlinks/alias, count, individual and aggregate sizes before
any payload read or destination creation of its own. Stock AiiDA creates the
parent `result/` for the fixed tuple before invoking the adapter. Unsupported
files, arrays and arbitrary root namespaces are never transferred as help.
No scientific/NFS execution tree is recursively copied. The prescribed literal
local `./result/executions` suffix is supported without normalizing arbitrary
unsafe paths. Destination descendants are created exclusively.

Operational limits remain 64 KiB per Snapshot, 8 MiB total Snapshot bytes,
min(selected frozen points, 10000) entries, and 2 MiB encoded Index/Bundle.
The existing nonrefundable native ledger supplies separate help credit: 256 MiB
logical metadata/work/copy credit and 32 MiB I/O credit. Before decode, a
conservative 512-bytes-per-input-byte workspace is reserved; original raw
buffers, retained/temporary/transport/readback copies and asset decodes receive
separate reservations. A smaller credit can make help unavailable before the
byte ceilings are reached. These accounting limits are not measured peak RSS,
total Python/AiiDA/PostgreSQL allocations, physical database I/O or campaign
quotas. Installed mirror/retrieval and total runtime memory remain separate N
measurements. All handles must close successfully before a detached reply;
cancellation remains primary when a secondary close also fails.

Stock AiiDA retrieval may create an empty local `result/` parent before the
optional tuple is read. An empty parent represents unavailable help and does not
change the original control packet; any nonempty parent must pass the strict
`result/executions` metadata grammar. Optional namespace or aggregate rejection
preserves scientific control retrieval; a user cancellation still propagates.

The additional serialized namespace census admits at most10000 total directory
and file nodes (including root), depth32, bounded component/path lengths and
2MiB conservative metadata-string weight. It iterates public stored
`NodeRepository.metadata` without `walk`, `deepcopy`, `list`, or
`Repository.from_serialized` first. ORM has already hydrated that metadata; this
is explicitly outside the additional census/heap accounting. Namespace
exhaustion returns incomplete, potentially below the raw Snapshot/Index caps.

Per-object sizes use a Root-qualified narrow adapter: AiiDA2.9.2,
disk-objectstore1.5.0, exact DiskObjectStoreRepositoryBackend, SHA256 keys,
original stored owner/repository identity and core.psql_dos/core.sqlite_dos.
The public owner.backend.get_repository and serialized metadata supply the
association; the sole private provider._container is used as a closing context
for public get_object_meta (with_streams=False: SQL metadata or loose stat).
The operation prepays and caches admitted size records. Unknown versions,
providers, owners and unstored metadata are refused before payload effects.
NodeRepository.get_object_size/as_path must not be used here: AiiDA2.9.2
implements that apparent size probe as a full payload copy. No HDF5/Snapshot
payload is opened merely to find its size. Current genuine qualification is PG;
the shared SQLite provider type is guarded, not a new SQLite runtime claim.

Optional submit preflight has separate prepaid original-load credit and keeps
the mandatory native16MiB budget intact. Destination ENOSPC/EDQUOT recovers
only after all exclusively created descendants are removed in reverse creation
order using held parent identities; unknown entries are never recursively
deleted. Foreign replacement/unsafe cleanup remains HOLD. Cancellation and
fatal/invariant/uncertain-close errors preserve their primary object; these
errors cannot publish a partial help tree or a successful detached DTO.


The optional native reread of the original plan prepays the existing 16 MiB
actor envelope from help credit. Additional packet/namespace proofs and optional
source-handle checks, including the original session close, use that same help
ledger. The mandatory operation retains its original 16 MiB cap and all default
checks. Accounting does not change what is proved: an actual original job,
namespace, file identity or ancestry mismatch still produces HOLD.


Ordinary scientific parsing obtains only its required path scope from public
serialized metadata, independently of optional HELP provider/version/size
qualification: fixed `result/series_result.json`, fixed
`result/pause-receipt.json`, and the bounded `result/recovery` subtree. Only
recovery receipt candidates are passed to the existing `select_recovery`; its
manifest/state/archive/byte/identity verification remains authoritative. HELP
`result/executions` and unrelated empty directories cannot change a scientific
exit. The required recovery namespace retains finite entry/depth/string caps
and file-bound refusal. Inventory/index completeness remains honest metadata;
it no longer supplies an unrelated global veto on the ordinary science verdict.

Native actor CREATE readback also uses separate HELP credit for optional root,
typed plan/execution queries, raw plan and strict help tree admission. The
original mandatory16MiB actor workspace and exact scientific controls are
unchanged. Typed provider refusal and terminal optional resource denial do not
accept help; malformed mandatory membership/bytes, namespace/security mismatch,
cancellation and fatal/cleanup failures remain failures.

AiiDA2.9.2 copies and stores FolderData inside `with transport`, before
transport.close. Consequently the adapter never promises rollback of stored
HELP through an already removed SandboxFolder. Owned destination handles stay
open only through pre-return source/destination checks and close before
`_get_help` returns. Recoverable refusal at these stages removes only recorded
exclusive descendants, closes HELP handles and detaches the failed ledger.

Before the first optional destination effect, the existing HELP budget burns
one concrete reservation for the three later source-close checks. Let P be the
number of frozen optional namespace roots, F the sum over frozen source trees
of one root-fstat probe plus ancestry/binding/held/absence/technical-FD probes,
and W the bounded shape-copy allowance derived from those actual roots/names.
`_proc_bytes` requests at most65537 bytes for each status/mountinfo stream and
at most130 bytes for boot_id (including EOF requests). With B=2*65537+130,
reserved metadata is3*(W+P*(8*B+4*512)+512*F), IO is3*P*B, and file-probe quota
is3*(4*P+F). W is4096 plus, per held source tree,4096+8*(root length+binding
name lengths+held name lengths+absence name lengths). These are logical work
credits, not measured heap/RSS/physical-IO bounds.

The prepayment is never refunded/refilled and cannot pay for ordinary work.
Close consumes the already burned counters, never calls a failed ledger, and
checks frozen namespace/tree/FD counts before constructing its bounded shape.
Count/shape growth, full source-directory metadata changes, foreign replacement,
forecast underflow, deadline/cancellation, IO and actual FD-close failures remain
HOLD/fatal/primary failures. Thus a correctly forecast close cannot introduce a
new legal resource refusal after store; the reservation itself may honestly
refuse HELP before destination creation. Ordinary CONTROL retrieval pays none
of these new HELP costs. Original mandatory close and performance proofs remain.


Stored binary source reads use `StoredLeafReader` for the ordinary result and
recovery operands, the HELP-declared actor control arm, and every repeated
`_native_result` read. The default actor without HELP retains its original
repository path. A genuine stored owner and its exact NodeRepository association
are required. Only the requested canonical relative path is traversed in the
already ORM-hydrated public serialized metadata: at most32 components,2048 path
characters,256 per component, and one file leaf with an opaque key of at most
1024 characters. No unrelated children are enumerated or copied.

The public `owner.backend.get_repository().open(key)` context returns the
provider's original binary stream. This avoids NodeRepository's lazy whole
`Repository.from_serialized` construction on cold nodes. Backend instances may
be fresh on each call; optional HELP package/version/provider qualification
never governs mandatory scientific/control reads. The backend key is not a raw
byte hash or provenance claim. Existing handle/read/workspace credits, seek/read
sizes, raw-byte/hash comparisons and unchanged recovery/result validators still
apply. A malformed selected file/key gives the existing invalid-result exit;
a missing result keeps the missing-result exit. Refused HELP siblings cannot
force a whole-tree materialization through a subsequent scientific open.

Clean close failure prevents a successful return; secondary close failure adds
a note to the original read, fatal or cancellation exception. ORM hydration and
public backend internals remain opaque costs; these path/key caps are additional
lookup bounds, not a bound on ORM decoding, copies or process RSS.


Admitted HELP payloads now use this same leaf operand: the production parser
passes it to `build_index`, selected snapshot and retained asset roles open via
their stored owners, and the service uses it for input/retrieved frozen plans
when a HELP ledger is present. The minimal `build_index` binary-reader protocol
is unchanged; it grants no ORM source access on its own. No full File hierarchy
is created for these FolderData reads, even for an admitted namespace with2000
empty siblings. No new tree-copy formula, ledger, refund or cap was introduced.

A terminal `SourceError` credit refusal is propagated before the malformed-byte
classification. An already failed ledger is refused at index entry, and no
final C1/encoding/projection follows a failed JSON reservation. The existing
parser optional handler publishes `complete=False, entries=[]`; original valid
bytes are not called malformed. Actual malformed payloads retain their invalid
classification. With the default256MiB credit, nine independently validated
60000-byte synthetic snapshots can exhaust JSON work below the8MiB aggregate
payload cap; this is honest HELP unavailability, not a scientific failure.

The `ledger=None` legacy service, generic report/artifact APIs and default actor
remain unchanged. The retained SourceGraph still uses the valid stored
SinglefileData invariant (exactly one file validated before store), bounded raw
read and explicit source joins; it does not enumerate FolderData. ORM hydration,
provider internals and whole-process RSS remain opaque.
