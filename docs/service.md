# Service API

Load the desired AiiDA profile before importing these functions into the request
lifecycle. Use one profile per API process. All functions are synchronous; the
caller should use a worker thread for blocking operations.

`aiida_qcl_negf.service` exports:

| Function | Return value |
|---|---|
| `list_runs(limit=50, offset=0)` | List of workflow summaries; limit 1–200 |
| `get_run(uuid)` | Summary plus child executions and parsed scientific results |
| `get_export_plan(uuid, execution_id, *, attempt=None, calcjob_uuid=None)` | Exact selected-attempt `plan` bytes, source, SHA256 and byte count |
| `submit_plan(plan, code_uuid, resources, label="", *, scratch_root=None, release_id=None, max_attempts=3, retry_backoff_seconds=10, archive_byte_budget=64*1024**3)` | Submitted workflow summary |
| `get_run_report(uuid)` | Latest 1000 workflow/child report entries, chronological |
| `kill_run(uuid)` | Actual terminal summary after confirmed cancellation |
| `list_artifacts(uuid)` | Cached entries with `execution_id`, `attempt`, `calcjob_uuid`, `path`, `size` |
| `get_artifact_metadata(uuid, execution_id, path, *, attempt=None, calcjob_uuid=None)` | One exact cached entry with execution, attempt, CalcJob UUID, path and size; no payload I/O |
| `open_artifact(uuid, execution_id, path, *, attempt=None, calcjob_uuid=None)` | Context manager for a binary stream from that exact attempt |

A summary has `uuid`, `pk`, `label`, `process_state`, `exit_status`,
`is_finished_ok`, `ctime`, and `mtime`. Child summaries also have `execution_id`
and `attempt`, `retrieved_uuid` (null before retrieval). `get_run` adds `plan_fingerprint`,
`children` and `results`, the latter keyed by execution ID. Scientific result
numbers are parsed from lossless file-backed provenance nodes.

The plan workflow publishes a `selections` namespace from the parser's immutable
selection outputs. A service request without explicit selectors uses this
CalcJob UUID and attempt. Multiple attempts without a published selection are
ambiguous and refused. Older single-attempt workflows remain readable. Compact
results are checked against their exact commit/state receipts, so a repeated
completion event cannot select the first similarly named execution by accident.

`get_artifact_metadata` resolves the child once, using the same selection policy
as `open_artifact`. Both explicit selectors constrain the same child; a mismatched
pair never falls back to the published selection. It reads only the child's cached
Dict inventory, with at most 10,000 entries, without repository walk, open, seek,
stat, hashing or payload reads. Missing retrieval, inventory or path raises
`LookupError`; incomplete, oversized or duplicate-path inventory raises
`ValueError`. Pass the returned attempt and CalcJob UUID to `open_artifact` to
keep later streaming pinned if the workflow selection changes. This metadata
operation and streaming API require full UUIDs and normalized relative POSIX paths.

`get_export_plan` reads at most 16 MiB from the selected child's retrieved
`result/scientific_plan.json`. It validates the schema and full decoded plan
against the workflow's file-backed input, without reserializing numerical text.
Only a missing repository entry permits fallback to the original raw
`SinglefileData` input. The returned `source` is
`aiida.retrieved:result/scientific_plan.json` or `aiida.input.plan`; missing
retrieval, oversize, corruption or differing input identity refuses export.
All nodes and stream handles stay with the caller's AiiDA actor; the return
value contains bytes and scalar provenance.

`submit_plan` accepts raw JSON bytes or a JSON string (preferred), or a Python
dictionary. Raw input is preserved byte-for-byte. Browsers must send the file
text rather than parsing and reserializing numbers. The limit is 16 MiB. A valid
plan's cryptographic scientific fingerprint is checked by Julia before executing
physical calculations; Python performs schema and supported-workflow validation.

`scratch_root` is an optional absolute path on the compute node. It is trusted
site configuration, not an HTTP request field. The portal reads
`QCL_NEGF_SCRATCH_ROOT` from its deployment environment and passes it here.
The default `None` runs in the shared AiiDA work directory. The path is stored
as a separate AiiDA input and passed to the runner without shell interpolation.

`release_id` optionally pins a Nix InstalledCode and runs the release admission
guard at `/run/current-system/sw/bin/qcl-negf-release guard`, independent of batch
PATH. Deployment `QCL_NEGF_RELEASE_ID` and `QCL_NEGF_SOLVER_EXECUTABLE` must form a
complete pair; the service uses that release when explicit `release_id` is absent
and rejects conflicts or a differing InstalledCode executable. The shared
`QCL_NEGF_RELEASE_GATE` (default `/srv/qcl-negf/jobs/.release-admission.json`) must
be open for this exact release before dispatch. Missing, malformed and closed
gates refuse submission. Standalone use with no deployment environment keeps the
optional release input absent. Retry attempts must be 1–100, backoff 0–300 seconds. These infrastructure
limits never change SCBA/Poisson budgets. `archive_byte_budget` bounds explicit
transfer of completed full finals into a fresh attempt, independently of recovery
retention. Missing, incompatible or over-budget required archives refuse resume.
These are trusted deployment options; callers must authorize their public exposure.

Errors are `ValueError` for invalid input or exceeded service bounds,
`LookupError` for an absent workflow/artifact, and `RuntimeError` when the daemon
cannot perform or confirm an operation. Only this plugin's WorkChain nodes are
visible through the service, not arbitrary AiiDA processes. UUIDs must be full
UUIDs. Artifact paths must be normalized relative POSIX paths.

The API allows at most 10,000 artifact entries and 32 MiB of metadata in a run
response. An over-limit response is rejected, not silently truncated; use AiiDA
directly to inspect or dump the repository. Report history intentionally exposes
the latest 1000 records, returned in chronological order with the original
`level`, `message`, `time` and `process_uuid` fields; the full log stays in AiiDA.

`get_run_report` also limits the successful report JSON body to 32 MiB
(33,554,432 bytes), including the existing `{"entries": reports}` HTTP envelope.
The exact size is the UTF-8 byte length of compact JSON produced with
`ensure_ascii=False`, `allow_nan=False`, `indent=None` and `separators=(",", ":")`.
All fields, punctuation and JSON escapes count; non-ASCII text counts as UTF-8
bytes. Equality is allowed. An oversized body raises `ValueError` with
`Report metadata exceeds the service response limit; use AiiDA directly` before
returning the list, without truncating records or messages. HTTP headers,
transfer framing and the error response body are outside this successful-body
limit.

This is an aggregate output guard. The ORM query can materialize large logs
before the check, which also allocates a complete temporary JSON string and
encoded bytes. It establishes no bound on database/provider reads, peak RSS or
temporary encoding allocations; their cost has not been measured.

The caller must authenticate requests, restrict the permitted installed Code
UUIDs and cap resource requests against site policy. It must bound artifact
downloads separately. The module provides no network listener, shared secret
store, arbitrary shell execution endpoint, or additional queue.

## Read-only frozen-plan structure

`get_run` additionally returns `plan_structure`, from the same single retained
plan read as `plan_fingerprint` and `plan_points`. Its available shape is
`{status, root: {kind, definition_id}, inclusions, executions, points}`.
Inclusions contain `id,parent_id,definition_id,label,path,execution_ids`;
executions contain `id,definition_id,variant_id,label,operation,point_ids,voltage_mode`;
points contain `id,execution_id,branch,order,initialization,predecessor_id,temperature_K,voltage_per_period_V`.
All values are copied from plan-v2, including `policies.voltage.mode`.
Only explicit `predecessor_id` defines a dependency; array/order adjacency does
not. Inclusion execution membership can be shared by several inclusions.

The pure projection checks unique identities/references, acyclic inclusion
parents, exact execution/point membership and earlier predecessors in the same
execution, branch and temperature, and finite scalar coordinates (no booleans).
Unsupported schemas return `{status: "unavailable", reason: "unsupported_plan_schema"}`;
missing/malformed structure returns reason `unsupported_structure`. This is
presentation availability, not admission, convergence or scientific acceptance.
Other detail fields remain readable. The entire response, including the
projection, retains the existing explicit size refusal without truncation;
over-budget plan metadata refuses before child result reads.

`children` is the existing flattened observed CalcJob descendant inventory,
including every exact attempt UUID and process/exit state, not direct CALL
edges. Execution-level selected results do not establish scientific assessment
for another attempt or any point. The structure reads no retrieved artifacts,
HDF5, resolved inputs or host files and never invokes a solver or command.

## Native research cards

The existing loaded-profile service exports `create_research_card(request_uuid,
raw)`, `edit_research_card(card_uuid, request_uuid, base_version_uuid, raw)`,
`list_research_cards`, `get_research_card`, `list_research_card_versions`,
`read_research_card_version(card_uuid, version_uuid, raw=False)`,
`attach_research_card_run(card_uuid, request_uuid, card_version_uuid, run_uuid,
plan_fingerprint)`, `list_research_card_runs`,
`attach_research_card_report(card_uuid, request_uuid, card_version_uuid,
anchor_run_uuid, report_uuid)`, `list_research_card_reports`, and
`get_research_card_request(request_uuid)`. Lists accept `limit=20, offset=0`.

Card versions are append-only `SinglefileData` retaining exact Contracts raw
UTF-8 bytes (at most 65536). The first version's native UUID is the stable card
UUID. Titles and identical contents never establish identity. Every version has
its own immutable UUID, parent and semantic request hash. Editing requires the
exact current base, and a request replay is resolved before checking that base.
There is no head extra, mutable registry or operation ledger. External writers
are outside the single-owner concurrency contract. Corrupt identity/parent/head
metadata is refused; listing explicitly refuses a catalogue exceeding 10000
version nodes rather than silently omitting cards. Individual lists are
paginated, receipts only; JSON responses are capped at 1 MiB.

Native `Dict` associations pin a card version to a **whole frozen run**, all its
variants and observed attempts, including failed and nonconverged attempts.
Analysis associations pin an existing immutable agent-report UUID and SHA-256.
Before storing an analysis, the owner rechecks its current frozen anchor and
all exact used-run execution/definition/variant/attempt/CalcJob selectors. Each
run requires an explicit association to this stable card and its current
fingerprint, potentially pinned to different versions of this same card. A
report's `question_snapshot` remains independent author prose. No association
changes a plan, fingerprint, attempt, scientific assessment or acceptance.

Mutations return `{status: "stored"|"replayed", kind, receipt}` or a known
pre-store `{status:"rejected", reason}`. Reasons are `not_found`,
`version_conflict` (also `current_version_uuid`), `request_conflict`,
`invalid_binding`, `unsupported_plan`. Unexpected constructor/store/post-store
exceptions propagate to the actor's unknown-outcome fence. A missing request
receipt does not prove no effect or authorize automatic retry. Readback never
removes that fence. The fake-ORM controls are source fixtures, not native UUID,
immutability or restart evidence; the isolated SQLite gate remains separate.
