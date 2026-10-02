# Service API

Load the desired AiiDA profile before importing these functions into the request
lifecycle. Use one profile per API process. All functions are synchronous; the
caller should use a worker thread for blocking operations.

`aiida_qcl_negf.service` exports:

| Function | Return value |
|---|---|
| `list_runs(limit=50, offset=0)` | List of workflow summaries; limit 1–200 |
| `get_run(uuid)` | Summary plus child executions and parsed scientific results |
| `get_export_plan(uuid, execution_id)` | Exact `plan` bytes, source, SHA256 and byte count |
| `submit_plan(plan, code_uuid, resources, label="", *, scratch_root=None)` | Submitted workflow summary |
| `get_run_report(uuid)` | Latest 1000 workflow/child report entries, chronological |
| `kill_run(uuid)` | Actual terminal summary after confirmed cancellation |
| `list_artifacts(uuid)` | Cached entries with `execution_id`, `path`, `size` |
| `open_artifact(uuid, execution_id, path)` | Context manager for a binary stream |

A summary has `uuid`, `pk`, `label`, `process_state`, `exit_status`,
`is_finished_ok`, `ctime`, and `mtime`. Child summaries also have `execution_id`
and `retrieved_uuid` (null before retrieval). `get_run` adds `plan_fingerprint`,
`children` and `results`, the latter keyed by execution ID. Scientific result
numbers are parsed from lossless file-backed provenance nodes.

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

Errors are `ValueError` for invalid input or exceeded service bounds,
`LookupError` for an absent workflow/artifact, and `RuntimeError` when the daemon
cannot perform or confirm an operation. Only this plugin's WorkChain nodes are
visible through the service, not arbitrary AiiDA processes. UUIDs must be full
UUIDs. Artifact paths must be normalized relative POSIX paths.

The API allows at most 10,000 artifact entries and 32 MiB of metadata in a run
response. An over-limit response is rejected, not silently truncated; use AiiDA
directly to inspect or dump the repository. Report history intentionally exposes
the latest 1000 records; the full log stays in AiiDA.

The caller must authenticate requests, restrict the permitted installed Code
UUIDs and cap resource requests against site policy. It must bound artifact
downloads separately. The module provides no network listener, shared secret
store, arbitrary shell execution endpoint, or additional queue.
