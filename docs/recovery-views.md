# Read-only recovery execution metadata

`service.list_recovery_executions(limit=50, offset=0)` lists exact
`aiida.workflows:qcl_negf.execution_restart` WorkChains. Pagination accepts
1–200 rows and nonnegative offsets, ordered by descending creation time and
primary key. Rows include both standalone and called workflows; `caller` is
null or the UUID/process type of the actual incoming `CALL_WORK` source.
Offset pagination is a current view, not a snapshot under concurrent inserts.

`service.get_recovery_execution(canonical_uuid)` adds the input plan UUID and
stored fingerprint, actual execution ID/attempt, optional recovery bundle and
prior archive input UUIDs, and up to 100 direct `CALL_CALC` children. Children
contain actual attempt, call label, stored job ID/scheduler state, and optional
retrieved/result output UUIDs. Missing optional metadata is null. No attempt is
inferred as 1. Foreign children, ambiguous callers, malformed metadata and
oversized responses fail closed. Queries cap children at 101 and callers at 2
before materialization. Each input/output label is resolved by an exact typed
link query capped at 2 rows, with ambiguous labels refused; ORM input/output
namespaces are never expanded. Plan reads retain the existing 16 MiB bound and JSON
responses retain the 32 MiB bound.

The view reads metadata only. Scheduler state is stored AiiDA metadata, not a
fresh scheduler query. Process success is not scientific acceptance. Bundle and
result UUIDs are associations, not verified scientific evidence. Result JSON,
HDF5, recovery contents and log bodies are not read. This view does not grant
existing Plan-run artifact, export, live selection or process-control authority.
