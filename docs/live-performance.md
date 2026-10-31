# Ordinary live/performance association

New ordinary CalcJobs may prepare an immutable `capture-spec.json` beside the
exact frozen `plan.json`. Its selection uses the actual CalcJob UUID, execution,
input attempt and full plan fingerprint. A fresh operational token separates
locator publications; it is not a new run/provenance identity. The InstalledCode
path/release guard and scientific argv stay unchanged. B0 is excluded.

The spec requests a fixed104MiB partition inside the existing operational credit:
32MiB performance,64MiB scalar mirror,8MiB metadata/temporaries. It grants nothing
by itself: Runner checks actual existing forecast/free space before publishing
the existing locator. No credit/locator/runtime means ordinary solver plus
unavailable telemetry. No scientific tolerances/model/FP change. CPU width stays
null; `allocation_id=calcjob-process:<UUID>:<input attempt>` is process scope,
not a measured Slurm grant. Root Julia CPU/RSS excludes descendants and cache.

The old fixed `performance-binding.json` v1 sidecar remains raw in retrieved
storage. Its adapter used retired Results keyword arguments. The parser now
reports `performance_status=invalid` and
`performance_reason=legacy_performance_binding_v1_retired` without changing
scientific outputs, recovery checks, or exit status. `verify_sidecar` keeps its
callable signature and explicitly refuses that retired wire. Retained legacy
bytes still require their exact creator/UUID/length/hash; they are unavailable
for typed re-verification.

Native inputs dispatch to a separate parser before legacy inventory. One
nonrefundable metadata320MiB/I/O256MiB operation prepays the reusable Results
128MiB workspace, binder96MiB workspace, and A's full16MiB/64MiB selected-access
lifetime. These are gross admission credits, not actual transferred bytes or
physical RSS. Readers and A close before output construction; genuine CREATE/raw
readback still precedes scalar selection.

The scientific creator is checked separately from source relation and counters.
The binder joins original Code/native argv, typed INPUT/CALL/CREATE links and raw
original script bytes. Its closed stock script grammar accepts only the original
unset or the fixed two-assignment capture export. Extra commands, environment,
paths and counter aliases are refused; previously weakly accepted scripts are
not grandfathered into provenance. Source/native availability and scientific
acceptance remain separate gates.

An absent-only result can produce the original raw `result` and a
`native_binding` SinglefileData pair. The CalcJob's public output mechanism
creates both exact nodes. Only after genuine CREATE/raw readback does it publish
its scalar selection. A scientific negative keeps its original status and exit;
a paused result has no owned native recovery bundle and returns invalid recovery.
Partial failures retain nodes and the primary error; atomic rollback is not
promised. This absent-only engineering control does not close the nonabsent
creator-positive acceptance gate.

`service.get_native_result_source(run_uuid, execution_id, attempt=...,
calcjob_uuid=..., point_id=..., binding_uuid=...)` rechecks original typed
INPUT/CALL/CREATE links, exact ORM files and raw projection entirely offline.
The returned raw UTF-8 and NodeFile pins preserve signed zero and original row
order. Its labels are `native_contexts=not_reverified`,
`current_access=not_checked`, `scientific_acceptance=not_assessed`. A retained
word `verified` is never a fresh HDF/source-access qualification.

`verify_native_performance(reader, telemetry=..., limits=...)` consumes the
public typed Results view. TelemetrySelection fields remain caller assertions;
the reader does not certify their actual ORM origin. A currently returns no
technical operand, so native runtime performance explicitly remains
`native_technical_context_unavailable`. No FolderData telemetry carrier or new
recovery/producer port is added. The exact service signature
`get_performance_source(run_uuid, execution_id, attempt=..., calcjob_uuid=...)`
is preserved and returns that unavailable reason for native selection.

`service.get_live_history_source(..., point_id=..., source_attempt=..., namespace_id=...)`
returns graph/frozen-point mapping only (`live-source.v1`, descriptor_verified=false).
Existing trusted namespace injection must explicitly set live_enabled=true and
match measured storage/Computer identity. It supplies relative_job_root and fixed
`<relative_job_root>/live/current.json`; no client host path or private root is
returned. This active branch does not call the L1 registrar/quiescence binding.
The loaded-profile owner opens no NFS files. A detached Portal child must resolve
the trusted mapping, verify actual descriptor/current point/index hash/generation,
then use the Results same-open selected-copy reader under the existing shared
128MiB I/O/64MiB spool bounds. Mapping alone proves no readable prefix or science.

Root/Platform inject the immutable capture executable through the existing
PythonSet/lock/solver package. Packaging/native mapping, finite runtime checks
and scientific acceptance remain separate gates; source alone closes none of them.

## Native capture preparation

Eligible native CalcJobs choose exactly
`export QCL_NEGF_CAPTURE_SPEC=capture-spec.json QCL_NEGF_NATIVE_CAPTURE_INPUT=native-execution.json`
before stock script generation. Noneligible native jobs retain
`unset QCL_NEGF_CAPTURE_SPEC`. Ordinary spec1 behavior is unchanged. Native
`qcl-negf.capture-spec.v3` has the original eight fields plus the single
`native_performance={run_uuid: actual CALL-chain run UUID}` operand; request ID is
not run UUID. Safe positive provider attempt and null CPU/scratch are required.
Original plan bytes, literal eleven native argv and Code/release stay unchanged.

Only after stock AiiDA creates and fully reads back the unique genuine
`native_submit_script` does preparation qualify the actual original graph through
the actor and generate the compact spec. It sends no unused four-pin producer
DTO. One bounded candidate/token is retained per preparation. Retries re-read
the graph and full original bytes under the same nonrefundable budget.

Before stock sandbox upload, preparation permits either that exact candidate
under the original owned directory identity, or verified absence of both the
fixed target and reserved temporary name. Unknown/stale files, symlinks,
hardlinks, changed parent and exhausted verification credit raise before upload.
The fixed final file is created once with exclusive/no-follow write access0600;
no temporary publication, link, rename or named deletion is performed. The old
reserved temporary name is checked only for absence. A partial or foreign
residual is retained and preparation aborts. After creation, only full SAME_C
on the same own inode with determined closes permits return, including recovery
from an ordinary late optional error. ABSENT optional-off is allowed only before
successful creation; ENOENT after creation cannot prove a renamed inode gone.
Unconfirmed residuals cannot be adopted by a retry. Genuine CREATE and graph
failures remain mandatory; cancellation stays primary when proof or FD close
also fails. No script rewrite, second CREATE, skip-submit flag or
provenance-exclude workaround is used.

Native retrieval declares the five existing controls plus optional fixed
`performance-source.json`. Verified absence yields the genuine core-five
retrieved Folder; observed present and declared yields core six. Raw performance
trees are not retrieved. Opaque sidecar bytes do not provide a public technical
context or replace original access/provenance checks.

SOURCE_L controls exercise actual stock presubmit, private ORM CREATE, selected
Platform script grammar and the existing Runner launcher/Results supervisor with
fresh fake executables. Missing optional spec keeps one solver spawn and its
exit status. Installed native origin, result-root birth/RO lease/NFS, current and
retained available R5, service/package qualification and scientific acceptance
are separate open gates.
