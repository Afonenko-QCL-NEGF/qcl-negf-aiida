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

Retrieve only fixed `performance-binding.json`, never the performance directory,
catalogs or Parquet. After normal independent scientific validation/pinning the
parser compares its exact result bytes and native point commit pins with the
embedded close sidecar. It retains a SinglefileData and a selection reference
with UUID/SHA/session/commit/summary pins. Missing/corrupt telemetry is unavailable
with a reason; raw remains in retrieved, and scientific outputs/exit checks are
unchanged. Old nodes are not mutated. Borrowed point1 does not acquire the cost
of enclosing execution2; the summary is explicitly execution-attempt scoped.

Native directory counters may exceed the CalcJob input counter after collision.
Only committed rows with exact frozen coordinates/full+scientific fingerprints,
canonical artifact root and verified commit/receipt/state/payload pins use that
exception. The existing parser verifies those facts before association. Greater
uncommitted rows and historical checkpoint overrides are refused. Current/prior
owner lookup uses exact retained raw+science pins and explicit recovery/prior
creator relations, with a bounded per-request proof cache; ambiguous/missing
source ownership stays unknown. Telemetry never authorizes a science association.

`service.get_performance_source(run_uuid, execution_id, attempt=..., calcjob_uuid=...)`
returns retained sidecar UTF-8 and the exact Results expected_binding. Its optional
L1 retained copy shares the existing1MiB metadata inventory; it is omitted there
when the remaining inventory cannot fit it. L1 sealed/quiescence gates remain.

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
