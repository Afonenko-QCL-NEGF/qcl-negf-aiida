# Native publication engineering checks

The engineering route uses the source-owned Runner `native-publication-fixture`
command and the existing [native actor runbook](../docs/native-actor.md). It uses
public Core constructors and normal Runner publication without SCBA or Poisson.
Its point is intentionally failed/invalid and unconverged. Process exit zero,
Parser `302 ERROR_SCIENTIFIC_FAILURE`, and root WorkChain
`400 ERROR_EXECUTION_FAILED` are separate expected outcomes. None establishes
scientific acceptance.

The obsolete Python native-v4 producer and its standalone installed rehearsal
script have been removed. There is **no supported standalone installed
acceptance entry point in this test directory**. Installed native rehearsal
remains **OPEN** until the operator has qualified the current native composition
and its original inputs through the owners described below. A successful local
test or an existing input node cannot supply that qualification.

## Current route and ownership

Use the actual solver InstalledCode from the selected release. Do not register
a replacement Python Code or bypass the submission service. The native Computer
uses `qcl_negf.native_slurm` and `qcl_negf.native_local`; Platform owns the
protected jobs root, launch/close state, stopped-writer gate and selected read
access. The native actor runbook describes the original INPUT/CALL/CREATE links,
mandatory Runner origin arguments and five-control transfer. The HDF5 files
remain selected native sources; ordinary whole-tree retrieval is not this route.

The source-owned profile is [native-publication-plan.json](fixtures/native-publication-plan.json).
Its full constructor profile, source histories, model and grid are fixed. The
AiiDA selector chooses `native-publication-fixture` for that profile, preserves
the original input bytes, and omits the legacy `--attempt` argument. A changed
physical parameter, grid or source history with the reserved profile ID must
refuse before upload. Ordinary plans continue to select `run-plan`.

The public admission port is `aiida_qcl_negf.service.submit_plan`. An operator
must already have the **genuine approved stored original** plan, source graph and
native execution binding, the matching Code/Computer/release, and qualified
native access. Supply the exact original plan bytes, the source graph node, and
`native_executions={"execution-1": original_execution_node}` with `max_attempts=1`
and the explicit release. Service validates their existing UUID/bytes/SHA joins
and deployment admission; it does not create an authoritative native binding.
The current [service API](../docs/service.md) and native actor runbook own these
ports. This document supplies no replacement setup helper, invented binding,
actor certificate or close receipt.

Before a live attempt, the operation owner records fresh host resources,
immutable installed composition and actual Code/Computer/input UUIDs, source
hashes, storage ownership and a finite check budget. Follow the native actor
runbook for exact launch, job identity, transfer, close and selected-access
readback. Failure or unknown ownership blocks the dependent action. Protect
unrelated jobs and storage. Installation and live submission are separate
explicit stages; the local checks below perform neither.

## Finite local controls

In the prepared Python 3.14 workspace:

```sh
python -m pytest -q tests/test_transport_fixture.py
python -m pytest -q tests/test_capture_submission.py
```

The replacement transport module tests public service admission of the exact
stored constructor inputs with one permitted transport child, rejection of
changed model/grid/history before upload, and actual native Parser302 output
retention through terminal root400. These use genuine isolated ORM links and
bounded broker/access test ports. The parser control uses an explicitly absent
failed source; it does not manufacture a Python scientific state or qualify a
native writer.

The existing native actor, parser and selected-source tests provide the
remaining exact controls: original input identity, complete StateRefs, creator
proof, close/access prerequisites, five-control transfer, read budgets and
cleanup failures. Their simulated transport/namespace observations remain L
controls. Run the affected cases selected from those modules for a release
change; do not treat their count as native installation evidence.

A separate artifact check may read a **previously emitted, explicitly supplied**
Runner constructor result. It must use public `StateReader` with the exact
selected StateRef and expected identity, `FinalDiagnosticsReader`,
`FinalObservationsReader`, and `describe_checkpoint_inputs`. Verify the original
plan/model bytes and references, failed/unconverged status and explicit
not-measured assessments. The fixture is engineering data, not an old scientific
run or an experimental reference.

For a whole-object export use `export_snapshot(reader, destination,
profile="full-state", byte_budget=..., reserve_bytes=...,
diagnostics_ref=..., observations_ref=...)` with the explicit selected reader
and exact references. Verify the single archive and reassemble into a fresh
destination through the public archive API. Compare every selected object
length/SHA and the original plan/model bytes. Export establishes byte integrity
and selection, not scientific acceptance or exact restart readiness.

## Acceptance gates that remain OPEN

| Gate | Required measured evidence | Local controls do not establish |
| --- | --- | --- |
| Installed native negative attempt | Current qualified release/Code/Computer, genuine original inputs, owned Slurm job, process exit zero, Parser302/root400, retained exact result/binding, Platform close and selected source access | An isolated SQLite graph or constructor file alone |
| Large archive delivery | Explicit selected source and measured archive size, server storage reserves, full object hashes | A small constructor export or a former arbitrary payload-size promise |
| Authenticated TLS/range/resume | Trusted certificate, authorization, exact HEAD/ETag/length, measured206 ranges and final digest | Local archive creation or browser download behavior |
| Cancellation and ownership | Cancellation of the recorded job, confirmed process/writer stop and terminal scheduler evidence | A completed negative attempt or a mocked wait |
| Controller backup/restore | Consistent paused-writer backup, external verified copy, separate offline restore, exact DB/repository/input/result identity readback | SQLite tests, exported scientific objects or copied fixtures |
| Scientific acceptance | Separately commissioned physical study and independent criteria/evidence | Any transport/process/schema/constructor result |

The integration repository's
[single-server deployment runbook](https://github.com/Afonenko-QCL-NEGF/qcl-negf/blob/main/docs/single-server-deployment.md)
owns server installation and backup/restore. The native actor runbook owns native
transport prerequisites and limitations. TLS/range, cancellation and large
archive delivery require separate explicit measured attempts. No retired
Python wrapper, whole-HDF retrieval command, legacy `result_commit` or
`scientific_accepted` envelope is an active acceptance instruction here.
