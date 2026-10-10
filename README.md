# QCL-NEGF for AiiDA

Run immutable QCL-NEGF scientific plans through AiiDA, retain their provenance and
retrieve their numerical artifacts. Slurm allocates the compute resources; this
package supplies the scientific CalcJob, parser and parallel WorkChain.

Each execution is one threaded process on one node. Independent executions can
run on different nodes. Continuation points stay within their execution so that
their numerical initialization is preserved. A workflow starts up to four independent
executions in each batch by default; direct WorkChain users can set
`max_concurrent` from 1 to 256. This is an admission cap, not a resource allocator.

## Install

Python 3.14 and AiiDA 2.9.2 are supported. Install the matching
`qcl-negf-contracts` and `aiida-qcl-negf` wheels from the same release artifact
set. Dependencies are specified in `pyproject.toml`. The integration repository
[`qcl-negf`](https://github.com/Afonenko-QCL-NEGF/qcl-negf) owns the shared Python lock and
the flat Git submodule graph. The package is not assumed to be available on PyPI.

```console
uv venv --python 3.14
uv pip install /path/to/wheels/qcl_negf_contracts-0.2.0-py3-none-any.whl /path/to/wheels/aiida_qcl_negf-0.2.0-py3-none-any.whl
verdi plugin list aiida.calculations qcl_negf.execution
```

Use a PostgreSQL-backed AiiDA profile and a configured daemon for deployment.
Register a Computer with `core.slurm` and the supported AiiDA SSH transport, then
register an InstalledCode whose executable is `qcl-negf` and whose default
calculation plugin is `qcl_negf.execution`. The executable must resolve to an
immutable solver build on every compute node. See [deployment](docs/deployment.md).
The `qcl-negf` executable belongs to
[`QCLNEGFRunner.jl`](https://github.com/Afonenko-QCL-NEGF/QCLNEGFRunner.jl); numerical
algorithms belong to [`QCLNEGF.jl`](https://github.com/Afonenko-QCL-NEGF/QCLNEGF.jl).

## Submit a plan

Create the frozen plan with the solver, for example
`qcl-negf plan config/operators.yaml > plan.json`. Submit it from an environment
with this plugin and an AiiDA profile:

```python
from pathlib import Path
from aiida import load_profile
from aiida_qcl_negf.service import submit_plan

load_profile()
plan = Path("plan.json").read_bytes()
run = submit_plan(
    plan,
    code_uuid="YOUR_REGISTERED_CODE_UUID",
    resources={
        "num_machines": 1,
        "num_mpiprocs_per_machine": 1,
        "num_cores_per_mpiproc": 8,
        "max_wallclock_seconds": 3600,
        "max_memory_kb": 16 * 1024 * 1024,
    },
    label="Operator checks",
)
print(run["uuid"])
```

For each CalcJob, the input plan is stored in AiiDA, written to `plan.json`, and
passed to `qcl-negf run-plan plan.json result --execution-id ID --attempt N`.
Retrieval preserves `result/archive`, bounded `result/recovery`, and
`result/executions`, alongside the frozen plan, compact series, publication
manifest and pause receipt. The execution tree includes operator diagnostic
commits, their complete scientific artifact closures, and execution provenance.
Solver logs are retained. Execution trees add storage and transfer costs; the
recovery retention limit does not bound the whole retrieved tree.
The parser validates the result schema, point membership, exact attempt/state
references and artifact hashes before exposing portable recovery.
A process exiting with status zero is still marked failed when its scientific
result failed or any point remained unconverged. Valid partial results remain
accessible. HDF5 numerical format and scientific evidence are independently verified by
[`qcl-negf-results`](https://github.com/Afonenko-QCL-NEGF/qcl-negf-results) when consuming
the numerical artifacts.

For node-local scratch, set the trusted deployment option
`QCL_NEGF_SCRATCH_ROOT=/scratch/qcl-negf` on the portal service, or pass
`scratch_root="/scratch/qcl-negf"` to `submit_plan`. The runner calculates on
the worker's local disk and stages a verified result tree to the shared job
directory before AiiDA retrieves it. This reduces live I/O over the network;
it does not make local checkpoints durable against node or disk failure.
See [scratch storage](docs/deployment.md#scratch-storage).

## Supported boundary

Ordinary frozen plans with independent executions are supported. Campaign admission,
reserve executions, cross-execution dependencies and evidence-gated scheduling are rejected before submission.
No retry silently changes physical parameters or convergence criteria. A failed
execution is recorded; unrelated executions continue. Each execution runs through
`QCLExecutionRestartWorkChain`, based on AiiDA's `BaseRestartWorkChain`. Defaults
are three total attempts and a ten-second delay in the next scheduler job.
The maximum and delay are finite, configurable inputs, separate from numerical
iteration budgets. Nonconvergence is terminal and preserves its scientific final.

Pause continues only from a hash-verified pause receipt and complete recovery
generation. Missing-result and supported scheduler failures can retry only when
AiiDA confirms the previous job stopped and has a verified recovery bundle.
Ambiguous ownership, missing recovery and invalid results terminate explicitly.
The next CalcJob receives the unchanged plan and `--recovery-bundle recovery`
in a separate working directory. Its hash-bound `execution_progress.json` carries
completed point records. Their full finals transfer through a separate
`prior_archive` FolderData input and `--archive-bundle prior-archive`; missing,
corrupt or over-budget archives refuse startup. The default `archive_byte_budget`
is 64 GiB, configurable separately from the 8 GiB recovery bound. Completed
points retain their original attempt identities and are not recomputed.
AiiDA preserves previous attempts as provenance; retrieved generations, selected
recovery and prior archive FolderData inputs add explicit storage and transfer
costs. The two-generation retention bound applies within each attempt and is
not a global AiiDA repository quota.

Optional `release_id` pins an immutable Nix `InstalledCode` path and runs
`/run/current-system/sw/bin/qcl-negf-release guard` before the solver. Slurm scripts disable independent
requeue and carry a base64 JSON attempt descriptor for the platform shutdown
adapter. Local tests inspect these scripts without submitting to Slurm.
CPU transfer, shared-filesystem durability and real Slurm node failure remain
hardware verification tasks.
The service accepts explicit `release_id`, `max_attempts`,
`retry_backoff_seconds` and `archive_byte_budget` deployment options.
When runtime `service.env` supplies `QCL_NEGF_RELEASE_ID` and
`QCL_NEGF_SOLVER_EXECUTABLE`, the service binds that identity automatically,
requires the selected InstalledCode executable to match, and checks the shared
`QCL_NEGF_RELEASE_GATE` before submission. Partial configuration, closed admission
and explicit conflicts are rejected. Standalone use without deployment environment
does not invent a release identity or require a gate.

The service API in `aiida_qcl_negf.service` is used by
[`qcl-negf-portal`](https://github.com/Afonenko-QCL-NEGF/qcl-negf-portal). It needs an already
loaded profile. Authentication, allowed Code UUIDs and request limits belong to
the caller. See [service API](docs/service.md).

## Agent report files

The loaded-profile service exposes `save_agent_report(anchor_uuid, raw)`,
`list_agent_reports(anchor_uuid, limit=20, offset=0)` and
`read_agent_report(anchor_uuid, report_uuid)`. It appends an immutable
`agent-report.json` SinglefileData node after the Contracts UTF-8 format and
all frozen run, execution, variant, attempt and CalcJob references are validated.
Original bytes (at most 262144) are preserved, including author whitespace.
The returned receipt contains the native UUID, filename, byte count, SHA256,
creation time and anchor. Repeating a save creates another file; no deduplication
or retry guarantee is provided.

Listing filters schema and anchor in the database, orders newest first and
limits each page to 1–100 receipts. It reads metadata only; the encoded
`reports` response remains bounded by 32 MiB. Reading opens only the selected
file, reads at most 262145 bytes and verifies format, size, hash and anchor
attributes before returning the exact bytes. Corrupt or mismatched files fail
explicitly. These operations do not submit jobs, read scientific results, alter
process logs or change assessments. Author conclusions, including “accepted”,
are prose and do not establish numerical or scientific acceptance.

This is a partial report view for a run. A process UUID is not a stable research
card; canonical questions and cross-run card relations still depend on R01.
Different referenced roots assert an author relationship only. Authentication
and loaded-profile ownership remain the caller's responsibility.

## Development

Use the integration repository's Python 3.14 environment and shared lock, or
install the contracts wheel followed by `uv pip install -e '.[test,build]'`.
The [bounded transport rehearsal](tests/TRANSPORT_ACCEPTANCE.md) provides an
explicitly synthetic native-payload fixture for infrastructure acceptance,
with 1 MiB local validity tests and an explicit 270 MiB live-fixture flag.
It performs no physical calculation and never enters the production Code allowlist.

Run `pytest`. Tests use an isolated SQLite AiiDA profile and actual local CalcJob
execution with a synthetic solver program. They test scheduler submission,
retrieval, provenance, scientific-failure handling and independent parallel execution. They
do not certify physical calculations or access a production Slurm cluster.

Source: [GitHub](https://github.com/Afonenko-QCL-NEGF/qcl-negf-aiida). License: MIT.
