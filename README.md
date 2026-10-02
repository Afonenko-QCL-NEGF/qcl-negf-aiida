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
passed to `qcl-negf run-plan plan.json result --execution-id ID`. The complete
`result/` directory and solver logs are retrieved into AiiDA's object store.
The parser validates the result schema, plan identities and point membership.
A process exiting with status zero is still marked failed when its scientific
result failed or any point remained unconverged. Valid partial results remain
accessible. HDF5 contents and artifact checksums are verified by
[`qcl-negf-results`](https://github.com/Afonenko-QCL-NEGF/qcl-negf-results) when consuming
the numerical artifacts; this parser does not duplicate that reader.

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
execution is recorded; unrelated executions continue. Submit a new
workflow for a deliberate new attempt. No cross-execution restart is implied.

The service API in `aiida_qcl_negf.service` is used by
[`qcl-negf-portal`](https://github.com/Afonenko-QCL-NEGF/qcl-negf-portal). It needs an already
loaded profile. Authentication, allowed Code UUIDs and request limits belong to
the caller. See [service API](docs/service.md).

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
