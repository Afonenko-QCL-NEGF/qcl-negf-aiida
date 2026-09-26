# Deployment

Use AiiDA 2.9.2 with a PostgreSQL profile and a persistent disk-objectstore.
Back up both together; the database alone does not contain the result files.
[`qcl-negf-platform`](https://github.com/AfonenkoA/qcl-negf-platform) owns the
NixOS modules, service accounts, Slurm nodes, runner and application environment.

## Profile and broker

Create the profile as the same Unix account that runs the daemon and API. AiiDA
2.9 provides the built-in `core.zeromq` broker for one controller. RabbitMQ is
also supported by AiiDA when the deployment calls for an external broker. Use
`verdi profile setup core.psql_dos --help` to choose the connection parameters
for the actual site; do not put passwords in Git or command history.

The platform deployment uses a local PostgreSQL socket with peer authentication.
Its bootstrap command configures that profile without a password. For manually
managed installations, follow the [AiiDA installation guide](https://aiida.readthedocs.io/projects/aiida-core/en/stable/installation/guide_complete.html).

Start the configured profile's daemon with `verdi -p qcl-negf daemon start` or
the platform's systemd service. The API must load the same profile. AiiDA owns
job submission, scheduler polling, retrieval and provenance.

## Computer and Code

On a controller with local Slurm submission tools and shared job storage:

```console
verdi -p qcl-negf computer setup --non-interactive --label slurm --hostname localhost --transport core.local --scheduler core.slurm --work-dir /srv/qcl-negf/jobs --mpiprocs-per-machine 1
verdi -p qcl-negf computer configure core.local slurm --non-interactive
verdi -p qcl-negf computer test slurm
```

For a remote login node, use AiiDA's `core.ssh_async` transport and configure it
with `verdi computer configure core.ssh_async`. SSH authentication and host-key
verification belong to that transport configuration, not individual workflow
scripts. The work directory must be accessible to the allocated compute node.

Register a solver build by its immutable Nix store path or an equivalent fixed
release path. Register a new Code when changing the solver build, so older
provenance continues to identify the executable that ran:

```console
verdi -p qcl-negf code create core.code.installed --non-interactive --label qcl-negf-0.2.0 --computer slurm --filepath-executable /path/to/immutable-build/bin/qcl-negf --default-calc-job-plugin qcl_negf.execution --no-with-mpi
verdi -p qcl-negf code show qcl-negf-0.2.0@slurm
```

The path above is a site setting. Verify the solver executable and its closure
are present on **every** Slurm worker before submitting physical work. Setting
`num_machines=2` cannot make this threaded Julia solver distributed. Horizontal
scaling comes from placing independent executions on separate nodes.

## Resource contract

| Field | Meaning | Default |
|---|---|---|
| `num_machines` | Nodes per execution; must be 1 | 1 |
| `num_mpiprocs_per_machine` | Processes per node; must be 1 | 1 |
| `num_cores_per_mpiproc` | Julia threads / Slurm CPUs per task | 1 |
| `max_wallclock_seconds` | Slurm job wall-time limit | 3600 |
| `max_memory_kb` | Slurm memory request in KiB | Cluster policy |
| `queue_name` | Slurm partition | Computer default |
| `account` | Slurm accounting account | Computer default |

The plugin sets `JULIA_NUM_THREADS` to the requested core count and
`OPENBLAS_NUM_THREADS=1`. Solver configuration must leave its thread count on
automatic selection or agree with the requested allocation. Numerical memory
estimates are planning information; Slurm cgroups enforce actual job limits.

To use the WorkChain directly, build a lossless plan node:

```python
from pathlib import Path
from aiida import load_profile, orm
from aiida.engine import submit
from aiida_qcl_negf.data import plan_data
from aiida_qcl_negf.workflow import QCLPlanWorkChain

load_profile()
node = submit(
    QCLPlanWorkChain,
    plan=plan_data(Path("plan.json").read_bytes()),
    code=orm.load_code("qcl-negf-0.2.0@slurm"),
    resources=orm.Dict(dict={"num_cores_per_mpiproc": 8}),
    max_concurrent=orm.Int(2),
)
print(node.uuid)
```

Frozen plans and scientific results use `SinglefileData`, because storing their
floating-point values as AiiDA `Dict` attributes can normalize their precision.
The original JSON bytes are retained and passed unchanged to the solver. Integer
resource requests and artifact inventories can safely use `Dict`.

## Scratch storage

Keep the AiiDA submission directory on shared storage, for example
`/srv/qcl-negf/jobs`, and create `/scratch/qcl-negf` on each compute worker's
local disk. Both paths must be writable by the Slurm job account. On the portal
service, configure `QCL_NEGF_SCRATCH_ROOT=/scratch/qcl-negf`; direct WorkChain
users can add `scratch_root=orm.Str("/scratch/qcl-negf")`. The plugin stores this
input in provenance and passes it to `QCLNEGFRunner.jl` as `--scratch-root`.
No shell interpolation or per-job SSH command is needed.

The runner creates a unique local directory, copies the immutable plan there and
performs the heavy calculation locally. It stages generated files into a
temporary directory beside the shared `result/` destination, verifies their
content, then atomically renames the complete tree into place. AiiDA retrieves
only the published `result/` tree. The output directory must be fresh for every
attempt; results from separate attempts are never merged automatically.

On a 1 Gb/s link, this confines most iterative I/O to the worker. Final staging
still transfers the full result, and AiiDA then archives it in its object store.
Reserve wall time and free space for that transfer. Failed scientific outcomes
remain scientific failures after staging; copying their files does not turn
them into successful calculations.

A hard kill, node crash or failed transfer can leave data only on local scratch.
Those files are recoverable only while that disk remains available. Retention
does not promise a consistent checkpoint at every interruption, and the plugin
does not automatically restart from it. Inspect retained output and runner logs
before any deliberate recovery. Follow the runner's documented cleanup policy;
do not remove scratch directories belonging to active jobs.

Omit `scratch_root` to calculate directly in the shared work directory. This is
useful for small diagnostics and retains AiiDA's ordinary execution behavior.

## Inspect and export

Use `verdi process list`, `verdi process show UUID`, and
`verdi process report UUID`. The portal is a client of the same provenance store.
Its child summaries include `retrieved_uuid`. To work with large numerical
artifacts outside the browser:

```console
verdi -p qcl-negf node repo dump RETRIEVED_UUID ./retrieved-run
```

The destination must not exist. This creates `retrieved-run/result/` and solver
logs. Pass the result directory to the documented `qcl-negf-results` readers.
The API's artifact inventory is calculated once after retrieval, avoiding a
repeated scan of compressed multi-gigabyte files during browser polling.

Cancellation uses the AiiDA daemon and scheduler. If the daemon cannot confirm
termination, the service returns an error and preserves the actual state; inspect
the process before submitting a replacement. No destructive remote-directory
cleanup is performed by this plugin.
