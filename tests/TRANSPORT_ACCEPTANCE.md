# Bounded synthetic infrastructure rehearsal

This is an operator-run acceptance fixture for Slurm submission, AiiDA
provenance/retrieval, negative-result parsing and archive delivery. It performs
no SCBA, Poisson, operator diagnostic or physical calculation. A native HDF5
contains opaque seeded bytes and explicitly empty diagnostic histories.
`scientific_accepted` and `converged` remain false. The frozen transport plan
preserves the schema's `model_revision` and template configuration, but its
fingerprints identify a **synthetic transport envelope**, not Julia's canonical
scientific plan. Never submit it to the production solver Code.

Use the reviewed integration Git revision and its application closure on both
controller and worker. A private Nix `writeShellApplication` wrapper must run
`APPLICATION/bin/python AIIDA_SOURCE/tests/transport_fixture.py "$@"
--fixture-native-mib 270`. Register this immutable store executable as a
separate `SYNTHETIC-transport-REVISION` InstalledCode on the existing `slurm`
Computer, with plugin `qcl_negf.execution` and `with_mpi=False`. Do not add its
UUID to the portal's submission allowlist. The wrapper is temporary acceptance
composition, not an installed production solver.

## Admission and budget

An infrastructure attempt has a single 1800-second deadline and at most one
active fixture job. At most two attempts are authorized; there is no automatic
retry loop. Run the negative and cancellation jobs **sequentially**. Request
one machine, one process, one CPU, 2 GiB RAM and 300 seconds per job. Native data
is 270 MiB, written in 1 MiB chunks; HDF5/container metadata adds bytes. Fixture
stdout contains only two short events. Keep the runbook/evidence directory
mode0700 and cap collected text logs at 1 MiB.

Before starting, record root revision, deployed application/fixture store
paths, production and fixture Code UUIDs, Computer UUID, mount devices/free
space, `slurmd -C`, scheduler limits, and the attempt start/deadline. The state,
NFS jobs and worker scratch mounts must be real mounted volumes. The API
conservatively admits four times retrieved inventory plus 256 MiB; reserve at
least 2 GiB **additional** free state-disk space for this fixture/export, plus
the separate measured backup/restore allocation. The configured 8 GiB export
budget must be reported by `/api/v1/config`. The native archive is expected to
exceed 200 MB because independent seeded bytes are not repetitive; check its
measured receipt size rather than assuming this.

Use `timeout` with the remaining global deadline for every waiting command.
On deadline/error, stop the attempt and inspect/cancel only its recorded
fixture UUID/job ID. Never cancel an unrelated process to clear the queue.
An unconfirmed kill, missing retrieved object, failed export or failed restore
is a failed infrastructure check. Save evidence and use a fresh empty restore
target for any authorized second attempt.

## Slurm/AiiDA negative result and artifact return

All commands here are preparation instructions; they do not run automatically.
Execute profile commands as `qcl-negf`, with
`AIIDA_PATH=/var/lib/qcl-negf/aiida`, `SLURM_CONF` from the deployed Slurm
configuration, and the pinned application's `verdi`.

```sh
SYNTHETIC_BIN=/nix/store/REVIEWED-fixture/bin/qcl-negf-synthetic-transport-270
AIIDA_SOURCE=/nix/store/REVIEWED-root/components/qcl-negf-aiida
ACCEPTANCE_DIR=/var/lib/qcl-negf/acceptance/ATTEMPT
install -d -m 0700 "$ACCEPTANCE_DIR"
"$SYNTHETIC_BIN" freeze-plan "$AIIDA_SOURCE/tests/fixtures/plan.json" \
  "$ACCEPTANCE_DIR/negative-plan.json" --mode negative
"$SYNTHETIC_BIN" freeze-plan "$AIIDA_SOURCE/tests/fixtures/plan.json" \
  "$ACCEPTANCE_DIR/cancel-plan.json" --mode cancel --wait-seconds 300
sha256sum "$ACCEPTANCE_DIR/negative-plan.json" "$ACCEPTANCE_DIR/cancel-plan.json"
```

Keep a private submission script with the following reviewed body. Invoke it
with `verdi -p qcl-negf run submit_fixture.py IMMUTABLE_FIXTURE_EXE PLAN LABEL`.
Use a `SYNTHETIC-` label unique to the attempt; retain its JSON output privately.
The script never supplies `scratch_root`: Python does not recreate Julia's
verified `stage_result_tree`. That runner boundary needs separate evidence.

```python
import json, sys
from pathlib import Path
from aiida import orm
from aiida_qcl_negf.service import submit_plan
from aiida_qcl_negf.validation import validate_plan

executable, plan_path, label = sys.argv[1:]
if not executable.startswith('/nix/store/') or not label.startswith('SYNTHETIC-'):
    raise ValueError('Require immutable fixture and explicit synthetic label')
plan_bytes = Path(plan_path).read_bytes()
plan = validate_plan(json.loads(plan_bytes))
spec = plan['executions'][0]['provenance']['synthetic_transport']
if len(plan['executions']) != 1 or spec['native_bytes'] != 270 * 1024 * 1024:
    raise ValueError('Require one frozen 270 MiB synthetic execution')
computer = orm.load_computer('slurm')
existing = orm.QueryBuilder().append(orm.Code, filters={'label': label}).all(flat=True)
if existing:
    if len(existing) != 1:
        raise ValueError('Ambiguous fixture label')
    code = existing[0]
    if (not isinstance(code, orm.InstalledCode) or code.computer.uuid != computer.uuid
            or str(code.filepath_executable) != executable
            or code.default_calc_job_plugin != 'qcl_negf.execution' or code.with_mpi is not False):
        raise ValueError('Existing label has different immutable identity')
else:
    code = orm.InstalledCode(label=label, computer=computer,
        filepath_executable=executable, default_calc_job_plugin='qcl_negf.execution',
        with_mpi=False).store()
run = submit_plan(plan_bytes, code.uuid, {
    'num_machines': 1, 'num_mpiprocs_per_machine': 1,
    'num_cores_per_mpiproc': 1, 'max_memory_kb': 2 * 1024 * 1024,
    'max_wallclock_seconds': 300,
}, label=label)
print(json.dumps({'run': run, 'fixture_code_uuid': code.uuid,
                  'computer_uuid': computer.uuid, 'executable': executable}))
```

Poll `GET /api/v1/runs/RUN_UUID` within the global deadline. For the negative
run require WorkChain `finished`, exit400, one child `finished`, exit303,
retrieved UUID present, and result point `completed`, `unconverged`,
`converged=false`. The external fixture exits zero. Record the CalcJob's
`job_id` attribute and scheduler terminal state separately; scientific success
does not follow from that state. `GET /api/v1/runs/RUN_UUID/artifacts` must list
the full HDF5, commit/current pointers, exact frozen plan and bounded stdout.
Compare the retrieved commit's `plan_bytes_sha256`, native object hash and
length with the frozen input and later restored export.

## Big archive through authenticated TLS and resume

The private site's nginx binds `127.0.0.1:443`. Forward a local port through the
approved SSH route: `ssh -N -L 8443:127.0.0.1:443 CONTROL_SSH_ALIAS`. Use
`https://localhost:8443` and the trusted CA/certificate appropriate for its
localhost SAN. Do not disable certificate verification. A mode0600 curl config
`AUTH_CONFIG` supplies the bearer header; keep the token out of command arguments,
URLs and logs. Cookie/header evidence also stays in the mode0700 directory.

```sh
curl --config "$AUTH_CONFIG" --cacert "$TLS_CA" --fail-with-body \
  --json '{"execution_id":"execution-1","profile":"full-state"}' \
  "$BASE/api/v1/runs/$RUN_UUID/exports" --output "$EVIDENCE/export-created.json"
```

Extract the returned `export_id` as `EXPORT_ID`. Poll the protected
`GET /api/v1/exports/EXPORT_ID` for at most 300 seconds and within the remaining
global deadline. Require `state=ready`; preserve the full response and extract
its `receipt` to `receipt.json`. Verify receipt schema
`qcl-negf.science-export.v3`, transport `qcl-negf.export-archive.v1`, one
`.tar.xz` filename, `bytes > 200000000`, SHA256 and snapshot identity. There
must be no multipart fields.

```sh
curl --config "$AUTH_CONFIG" --cacert "$TLS_CA" --fail-with-body \
  --request POST --cookie-jar "$EVIDENCE/cookies" \
  "$BASE/api/v1/exports/$EXPORT_ID/authorize" --output "$EVIDENCE/authorized.json"
curl --cacert "$TLS_CA" --cookie "$EVIDENCE/cookies" --fail-with-body --head \
  "$BASE/api/v1/exports/$EXPORT_ID/download" --output "$EVIDENCE/head.txt"
curl --cacert "$TLS_CA" --cookie "$EVIDENCE/cookies" --fail-with-body \
  --range 0-1048575 --header "If-Range: \"$ARCHIVE_SHA256\"" \
  --dump-header "$EVIDENCE/first.headers" --output "$EVIDENCE/download.tar.xz" \
  "$BASE/api/v1/exports/$EXPORT_ID/download"
curl --cacert "$TLS_CA" --cookie "$EVIDENCE/cookies" --fail-with-body \
  --continue-at - --header "If-Range: \"$ARCHIVE_SHA256\"" \
  --dump-header "$EVIDENCE/resume.headers" --output "$EVIDENCE/download.tar.xz" \
  "$BASE/api/v1/exports/$EXPORT_ID/download"
sha256sum "$EVIDENCE/download.tar.xz"
APPLICATION/bin/python -m qcl_negf_results.archive verify \
  "$EVIDENCE/download.tar.xz" --receipt "$EVIDENCE/receipt.json"
APPLICATION/bin/python -m qcl_negf_results.archive reassemble \
  "$EVIDENCE/download.tar.xz" --destination "$EVIDENCE/recovered"
```

Set `ARCHIVE_SHA256` from the validated receipt. Require HEAD Content-Length
equal receipt bytes, quoted digest ETag, Accept-Ranges=bytes; both range
responses must be206 with exact Content-Range, and the resumed file's byte
count and SHA256 must match the receipt. A fresh client without bearer/cookie
must receive401. Compare the restored HDF5 hash/bytes with the committed native
object, byte-identical frozen plan dependency, and inspect its synthetic
provenance/empty diagnostic tables. Repeat the
export with `science` after the first download completes: its profile must be
science and its native object identical; a new whole download is unnecessary.
The UI's native browser download/resume can be checked separately; curl proves
the HTTP path, not browser download-manager behavior.

## Cancel a second job, then rehearse controller-state restore

Only after the negative job terminates, submit `cancel-plan.json` using a new
synthetic label and the same immutable fixture executable. Wait for its child
to have an assigned job ID and Slurm RUNNING state, then POST
`/api/v1/runs/CANCEL_RUN_UUID/kill`. Within the remaining deadline require the
WorkChain `process_state=killed`, its child terminated and **that recorded job
ID** absent from the active queue. Record its retained Slurm CANCELLED state
when available; a naturally completed/failed job is not cancellation evidence.
Save AiiDA states and scheduler evidence; do not expect complete native
artifacts from a killed uncommitted fixture.

Pause all submission/database/repository writers; require an empty queue.
Measure profile/repository/physical PG sizes and admit a finite archive budget.
On the authoritative controller, use its reviewed state-archive tool:

```sh
qcl-negf-state-archive create /STATE_STAGING/controller-ATTEMPT.tar.gz \
  --source-revision REVIEWED_ROOT_REVISION --disk-budget-bytes DECLARED_BYTES \
  --writers-paused
qcl-negf-state-archive verify /STATE_STAGING/controller-ATTEMPT.tar.gz
```

Copy archive and SHA256 sidecar manually to the external HDD and verify there;
record its trusted hash/copy time. Controller staging alone is not offhost
backup. NFS data needs a separately budgeted complete copy/manifest.

Prepare a **separate offline VM** using the same pinned OS/application closure,
UID3000 and PostgreSQL17. Its separate empty state disk uses the same absolute
profile/PG/Slurm paths. Automatic bootstrap, API, daemon, Slurm, Munge and nginx
must be disabled; there is no production network/NFS mount. PostgreSQL alone
initializes an empty `qcl-negf` DB. Copy the verified external archive/sidecar
to a writable dedicated restore staging directory outside the source/destination
trees: the CLI extracts temporary files beside its input archive. Keep the
trusted external copy intact. Verify the staged copy and finite
restore admission (extraction + profile + Slurm + twice physical PG + at least
1 GiB WAL/index reserve, aggregated by filesystem), then:

```sh
qcl-negf-state-archive restore /RESTORE_STAGING/controller-ATTEMPT.tar.gz \
  --disk-budget-bytes DECLARED_BYTES --writers-paused
```

Keep services offline. As the service user, run `verdi -p qcl-negf status` and
read the backed-up run/Computer/production Code/fixture Code UUIDs. Compare them
exactly, compare exact plan bytes and representative retrieved native object
hashes, and retain PG/version/storage reports. Reconcile bootstrap identity
only after that offline comparison and separately restored NFS/secrets, before
an operator permits one authoritative controller. No such live restore or
server execution is performed by local fixture tests.

## Local evidence

`pytest -q tests/test_transport_fixture.py` creates a 1 MiB native object,
validates the frozen synthetic plan/result/native commit, runs the real parser
against an in-memory adapter, and checks whole-object full-state export bytes.
It starts no AiiDA profile, scheduler or physical solver. It does not prove
live Slurm cancellation, TLS/browser behavior, Julia staging, PostgreSQL
restore or scientific validity.
