# Native actor and ordinary attempt bridge

This opt-in source boundary connects AiiDA provenance to Platform's ordinary
attempt owner. Slurm owns queue and resource policy. The same CalcJob retains
its job ID and submission state. This module does not accept science, restore a
checkpoint, create a second retry worker, or qualify an installed deployment.

Use a separately registered Computer with scheduler `qcl_negf.native_slurm`
and transport `qcl_negf.native_local`. Its AuthInfo requires
`native_computer_uuid`, an exact ORM selector. Existing Computers and Codes must
not be relabelled. Package entry-point metadata alone does not install or
qualify a Computer, a console script, or a privileged caller.

## Original inputs and script

The native input branch uses original `SinglefileData` nodes for plan,
source graph, and each execution binding. Their original bytes and UUIDs are
retained. The binding uses the existing `qcl-negf.aiida-native-execution.v1`
fields; its receipt strings and constructor assertions grant no authority.
Plan execution order maps to `native_executions.execution_<index>`. The same
plan, graph, Code and selected execution node pass through the existing
RestartWorkChain into INPUT_CALC links. Native admission requires a one-child retry policy until the separate recovery
frontier integration exists. The immutable transport counter can be larger than
one in a retained same-attempt context; it is not a native point counter.
B0, legacy recovery bundles and prior archives are not native aliases.

The frozen plan provides expected per-point model hashes before launch.
Actual original resolved model bytes are a later Runner/result/checkpoint
obligation. An expected hash cannot stand in for its preimage or scientific
acceptance. Native point attempt counters remain separate from the CalcJob
transport `Int`; the latter must not become a native Runner attempt argument.

The native branch requires `QCL_NEGF_NATIVE_JOBS_ROOT`, a normalized absolute
selector provided by its daemon environment. Missing or invalid values fail
before claim; there is no legacy `result` directory fallback. It constructs
output argv from that selector and the original relative execution root. This
environment value is untrusted: Platform P2 must independently compare actual
argv with its protected `policy.jobs_root` and original relative root before
claim. That Platform check and protected daemon wiring remain unavailable.
The native command uses original plan/source-graph files and omits legacy
`--attempt`. Its `--source-graph` producer port is currently an unpublished
Runner overlay, so this source branch must not activate against an older
installed Runner.

Service admission resolves the original plan through the existing full
`native_execution.input_plan` NodeFile UUID. It requires the exact stored
SinglefileData, length, SHA and original bytes equal to the submitted plan.
A fresh equivalent UUID, newest row, hash scan or mutable extras cannot select
an original plan. Graph and execution leaves must already be stored.

Native presubmit retains the actual generated `_aiidasubmit.sh` as the same
CalcJob's `native_submit_script` SinglefileData output. Public
`Process.update_outputs()` stores its genuine CREATE link. Readback must match
every original byte. Repeated UPLOAD reuses only that exact node and script;
conflicts are not replaced. No atomic DB/filesystem transaction is claimed.
The raw CalcJob input repository is not this immutable source: AiiDA's upload
engine itself updates that repository after the process node is stored.

If presubmit storage/readback fails, preserve the original exception. AiiDA
2.9.2 wraps presubmit exceptions in `PreSubmitException`; this earlier failure
can leave the process EXCEPTED before any native claim or external dispatch.
It is not the same-node pause/play guarantee for an unknown submit outcome.

## Public read ports

`native_actor.load_native_launch(calcjob_uuid, *, maximum_metadata_bytes)`
reads the actual stored INPUT/CALL/CREATE graph and returns the existing P1
Launch fields. Active unsealed processes are valid: public AiiDA APIs forbid
adding new INPUT links once a ProcessNode is stored. A CALL label by itself is
not a unique process identity. Actual link types, directions, UUIDs and
cardinalities are required.

`read_native_execution` has the same arguments and returns original execution
binding file bytes. `native_calcjob_uuid(*, computer_uuid,
remote_workdir=None, job_id=None, maximum_metadata_bytes)` requires exactly
one selector and a unique exact match, with query limit two. Runtime workdir
and job ID are mutable observations; they are reread and never treated as
immutable INPUT certificates. Mutable extras and newest-row selection are not
proof sources.

These library calls require a profile already loaded by their owner. The
`qcl-negf-native-actor --profile PROFILE {launch|execution} CALCJOB_UUID
--maximum-metadata-bytes N` console entry reads an existing named profile;
it does not create, migrate or bootstrap one. The execution verb emits exact
original bytes without appending a newline. Failure emits a bounded diagnostic
without DB credentials. Root must select the actual immutable APP, profile
and namespace from protected deployment facts. Invoking this CLI manually or
receiving its dictionary does not authenticate a native actor.

## Submission and unavailable gates

NativeSlurmScheduler finds the exact CalcJob by Computer and upload workdir.
It invokes the fixed Platform client with the existing Request protocol.
A qualified `bound` reply returns the numeric ID to the ordinary AiiDA
execmanager, which writes it to the same CalcJob. HOLD, failed authentication,
unknown outcome, malformed/truncated reply and command timeout raise
`PauseInterruption`, bypassing ordinary submit backoff. Playing the same
SUBMIT_COMMAND can reconcile the same durable P1 key. It never authorizes a
second sbatch, releases a claim after a timeout, or constructs another child.
Missing ACK stays HOLD until a trustworthy owning reconciliation exists;
absence from squeue/sacct or a matching comment is insufficient.

Polling preserves ordinary Slurm status parsing. RUNNING requests P1's actual
start/allocation observation. Missing jobs may become the stock DONE
observation; DONE grants neither writer closure nor retrieval. A finite shared
adapter operation bounds commands, captured output, metadata workspace and
child cleanup. A batch shares its deadline and read debit; it does not reset a
window for each job. Package-internal read helpers reuse that same ledger;
this is not a new public budget/authority framework. Import/ORM memory and
deployed DB latency still require native measurement.

Currently **get, getfile and gettree always HOLD** because the owning public
Q1 control-export/close port is absent. `ignore_nonexisting=True`, a supplied
dictionary, a stored result, or scheduler DONE cannot bypass this gate. A
future separately reviewed integration must qualify the entire declared
small control set before its first copy, preserve original selected input
FileRefs, and verify bounded same-FD bytes and local readback. Scientific
HDF/history payloads stay in their single NFS copy. Checkpoint plan/model
selection remains outside the closed native-source schema, and A4 checkpoint
contexts keep `.plan` and `.model` null.

Cancellation likewise remains HOLD until Platform provides a fresh
same-generation admission. Its present retained status/job ID is insufficient
to authorize scancel. No scheduler/retry owner is added by either guard.

Platform's fixed actor-loader/caller ancestry/profile/namespace bridge,
canonical worker output mapping, source-owned Q1 publication and CP verifier,
current-generation cancellation, parser CREATE/readback integration and
native installation are separate gates. Existing P1 currently holds on the
missing actor binding. Source tests and default HOLD behavior do not qualify
these missing production paths or scientific/optical acceptance.
