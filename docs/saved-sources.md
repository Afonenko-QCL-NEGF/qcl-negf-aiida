# L1 historical saved-source registration

`aiida_qcl_negf.saved_sources` implements an administrative operation on the
existing sole loaded-profile AiiDA owner. There is no registration HTTP route,
new Data plugin, registry daemon, scheduler or storage migration. Calculation,
parser, workflow, recovery and retrieval are unchanged. L2 external-primary
execution/recovery is not implemented or enabled by this module.

`configure_saved_sources(namespaces)` installs trusted process-local site
configuration. It does not read storage or establish a native BIND. No namespace,
mount, storage identity, hold, retention owner or read budget is invented.
Each namespace requires `root` (approved local NFS alias), `remote_root` (measured
worker path of the same namespace), `storage_identity`, `computer_uuid`,
`registration_source_id`, and `admissions` indexed by exact CalcJob UUID.
An admission requires run_uuid, execution_id, attempt, remote_folder_uuid,
relative_result_root, storage_identity, scheduler_stopped=true,
writers_stopped=true, hold_ref, access_binding_ref, retention_policy_id,
retention_owner_id, and a finite positive maximum_read_bytes. The hold covers
the entire closed root/declared closure, indefinite retention until explicit
operator disposition, and no writer/import/prune. These are named site-owner
assertions from an admitted packet, not measurements inferred by this code.
Private roots never enter the public saved-source record.

`register_saved_source(run_uuid, execution_id, *, attempt, calcjob_uuid,
request_uuid, namespace_id, retention_policy_id, expected_result_sha256,
expected_result_bytes, maximum_metadata_bytes)` requires both exact selectors.
The existing `_export_child` establishes run/child membership. Terminated child,
actual code/remote-folder Computer, remote-folder creator and original plan
nodes are checked. The relative result root is computed from that RemoteData's
actual remote path plus the owning calculation contract's `result` directory;
there is no UUID-shard/path reconstruction. The exact admission must match it.

The original child.outputs.result node and NFS series_result.json are both
mandatory before Data constructors or retention/store. Missing either gives
`source_series_required`, detail `no_series_registration_deferred`, even if
closed commits or pause/stop files exist. Exact retained series bytes and positive
byte-count must match the supplied SHA/count and the NFS file. Present corrupt
series is corrupt_result; digest/count conflicts are source_identity_mismatch.
Closed negative results are supported without any acceptance filter.

Captured files are small JSON only. Point commits come exclusively from the
captured series point.data.result_commit; mutable current pointers and directory
scans are never used. The input plan is retained exactly and its input-node UUID,
byte SHA and scientific fingerprints are recorded. The NFS scientific plan must
have the same exact semantic JSON values; its original bytes are captured
separately. Point coordinates/membership, native identity, commit/receipt
SHA/state, declared analysis owner and declared model JSON are checked. Analysis
payload is not opened or hashed; integrity.payload remains not_measured.
copy_status remains unknown: the presence of a retrieved node alone does not
prove complete raw mirroring. Existing historical copies are not removed. A
receipt's verified status does not become new physics/payload acceptance.

row_attempt remains the captured row attempt. source_attempt is the actual
commit/native identity attempt. Borrowed resource-pause lineage must explicitly
name checkpoint_source_attempt and the owning pause provenance fields. Source
CalcJob comes only from current identity or the child's explicit recovery_bundle/
prior_archive creator relation with matching execution/attempt/plan UUID. Absent
or ambiguous legacy relation stays null with a reason; a matching attempt number
from unrelated descendants is never authority. Imported bytes remain attributed
to their actual source attempt while the physical owning root is the selected
closed CalcJob's root. Optional stop/pause receipts only cross-check these same
series/commit anchors. Captured metadata and retained input bytes are read back
before storage. Quiescence/hold is an admission assumption, not a guarantee
against a hostile storage administrator.

A single standalone immutable native `orm.Dict` stores the registration and all
small retained bytes atomically in one store. Its internal envelope has
qcl_saved_source_kind, request_uuid/request_sha256, exact selection scalars,
record_json (canonical JSON string), and retained_metadata (path→exact UTF-8 text).
Encoding JSON as strings avoids ORM float normalization. Public `record` is the
READY qcl-negf.aiida-saved-source.v1 object, with its complete source SHA computed
from canonical JSON bytes. Each metadata row binds path, byte-count, SHA and
retained_node_uuid: series_result.json points to the original child result node;
other captured NFS files point to this source Dict. Original plan authority is
record.plan.input_node_uuid/sha256, and returned `plan` is its exact retained
UTF-8 string. Thus a semantically identical NFS plan with different whitespace
never pretends to be original input-node bytes.

All admissions and exact envelope-size checks precede construction. A placeholder
self UUID has the exact canonical UUID width; substitution does not increase
size. One owner serializes writes. Same request UUID+meaning replays the same
immutable node; different meaning refuses. Store exceptions return unknown with
the generated source UUID. The in-process pending fence never constructs a
replacement; next same request queries native immutable evidence first. A
persisted node reconciles to replayed. Absent evidence remains unknown in that
owner, without retries. After owner failure/restart, native readback is required
before any new administrative action; this is not a multi-writer distributed
unique-key service. Group, extras and ctime/latest ordering are not authority.

`service.get_saved_sources` and `service.get_saved_source` expose detached read
operations with mandatory execution/attempt/CalcJob. A pinned source UUID/SHA
remains stable if another registration appears; multiple candidates without
explicit source UUID require selection. These reads rebind the actual AiiDA
owner graph, original plan/result bytes and captured point commit/receipt mapping.
They never access NFS, a mount, HDF5, archive/export or solver. Storage outage
therefore does not remove retained brief metadata. Access readiness is explicitly
not_checked, to be checked by the separately admitted Portal/Platform provider.

Bounds: maximum256 captured points, total captured metadata≤1MiB, immutable
internal envelope/public response≤2MiB. Registration delivered reads (including
readback/retained bytes) obey the explicit site's maximum_read_bytes, independently
of small response size. Detached reads/catalogue share an8MiB logical read
admission and at most256 records; larger requests fail budget_exceeded. No
HDF5/full payload is opened, no full copy or historical mirror is deleted.
Safe regular-file opens reject root/ancestor/final symlinks, absolute/../backslash/
NUL paths, and use read-only O_NOFOLLOW. No empty mount/local fallback/mkdir is
created. Actual mount identity/read access/retention/liveness and native BIND
remain separate operator evidence. One-copy delivery/recovery, NFS power-loss,
backup/restore, R04 and full GUI/scientific acceptance are not established here.

Prepared targeted tests: `tests/test_saved_sources.py`, in the booked existing
Python environment only. No install/profile bootstrap or production storage
access belongs to that focused gate.

## Original selection pins and acknowledgement cross-checks

L1 additionally requires the actual immutable `child.outputs.selection` produced
by the native parser. Its creator must be that child; its CalcJob/execution/
attempt/result SHA must match the exact original series. Each captured committed
point must match the original point pin's path, commit SHA, full native identity,
state ID and state sequence. A coherent replacement commit/receipt pair with the
same series path therefore cannot replace an already pinned original state.
A legacy child without this authority is explicitly unavailable:
`source_selection_required`, detail `retained_point_pin_unavailable`. Neither
hold/quiescence nor a newly read NFS receipt fabricates the original pin.

The pin's canonical native Dict value is retained under the reserved metadata
path `_aiida/selection.json`, with the original selection node UUID. This is an
AiiDA metadata identifier, **not an NFS filename**. GET rebinds that original
selection node/creator and its point pins without NFS access. Its exact canonical
value/digest is included in the metadata set and source hash. This does not
change the native scientific file format or the original series byte anchor.
Recorded native CalcJob identity must agree with the graph source relation;
current-source recorded run identity also cannot name a different run. Borrowed
point native source attempts remain unchanged; absent original source relation
stays null rather than being attributed to the current CalcJob.

The same pure `_validate_record` is called before any ORM constructor/store and
on readback. All existing record/storage-class/count/metadata digest guards are
shared. Invalid/missing storage_class and more than1024 metadata entries refuse
before node creation; finite public response size is admitted as well as internal
envelope size. Raw series/plan are not reserialized to make these checks pass.

Stop acknowledgements require the exact saved terminal series status, result
SHA, selected attempt/execution and local_filesystem publication scope. Pause
acknowledgements require paused series/point/status, matching point ID and the
same native commit/state/source attempt. In a borrowed-source case the native
acknowledgement belongs to the source attempt; it is never rewritten to the row
attempt and does not substitute for the current closed-root site admission.
Both acknowledgement branches use shared checks at capture and retained GET.
These comparisons preserve recorded failure/unknown outcomes and do not evaluate
new physical acceptance or rehash physical payloads.
