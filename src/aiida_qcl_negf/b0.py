"""Versioned genuine AiiDA B0 adapter; native producer activation is fail closed.

The native port must authenticate Root's existing escrow and install interceptors
BEFORE returning a permit: repository/temp/file, DBAPI+server WAL/tail, daemon
broker, scheduler/transport. It owns finite sequence/call/debt/STOP counters across
controller and daemon processes. A tuple, bool, SQL row limit or this Python class
is never native authentication. The unconfigured port deliberately refuses.
Imports/profile bootstrap itself requires Root's prepaid startup child allowance.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from uuid import UUID

B0_CONTROL_FILES = ("result/b0-control.json", "result/b0-report.json")
B0_REPORT_LIMIT = 1024 * 1024
ROOT_HELPER = "/run/current-system/sw/bin/qcl-negf-b0-slot"


class B0Refusal(RuntimeError):
    pass


@dataclass(frozen=True)
class ControllerProducerProfile:
    """Inclusive upper bounds; STOP and all descendant copies are inside these."""
    version: str = "aiida-2.9.2-b0-controller-v1"
    resident: int = 2 * 1024**3
    read: int = 8 * 1024**3
    write: int = 8 * 1024**3
    logs: int = 128 * 1024**2
    memory: int = 2 * 1024**3
    poll_calls: int = 65536
    scheduler_submits: int = 1
    requeues: int = 0
    retrievals: int = 1
    transfer_retries: int = 0
    db_calls: int = 65536
    sql_request: int = 32768
    sql_response: int = 65536
    stop_resident: int = 64 * 1024**2
    stop_read: int = 128 * 1024**2
    stop_write: int = 128 * 1024**2
    stop_logs: int = 8 * 1024**2


PROFILE = ControllerProducerProfile()


@dataclass(frozen=True)
class ProducerBinding:
    family: str
    version: str
    domains: tuple[str, ...]
    enforcement_handle: object
    receipt_reference: str


@dataclass(frozen=True)
class AuthenticatedControllerBinding:
    """Native-authenticated records; construction alone confers no authority."""
    handle: object
    producers: tuple[ProducerBinding, ...]
    profile: ControllerProducerProfile
    deadline_ns: int
    memory_bytes: int
    registry_reference: str
    operation_graph_sha256: str
    stop_reference: str


@dataclass(frozen=True)
class RootIntentReceipt:
    handle: object
    intent_reference: str
    plan_sha256: str


@dataclass(frozen=True)
class NativeControllerPermit:
    handle: object
    intent: RootIntentReceipt
    profile: ControllerProducerProfile


def _opaque(value):
    return value is not None and not isinstance(value, (bool, dict, str, bytes, int))


def _binding(value):
    if (type(value) is not AuthenticatedControllerBinding or not _opaque(value.handle)
            or value.profile != PROFILE or type(value.producers) is not tuple
            or len(value.producers) != 4
            or type(value.deadline_ns) is not int or not 0 < value.deadline_ns < 2**64
            or type(value.memory_bytes) is not int or not 0 < value.memory_bytes <= PROFILE.memory
            or not value.registry_reference or not value.stop_reference
            or not isinstance(value.operation_graph_sha256, str)
            or len(value.operation_graph_sha256) != 64
            or any(c not in "0123456789abcdef" for c in value.operation_graph_sha256)):
        raise B0Refusal("Typed authenticated all-used producer binding required")
    if {p.family for p in value.producers if type(p) is ProducerBinding} != {
            "repository_stream", "orm_database_server_tail", "daemon_broker", "scheduler_transport"}:
        raise B0Refusal("Missing controller native producer coverage")
    for producer in value.producers:
        if (not producer.version or not producer.domains or not producer.receipt_reference
                or not _opaque(producer.enforcement_handle)):
            raise B0Refusal("Native producer bounds/version/domain proof unavailable")
    return value


class NativeControllerBackend:
    """Trusted installation port, not an extension point for request JSON.

    admit must verify credentialled Root capability and the exact producer graph,
    versions, finite RAM/deadline/calls, physical+allocated/inode enforcement,
    DB/WAL/TOAST/index/autovacuum drain and broker/scheduler tails. All methods
    use the SAME escrow child registry. Unknown coverage refuses before effects.
    preconsume is irreversible even on exception; bind/refine never grants again.
    operation must install complete interceptors including asynchronous engine
    persistence, status polling, Slurm retry0 and retrieval sender/receiver. A
    reserved disjoint read/report lease + worker quiescence/ACK precede transfer.
    """
    def authenticate_installation(self, bootstrap_fd, profile):
        raise B0Refusal("Root authenticated producer installation unavailable")

    def admit(self, capability_fd, profile):
        raise B0Refusal("Authenticated controller producer backend unavailable")

    def preconsume_intent(self, authenticated, digest, metadata):
        raise B0Refusal("Root intent consume unavailable")

    def bind_controller(self, consumed, profile):
        raise B0Refusal("ControllerPermit unavailable")

    def resume(self, reference, node_uuid):
        raise B0Refusal("Authenticated daemon controller binding unavailable")

    def operation(self, permit, family):
        raise B0Refusal("Producer operation interceptors unavailable")

    def refine(self, permit, role, node_uuid):
        raise B0Refusal("Root actual node refinement unavailable")

    def claim_submit(self, permit, role, node_uuid):
        raise B0Refusal("Irrevocable single enqueue claim unavailable")

    def freeze_stop(self, permit, primary):
        raise B0Refusal("Reserved controller STOP unavailable")

    def read_delivery(self, permit, repository, files, limit):
        # Must use registered bounded streams and authenticate terminal receipt,
        # actual identity and controlled read descriptor; no repository.walk.
        raise B0Refusal("Quiescent bounded report/read channel unavailable")


_native_backend = NativeControllerBackend()


def install_native_controller_backend(backend, bootstrap_fd):
    """Only trusted Root bootstrap calls this, before loading any AiiDA profile.

    Authentication must verify live credentials/native producer enforcement;
    receipt fields themselves are structural checks, never the authenticator.
    """
    global _native_backend
    if not isinstance(backend, NativeControllerBackend):
        raise B0Refusal("Typed native controller backend required")
    _binding(backend.authenticate_installation(bootstrap_fd, PROFILE))
    _native_backend = backend


@dataclass
class B0Slot:
    """Root bootstrap gives an authenticated fd; request mappings are rejected.

    The process-local latch is an additional guard. Native escrow remains the
    durable cross-process authority and must retain uncertain consumes forever.
    """
    capability_fd: int
    _spent: bool = False

    def consume(self, digest, metadata):
        if self._spent:
            raise B0Refusal("B0 intent already used or uncertain")
        if type(self.capability_fd) is not int or self.capability_fd < 0:
            raise B0Refusal("Root capability descriptor required")
        authenticated = _binding(_native_backend.admit(self.capability_fd, PROFILE))
        # Mark BEFORE calling Root; an uncertain response must never retry.
        self._spent = True
        consumed = _native_backend.preconsume_intent(authenticated, digest, metadata)
        if (type(consumed) is not RootIntentReceipt or not _opaque(consumed.handle)
                or consumed.plan_sha256 != digest or not consumed.intent_reference):
            raise B0Refusal("Authenticated Root intent receipt required; slot stays spent")
        permit = _native_backend.bind_controller(consumed, PROFILE)
        reference = consumed.intent_reference
        if (type(permit) is not NativeControllerPermit or not _opaque(permit.handle)
                or permit.intent != consumed or permit.profile != PROFILE
                or not isinstance(reference, str) or not reference or len(reference) > 128
                or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in reference)):
            raise B0Refusal("Opaque native permit and immutable slot reference required")
        return ControllerPermit(_native_backend, permit, reference)


class ControllerPermit:
    def __init__(self, backend, permit, reference):
        self.backend, self.native, self.reference = backend, permit, reference
        self._submitted = set()
        self._bound = set()

    @contextmanager
    def operation(self, family):
        if family not in {"create_plan_and_workchain", "enqueue_workchain", "workchain_setup",
                          "create_calcjob", "enqueue_calcjob", "prepare_bound_calcjob",
                          "parse_bound_report", "workchain_inspect", "workchain_finalize"}:
            raise B0Refusal("Unknown controller producer operation")
        try:
            with self.backend.operation(self.native, family):
                yield self
        except BaseException as error:
            try:
                self.backend.freeze_stop(self.native, type(error).__name__)
            except BaseException:
                pass  # Original failure remains primary; all debt stays spent.
            raise

    def refine(self, role, node):
        if role in self._bound or role not in {"workchain", "calcjob"} or not node.is_stored:
            raise B0Refusal("Actual stored UUID refinement is one way")
        node_uuid = str(UUID(node.uuid))
        self._bound.add(role)  # Unknown refinement is also spent.
        self.backend.refine(self.native, role, node_uuid)

    def enqueue(self, role, process, runner):
        if (role not in self._bound or role in self._submitted
                or process.runner is not runner):
            raise B0Refusal("Single enqueue requires stored bound identity and the same runner")
        if role == "calcjob" and not runner.is_daemon_runner:
            raise B0Refusal("CalcJob enqueue requires the actual daemon broker runner")
        self._submitted.add(role)
        # Native registry must claim once BEFORE persister/broker effects; daemon
        # redelivery must not replay a scheduler submit or construct another job.
        self.backend.claim_submit(self.native, role, process.node.uuid)
        if role == "workchain":
            # Public AiiDA2.9.2 launch.submit supports an existing Process with
            # the same cached runner even when broker_submit=False. It checks
            # controller/persister and checkpoints/continues through the daemon.
            # Never replace or close the service's global runner/event loop.
            from aiida.engine import submit
            return submit(process)
        return runner.submit(process)


def resume_controller(reference, node_uuid):
    permit = _native_backend.resume(reference, node_uuid)
    if not isinstance(permit, ControllerPermit) or permit.reference != reference:
        raise B0Refusal("Authenticated daemon permit required")
    return permit


def validate_retrieval(paths):
    if type(paths) not in (tuple, list) or tuple(paths) != B0_CONTROL_FILES:
        raise B0Refusal("Only exact bounded B0 control/report retrieval is admitted")
    return B0_CONTROL_FILES


def submit_bound_plan(raw, digest, metadata, code_uuid, resources, label, *, slot,
                      release_id, scratch_root, max_attempts):
    """Concrete AiiDA 2.9.2 store→bind→daemon enqueue, under native ports."""
    if type(slot) is not B0Slot:
        raise B0Refusal("A Root B0Slot capability is required")
    if type(max_attempts) is not int or max_attempts != 1:
        raise B0Refusal("B0 requires max_attempts=1 and no automatic restart")
    if len(metadata["executions"]) != 1:
        raise B0Refusal("B0 admits exactly one execution")
    UUID(code_uuid)
    if not isinstance(label, str) or len(label) > 255:
        raise ValueError("Label must be at most 255 characters")
    from .validation import scheduler_options, validate_scratch_root
    scheduler_options(resources)
    if scratch_root is not None:
        validate_scratch_root(scratch_root)
    controller = slot.consume(digest, metadata)
    with controller.operation("create_plan_and_workchain"):
        # Even imports/profile/gate reads are after ControllerPermit. Startup of
        # the containing service still needs its separate prepaid bootstrap.
        from aiida import orm
        from aiida.manage import get_manager
        from .data import plan_data
        from .service import _bound_release_code, _summary
        from .workflow import QCLPlanWorkChain
        code, release = _bound_release_code(code_uuid, release_id)
        runner = get_manager().get_runner()
        inputs = dict(code=code, plan=plan_data(raw), resources=orm.Dict(dict=resources),
                      release_id=orm.Str(release), max_attempts=orm.Int(1),
                      max_concurrent=orm.Int(1), retry_backoff_seconds=orm.Int(0),
                      b0_reference=orm.Str(controller.reference), metadata={"label": label})
        if scratch_root is not None:
            inputs["scratch_root"] = orm.Str(scratch_root)
        process = runner.instantiate_process(QCLPlanWorkChain, **inputs)
        process.node.store()
        controller.refine("workchain", process.node)
    with controller.operation("enqueue_workchain"):
        node = controller.enqueue("workchain", process, runner)
        return _summary(node)


def validate_delivery(delivery, *, execution_id, plan_fingerprint, node_uuid):
    """Validate detached bounded metadata after native channel authentication.

    Native read_delivery is responsible for all stream sizes and ACK/hash proof;
    these checks are structural, and deliberately cannot manufacture that proof.
    """
    if not isinstance(delivery, dict) or set(delivery) != {
            "execution_id", "calcjob_uuid", "attempt", "plan_fingerprint",
            "local_status", "raw_delivery", "execution_status", "scientific_verdict",
            "read_channel", "quiescence_ref"}:
        raise B0Refusal("Incomplete B0 delivery descriptor")
    if (delivery["execution_id"] != execution_id or delivery["calcjob_uuid"] != node_uuid
            or type(delivery["attempt"]) is not int or delivery["attempt"] != 1
            or delivery["plan_fingerprint"] != plan_fingerprint
            or delivery["local_status"] != "local_verified"
            or delivery["raw_delivery"] not in {"raw_delivery_pending", "retrieved"}
            or delivery["execution_status"] not in {"completed", "failed", "unknown"}
            or delivery["scientific_verdict"] not in {"not_measured", "failed", "pass"}
            or not isinstance(delivery["quiescence_ref"], str) or not delivery["quiescence_ref"]):
        raise B0Refusal("B0 report identity/status mismatch")
    channel = delivery["read_channel"]
    if (not isinstance(channel, dict) or set(channel) != {"reference", "max_request_bytes", "retry"}
            or not isinstance(channel["reference"], str) or not channel["reference"]
            or type(channel["max_request_bytes"]) is not int
            or not 1 <= channel["max_request_bytes"] <= B0_REPORT_LIMIT
            or type(channel["retry"]) is not int or channel["retry"] != 0):
        raise B0Refusal("Working controlled finite read/report channel required")
    return delivery
