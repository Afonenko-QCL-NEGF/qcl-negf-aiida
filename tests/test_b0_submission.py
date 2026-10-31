"""Literal injected ORM/broker/transport fixtures; no native proof or science.

These fixtures are intentionally not executed by the source-only ticket. Running
this file still imports AiiDA and needs the separately authorized runtime packet.
"""
from contextlib import contextmanager
import json
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiida import orm
import aiida.manage
import aiida.engine

from aiida_qcl_negf import b0, data, service

CODE_UUID = "00000000-0000-0000-0000-000000000001"
NODE_UUID = "00000000-0000-0000-0000-000000000002"
SOLVER = "/nix/store/b0-solver/bin/qcl-negf"


class FakeNative(b0.NativeControllerBackend):
    """Test double only: receipt construction is NOT native authentication."""
    def __init__(self, events):
        self.events = events
        self.spent = set()
        self.claims = set()
        self.missing = None
        self.uncertain_consume = False

    def admit(self, fd, profile):
        self.events.append("native_admit")
        if self.missing:
            raise b0.B0Refusal(self.missing)
        producers = tuple(b0.ProducerBinding(family, "fixture-v1", ("fixture-domain",), object(), "fixture-receipt")
            for family in ("repository_stream", "orm_database_server_tail", "daemon_broker", "scheduler_transport"))
        return b0.AuthenticatedControllerBinding(object(), producers, profile, 10**18, 1024**3, "fixture-registry", "a" * 64, "fixture-stop")

    def preconsume_intent(self, authenticated, digest, metadata):
        self.events.append("root_consume")
        if 7 in self.spent:
            raise b0.B0Refusal("intent spent")
        self.spent.add(7)
        if self.uncertain_consume:
            raise OSError("lost Root acknowledgement")
        return b0.RootIntentReceipt(object(), "fixture-slot", digest)

    def bind_controller(self, receipt, profile):
        self.events.append("controller_permit")
        return b0.NativeControllerPermit(object(), receipt, profile)

    @contextmanager
    def operation(self, permit, family):
        self.events.append("operation:" + family)
        yield

    def refine(self, permit, role, uuid):
        self.events.append("refine:" + role)

    def claim_submit(self, permit, role, uuid):
        key = (role, uuid)
        if key in self.claims:
            raise b0.B0Refusal("enqueue spent")
        self.claims.add(key)
        self.events.append("claim:" + role)

    def freeze_stop(self, permit, primary):
        self.events.append("reserved_stop")


class FakeORM:
    """Every constructor, including SinglefileData, is an observable effect."""
    def __init__(self, events):
        self.events = events
        self.fail_constructor = False
        self.code = self.InstalledCode()
        self.code.filepath_executable = SOLVER
        self.code.default_calc_job_plugin = "qcl_negf.execution"
        self.code.computer = SimpleNamespace(scheduler_type="core.slurm")

    class InstalledCode:
        pass

    def load_code(self, uuid):
        self.events.append("db_load_code")
        return self.code

    def SinglefileData(self, **kwargs):
        self.events.append("SinglefileData")
        if self.fail_constructor:
            raise OSError("repository constructor failed")
        return SimpleNamespace(**kwargs)

    def Int(self, value):
        self.events.append("orm_Int")
        return SimpleNamespace(value=value)

    def Str(self, value):
        self.events.append("orm_Str")
        return SimpleNamespace(value=value)

    def Dict(self, **kwargs):
        self.events.append("orm_Dict")
        return SimpleNamespace(**kwargs)


class FakeRunner:
    # Normal cached service runner; top-level public submit must work with this.
    is_daemon_runner = False

    def __init__(self, events):
        self.events = events
        self.fail_store = self.uncertain_submit = False
        self.submits = 0
        self.controller, self.persister = object(), object()
        self.processes = []

    def close(self):
        raise AssertionError("Do not close the shared service runner/event loop")

    def instantiate_process(self, cls, **inputs):
        self.events.append("process_constructor")
        node = SimpleNamespace(uuid=NODE_UUID, is_stored=False)
        def store():
            self.events.append("process_store")
            if self.fail_store:
                raise OSError("unknown store acknowledgement")
            node.is_stored = True
            return node
        node.store = store
        process = SimpleNamespace(node=node, inputs=inputs, runner=self)
        self.processes.append(process)
        return process

    def submit(self, process):
        if not self.is_daemon_runner:
            raise AssertionError("Cached runner.submit must not enqueue the top-level WorkChain")
        self.events.append("daemon_runner_submit")
        return self._queue(process)

    def _queue(self, process):
        # Literal broker producer boundary: one queued process only.
        self.events.append("broker_enqueue")
        self.submits += 1
        if self.uncertain_submit:
            raise OSError("unknown broker enqueue outcome")
        return process.node


class FakeManager:
    def __init__(self, runner):
        self.runner = runner

    def get_runner(self):
        return self.runner

    def set_runner(self, runner):
        raise AssertionError("Do not replace the manager's shared runner")

    def create_runner(self, **kwargs):
        raise AssertionError("Do not create another runner for this API fix")


@pytest.fixture
def plan():
    # Manufactured current-wire input; B0 assertions do not establish native provenance.
    return json.loads((Path(__file__).parent / "fixtures/export-plan/current-plan-v2.json").read_bytes())


@pytest.fixture
def fake(monkeypatch):
    events = []
    native = FakeNative(events)
    fakeorm = FakeORM(events)
    runner = FakeRunner(events)
    manager = FakeManager(runner)
    state = SimpleNamespace(events=events, native=native, orm=fakeorm, runner=runner, manager=manager,
                            admission={"open": True, "release_id": "release-b0"})
    monkeypatch.setattr(b0, "_native_backend", native)
    # b0 imports `from aiida import orm` after permit; replace concrete exported
    # constructors rather than bypassing the actual service/data call chain.
    for name in ("SinglefileData", "Int", "Str", "Dict", "InstalledCode", "load_code"):
        monkeypatch.setattr(orm, name, getattr(fakeorm, name))
    monkeypatch.setattr(aiida.manage, "get_manager", lambda: manager)
    def public_submit(process):
        events.append("public_submit_instance")
        assert process is runner.processes[0]
        assert process.runner is manager.get_runner()
        if runner.controller is None:
            raise RuntimeError("No process controller")
        if runner.persister is None:
            raise RuntimeError("No persister")
        return runner._queue(process)
    monkeypatch.setattr(aiida.engine, "submit", public_submit)
    monkeypatch.setattr(service, "get_daemon_client", lambda: SimpleNamespace(is_daemon_running=True))
    monkeypatch.setattr(service, "_summary", lambda node: {"uuid": node.uuid})
    monkeypatch.setenv("QCL_NEGF_RELEASE_ID", "release-b0")
    monkeypatch.setenv("QCL_NEGF_SOLVER_EXECUTABLE", SOLVER)
    monkeypatch.setenv("QCL_NEGF_RELEASE_GATE", "/fixture/admission.json")
    def gate_open(path, mode):
        import json
        events.append("gate_read")
        return BytesIO(json.dumps(state.admission).encode())
    monkeypatch.setattr(service.Path, "open", gate_open)
    return state


def call(plan, slot=None, **kwargs):
    return service.submit_plan(plan, CODE_UUID, {}, b0_slot=slot or b0.B0Slot(7), max_attempts=kwargs.pop("max_attempts", 1), **kwargs)


def test_consume_permit_precede_all_constructor_gate_and_db_effects(plan, fake):
    assert call(plan) == {"uuid": NODE_UUID}
    e = fake.events
    assert e.index("root_consume") < e.index("controller_permit")
    for effect in ("gate_read", "db_load_code", "SinglefileData", "orm_Int", "orm_Str", "orm_Dict", "process_constructor", "process_store"):
        assert e.index("controller_permit") < e.index(effect)
    assert e.index("process_store") < e.index("refine:workchain") < e.index("claim:workchain") < e.index("broker_enqueue")
    assert e.index("claim:workchain") < e.index("public_submit_instance") < e.index("broker_enqueue")
    assert fake.runner.is_daemon_runner is False
    assert fake.manager.get_runner() is fake.runner
    assert fake.runner.submits == 1
    assert "daemon_runner_submit" not in e


@pytest.mark.parametrize("missing", ["repository bounds", "DB/WAL server tail", "broker debt", "scheduler pipes"])
def test_uncovered_producer_zero_effects(plan, fake, missing):
    fake.native.missing = missing
    with pytest.raises(b0.B0Refusal):
        call(plan)
    assert fake.events == ["native_admit"]
    assert fake.runner.submits == 0


def test_unconfigured_backend_refuses_before_effects(plan, fake, monkeypatch):
    monkeypatch.setattr(b0, "_native_backend", b0.NativeControllerBackend())
    with pytest.raises(b0.B0Refusal):
        call(plan)
    assert fake.events == []


@pytest.mark.parametrize("attempts", [2, 3, True])
def test_retries_refused_before_root_or_orm(plan, fake, attempts):
    with pytest.raises(b0.B0Refusal):
        call(plan, max_attempts=attempts)
    assert fake.events == []


@pytest.mark.parametrize("fault", ["constructor", "store", "submit", "consume"])
def test_uncertain_failure_consumed_never_resubmitted(plan, fake, fault):
    fake.orm.fail_constructor = fault == "constructor"
    fake.runner.fail_store = fault == "store"
    fake.runner.uncertain_submit = fault == "submit"
    fake.native.uncertain_consume = fault == "consume"
    slot = b0.B0Slot(7)
    with pytest.raises(OSError):
        call(plan, slot)
    count = fake.runner.submits
    with pytest.raises(b0.B0Refusal):
        call(plan, slot)
    assert fake.runner.submits == count
    # A fresh Python slot cannot reclaim the durable native intent.
    with pytest.raises(b0.B0Refusal):
        call(plan, b0.B0Slot(7))
    assert fake.runner.submits == count


@pytest.mark.parametrize("fault", ["missing_release", "closed_gate", "wrong_release", "wrong_code"])
def test_missing_or_wrong_release_gate_code_zero_submit(plan, fake, monkeypatch, fault):
    if fault == "missing_release":
        monkeypatch.delenv("QCL_NEGF_RELEASE_ID")
    if fault == "closed_gate":
        fake.admission["open"] = False
    if fault == "wrong_release":
        fake.admission["release_id"] = "different"
    if fault == "wrong_code":
        fake.orm.code.filepath_executable = "/nix/store/other/bin/qcl-negf"
    with pytest.raises(ValueError):
        call(plan)
    assert fake.runner.submits == 0
    assert "SinglefileData" not in fake.events


@pytest.mark.parametrize("paths", [["result/*"], ["result/archive"], ["result/b0-control.json", "result/b0-report.json", "raw.h5"]])
def test_unexpected_retrieval_zero_transfer(paths):
    transfers = []
    with pytest.raises(b0.B0Refusal):
        admitted = b0.validate_retrieval(paths)
        transfers.extend(admitted)
    assert transfers == []


def test_boolean_authentication_is_not_coverage(plan, fake):
    fake.native.admit = lambda fd, profile: True
    with pytest.raises(b0.B0Refusal):
        call(plan)
    assert fake.events == []


def test_delivery_requires_controlled_reader_and_rejects_unexpected_fields():
    delivery = dict(execution_id="execution-1", calcjob_uuid=NODE_UUID, attempt=1,
        plan_fingerprint="a" * 64, local_status="local_verified",
        raw_delivery="raw_delivery_pending", execution_status="completed",
        scientific_verdict="not_measured", quiescence_ref="fixture-quiescence",
        read_channel=dict(reference="fixture-reader", max_request_bytes=1024, retry=0))
    expected = dict(execution_id="execution-1", plan_fingerprint="a" * 64, node_uuid=NODE_UUID)
    assert b0.validate_delivery(delivery, **expected) == delivery
    delivery["read_channel"]["retry"] = 1
    with pytest.raises(b0.B0Refusal):
        b0.validate_delivery(delivery, **expected)
    delivery["read_channel"]["retry"] = 0
    delivery["raw_path"] = "arbitrary-raw-glob/*"
    with pytest.raises(b0.B0Refusal):
        b0.validate_delivery(delivery, **expected)


@pytest.mark.parametrize("missing", ["controller", "persister"])
def test_public_submit_missing_broker_prerequisite_spends_claim_no_retry(plan, fake, missing):
    setattr(fake.runner, missing, None)
    slot = b0.B0Slot(7)
    with pytest.raises(RuntimeError):
        call(plan, slot)
    assert fake.events.count("claim:workchain") == 1
    assert fake.events.count("public_submit_instance") == 1
    assert fake.runner.submits == 0
    with pytest.raises(b0.B0Refusal):
        call(plan, slot)
    assert fake.events.count("public_submit_instance") == 1
    assert fake.manager.get_runner() is fake.runner


def test_calcjob_enqueue_uses_actual_daemon_runner_and_never_public_submit(fake):
    runner = fake.runner
    runner.is_daemon_runner = True
    process = runner.instantiate_process(object)
    process.node.store()
    controller = b0.B0Slot(7).consume("a" * 64, {})
    controller.refine("calcjob", process.node)
    with controller.operation("enqueue_calcjob"):
        assert controller.enqueue("calcjob", process, runner) is process.node
    assert fake.events.index("claim:calcjob") < fake.events.index("daemon_runner_submit")
    assert "public_submit_instance" not in fake.events
    assert runner.submits == 1


def test_enqueue_rejects_changed_runner_before_claim(fake):
    runner = fake.runner
    process = runner.instantiate_process(object)
    process.node.store()
    controller = b0.B0Slot(7).consume("a" * 64, {})
    controller.refine("workchain", process.node)
    with pytest.raises(b0.B0Refusal):
        controller.enqueue("workchain", process, FakeRunner(fake.events))
    assert "claim:workchain" not in fake.events
    assert runner.submits == 0
