"""Bounded public ORM reads for the ordinary native attempt bridge.

Returned data is not a certificate. Only the source-owned privileged caller can
join these observations with its current release, namespace and native policy.
Library calls require an already loaded profile; importing never loads one.
"""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import re
import sys
from uuid import UUID

from qcl_negf_contracts.messages import decode, encode, scientific_plan

from . import science_association as association

MAX_FILE = 65536
MAX_METADATA = 8 * 1024 * 1024
SCHEDULER = "qcl_negf.native_slurm"
TRANSPORT = "qcl_negf.native_local"
CONTROL_FILES = ("series_result.json", "native-selection.json", "native-close.json", "_scheduler-stdout.txt", "_scheduler-stderr.txt")


class NativeActorUnavailable(ValueError):
    """The original exception is chained; no constructor grants authority."""


def _need(condition, reason):
    if not condition:
        raise NativeActorUnavailable(reason)


def _orm():
    # AiiDA's settings import creates instance directories. Do not import it
    # merely to reject a missing profile or to import this CLI module.
    aiida = sys.modules.get("aiida")
    _need(aiida is not None and aiida.get_profile() is not None,"loaded_profile_required")
    from aiida import orm
    return orm


def _uuid(value):
    _need(type(value) is str and str(UUID(value)) == value and UUID(value).int != 0, "invalid_uuid")
    return value


def _path(value):
    _need(type(value) is str and value.startswith("/") and len(value.encode()) <= 4096 and
          all(p not in ("", ".", "..") for p in value.split("/")[1:]) and
          all(ord(c) >= 32 for c in value), "invalid_workdir")
    return value


class _Reads:
    """One nonrefundable read workspace, never an admission owner."""
    def __init__(self, maximum, *, before=None):
        _need(type(maximum) is int and 0 < maximum <= MAX_METADATA, "metadata_budget")
        self.remaining, self.calls, self.cache = maximum, 0, {}
        self.before = before

    def charge(self, size):
        if self.before is not None:
            self.before()
        self.remaining -= size
        _need(self.remaining >= 0, "metadata_budget")

    def query(self, query, *, maximum=2):
        self.calls += 1
        _need(self.calls <= 4096, "query_budget")
        self.charge(8192 * maximum)
        rows = query.limit(maximum).all(flat=True)
        self.charge(0)
        return rows

    def file(self, node):
        orm = _orm()
        _need(isinstance(node, orm.SinglefileData), "singlefile_input_required")
        if node.uuid not in self.cache:
            # Charge raw/JSON workspace before opening/allocating the buffer.
            raw = bytearray()
            with node.open(mode="rb") as stream:
                while len(raw) <= MAX_FILE:
                    size = min(4096,MAX_FILE+1-len(raw))
                    self.charge(8 * size)
                    chunk = stream.read(size)
                    if not chunk:
                        break
                    raw.extend(chunk)
            _need(len(raw) <= MAX_FILE, "input_file_bound")
            self.charge(0)
            self.cache[node.uuid] = bytes(raw)
        return self.cache[node.uuid]


def _related(node, kind, label, budget, *, outgoing=False):
    orm = _orm()
    query = orm.QueryBuilder().append(orm.Node, filters={"uuid": node.uuid}, tag="anchor")
    edge = {"type": kind.value}
    if label is not None:
        edge["label"] = label
    query.append(orm.Node, **{"with_incoming" if outgoing else "with_outgoing": "anchor"},
                 edge_filters=edge, project="*")
    rows = budget.query(query)
    _need(len(rows) == 1, "missing_or_ambiguous_typed_link")
    return rows[0]


def _input(node, label, budget):
    orm = _orm()
    from aiida.common.links import LinkType
    kind = LinkType.INPUT_CALC if isinstance(node, orm.CalcJobNode) else LinkType.INPUT_WORK
    return _related(node, kind, label, budget)


def _file_claim(node, raw, declared):
    association._binding_node_file(declared)
    _need(declared == dict(node_uuid=node.uuid, bytes=str(len(raw)),
                          sha256=hashlib.sha256(raw).hexdigest()), "original_file_identity")


def _validate_native_inputs(plan_node, graph_node, native_node, code, execution_id, budget):
    orm = _orm()
    _need(all(isinstance(n, orm.SinglefileData) and n.is_stored
              for n in (plan_node, graph_node, native_node)) and code.is_stored,
          "stored_original_inputs_required")
    raw_plan, raw_graph, raw_native = (budget.file(n) for n in (plan_node, graph_node, native_node))
    plan = scientific_plan(decode(raw_plan, maximum=MAX_FILE))
    association._binding_schema(plan, "scientific-plan.schema.json")
    graph = decode(raw_graph, maximum=MAX_FILE)
    association._binding_schema(graph, "model-help.schema.json", "#/$defs/SourceGraph")
    paths = [row["path"] for row in graph["components"]]
    _need(len(paths) == len(set(paths)), "duplicate_source_component")
    native = decode(raw_native, maximum=MAX_FILE)
    association._binding_keys(native, "schema request_id execution_id storage source_graph code input_plan access_receipt_ref")
    _need(native["schema"] == "qcl-negf.aiida-native-execution.v1", "native_execution_schema")
    association._binding_uuid(native["request_id"])
    association._binding_opaque(native["access_receipt_ref"])
    association._binding_storage(native["storage"])
    association._binding_code(native["code"])
    _file_claim(plan_node, raw_plan, native["input_plan"])
    _file_claim(graph_node, raw_graph, native["source_graph"])
    _need(native["storage"]["execution_binding_uuid"] == native_node.uuid, "native_input_uuid")
    _need(isinstance(code, orm.InstalledCode) and code.default_calc_job_plugin == "qcl_negf.execution", "installed_code_required")
    _need(code.computer is not None and code.computer.scheduler_type == SCHEDULER and
          code.computer.transport_type == TRANSPORT, "native_computer_required")
    _need(native["code"]["code_uuid"] == code.uuid and
          native["code"]["computer_uuid"] == native["storage"]["computer_uuid"] == code.computer.uuid and
          native["code"]["solver_executable"] == str(code.filepath_executable), "code_input_binding")
    _need(native["execution_id"] == execution_id and execution_id in {e["id"] for e in plan["executions"]}, "execution_input_binding")
    return native, plan, (raw_plan, raw_graph, raw_native)


def _reviewed_options(code, options):
    _need(not code.prepend_text and not code.append_text and
          not code.computer.get_prepend_text() and not code.computer.get_append_text(),
          "native_shell_customization")
    for key in ("prepend_text","append_text","custom_scheduler_commands","additional_retrieve_list",
                "stash","unstash","rerunnable","provenance_exclude_list"):
        _need(not options.get(key),"native_option_customization")
    _need(not options.get("withmpi"),"native_mpi_unavailable")
    for key,name in (("submit_script_filename","_aiidasubmit.sh"),
                     ("scheduler_stdout","_scheduler-stdout.txt"),("scheduler_stderr","_scheduler-stderr.txt")):
        _need(options.get(key) in (None,name),"native_control_filename")


def _original_plan(candidate, executions, budget):
    """An explicit full NodeFile selects stored original bytes, never latest."""
    orm = _orm()
    _need(type(executions) is dict and executions,"native_execution_coverage")
    raw = budget.file(candidate)
    plan = scientific_plan(decode(raw,maximum=MAX_FILE))
    first = plan["executions"][0]["id"]
    _need(first in executions,"native_execution_coverage")
    declaration = decode(budget.file(executions[first]),maximum=MAX_FILE)
    reference = declaration["input_plan"]
    association._binding_node_file(reference)
    rows = budget.query(orm.QueryBuilder().append(orm.SinglefileData,
        filters={"uuid":reference["node_uuid"]},project="*"))
    _need(len(rows)==1,"original_plan_node_unavailable")
    original = rows[0]
    original_raw = budget.file(original)
    _file_claim(original,original_raw,reference)
    _need(original_raw==raw,"original_plan_bytes_differ")
    return original


def _ingress(plan_node, graph, executions, code, max_attempts, *, budget=None):
    _need(graph is not None and type(executions) is dict and max_attempts == 1 and type(max_attempts) is int,
          "native_requires_inputs_and_one_attempt")
    _need(len(executions) <= 256, "native_execution_coverage")
    budget = _Reads(MAX_METADATA) if budget is None else budget
    plan = scientific_plan(decode(budget.file(plan_node), maximum=MAX_FILE))
    identifiers = [e["id"] for e in plan["executions"]]
    _need(set(executions) == set(identifiers) and len(identifiers) <= 256, "native_execution_coverage")
    mapped = {}
    for index, identifier in enumerate(identifiers):
        _validate_native_inputs(plan_node, graph, executions[identifier], code, identifier, budget)
        mapped[f"execution_{index}"] = executions[identifier]
    return mapped


def _load(calcjob_uuid, maximum_metadata_bytes, *, budget=None):
    orm = _orm()
    from aiida.common.links import LinkType
    budget = _Reads(maximum_metadata_bytes) if budget is None else budget
    _uuid(calcjob_uuid)
    rows = budget.query(orm.QueryBuilder().append(orm.CalcJobNode,
        filters={"uuid": calcjob_uuid, "process_type": association._CALC_PROCESS}, project="*"))
    _need(len(rows) == 1, "calcjob_unavailable")
    calc = rows[0]
    _need(calc.is_stored, "stored_calcjob_required")
    parent = _related(calc, LinkType.CALL_CALC, None, budget)
    if parent.process_type == association._RESTART_PROCESS:
        run = _related(parent, LinkType.CALL_WORK, None, budget)
        processes = (run, parent, calc)
    else:
        run, processes = parent, (parent, calc)
    _need(isinstance(run, orm.WorkChainNode) and run.process_type == association._PLAN_PROCESS, "plan_call_owner")
    plan_node, graph_node, code, native_node = (_input(calc, label, budget) for label in
        ("plan", "source_graph", "code", "native_execution"))
    attempt, execution = _input(calc, "attempt", budget), _input(calc, "execution_id", budget)
    _need(type(attempt) is orm.Int and type(attempt.value) is int and attempt.value > 0 and
          type(execution) is orm.Str, "transport_scalar_inputs")
    for process in processes:
        _need(process.is_stored, "stored_process_required")
        for label, original in (("plan",plan_node),("source_graph",graph_node),("code",code)):
            _need(_input(process, label, budget).uuid == original.uuid, "call_input_identity")
        if process.uuid != run.uuid:
            for label, original in (("native_execution",native_node),("attempt",attempt),("execution_id",execution)):
                _need(_input(process, label, budget).uuid == original.uuid, "call_scalar_identity")
    native, plan, raws = _validate_native_inputs(plan_node, graph_node, native_node, code, execution.value, budget)
    _reviewed_options(code,calc.get_options())
    release = _input(calc,"release_id",budget)
    _need(type(release) is orm.Str and release.value==native["code"]["release_id"],"release_input_binding")
    for process in processes:
        _need(_input(process,"release_id",budget).uuid==release.uuid,"call_release_identity")
    _need(calc.computer.uuid == code.computer.uuid, "calcjob_computer_binding")
    index = [e["id"] for e in plan["executions"]].index(execution.value)
    _need(_input(run, f"native_executions__execution_{index}", budget).uuid == native_node.uuid,
          "plan_native_input_selection")
    script = _related(calc, LinkType.CREATE, "native_submit_script", budget, outgoing=True)
    raw_script = budget.file(script)
    _need(calc.get_option("submit_script_filename") == "_aiidasubmit.sh", "submit_filename")
    workdir, job_id = _path(calc.get_remote_workdir()), calc.get_job_id()
    launch = dict(request_id=native["request_id"], run_uuid=run.uuid, execution_id=execution.value,
        transport_attempt=attempt.value, code_uuid=code.uuid, release_id=native["code"]["release_id"],
        native_execution_uuid=native_node.uuid, input_plan_uuid=plan_node.uuid, source_graph_uuid=graph_node.uuid,
        relative_execution_root=native["storage"]["relative_execution_root"], remote_workdir=workdir,
        files={name:dict(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()) for name,raw in
               zip(("plan","source_graph","native_execution","submit_script"),(*raws,raw_script))})
    fresh = orm.load_node(calc.uuid)
    _need(fresh.get_remote_workdir() == workdir and fresh.get_job_id() == job_id, "runtime_selection_changed")
    budget.charge(0)
    return launch, raws[2]


def load_native_launch(calcjob_uuid: str, *, maximum_metadata_bytes: int) -> dict:
    """Read actual typed graph; result alone grants no native admission."""
    try:
        return _load(calcjob_uuid, maximum_metadata_bytes)[0]
    except NativeActorUnavailable:
        raise
    except Exception as error:
        raise NativeActorUnavailable("actor_input_unavailable") from error


def read_native_execution(calcjob_uuid: str, *, maximum_metadata_bytes: int) -> bytes:
    """Return original INPUT file bytes, without newline/reencoding."""
    try:
        return _load(calcjob_uuid, maximum_metadata_bytes)[1]
    except NativeActorUnavailable:
        raise
    except Exception as error:
        raise NativeActorUnavailable("actor_input_unavailable") from error


def native_calcjob_uuid(*, computer_uuid: str, remote_workdir: str | None = None,
                       job_id: str | None = None, maximum_metadata_bytes: int) -> str:
    """Exact bounded lookup; identifiers and paths are selectors, not proof."""
    try:
        budget = _Reads(maximum_metadata_bytes)
        return _lookup(computer_uuid,remote_workdir,job_id,budget)
    except NativeActorUnavailable:
        raise
    except Exception as error:
        raise NativeActorUnavailable("actor_lookup_unavailable") from error


def _retrieved_packet(calcjob_uuid, expected, budget):
    """Actual CREATE/repository read; expected bytes never grant source access."""
    orm = _orm()
    from aiida.common.links import LinkType
    _uuid(calcjob_uuid)
    rows = budget.query(orm.QueryBuilder().append(orm.CalcJobNode,
        filters={"uuid": calcjob_uuid, "process_type": association._CALC_PROCESS}, project="*"))
    _need(len(rows) == 1 and rows[0].is_stored, "calcjob_unavailable")
    calc = rows[0]
    node = _related(calc, LinkType.CREATE, calc.link_label_retrieved, budget, outgoing=True)
    _need(isinstance(node, orm.FolderData) and node.is_stored, "original_retrieved_required")
    repository = node.base.repository
    budget.charge(8192)
    names = []
    for entry in repository.list_objects():
        budget.charge(1024)
        _need(len(names) < 5 and entry.is_file(), "retrieved_packet_membership")
        names.append(entry.name)
    _need(set(names) == set(CONTROL_FILES) == set(expected), "retrieved_packet_membership")
    result = {}
    for name in CONTROL_FILES:
        size = len(expected[name]); raw = bytearray()
        with repository.open(name, mode="rb") as stream:
            while len(raw) <= size:
                count = min(4096, size + 1 - len(raw))
                budget.charge(8 * count)
                chunk = stream.read(count)
                if not chunk:
                    break
                raw.extend(chunk)
        budget.charge(len(raw))
        _need(bytes(raw) == expected[name], "retrieved_packet_changed")
        result[name] = bytes(raw)
    return result


def _lookup(computer_uuid, remote_workdir, job_id, budget):
    orm = _orm()
    _uuid(computer_uuid)
    _need((remote_workdir is None) != (job_id is None), "one_selector_required")
    if remote_workdir is not None:
        filters = {"attributes.remote_workdir": _path(remote_workdir)}
    else:
        _need(type(job_id) is str and re.fullmatch(r"[1-9][0-9]{0,9}",job_id), "job_selector")
        filters = {"attributes.job_id": job_id}
    filters["process_type"] = association._CALC_PROCESS
    query = orm.QueryBuilder().append(orm.Computer,filters={"uuid":computer_uuid},tag="computer")
    query.append(orm.CalcJobNode,with_computer="computer",filters=filters,project="uuid")
    rows = budget.query(query)
    _need(len(rows) == 1, "calcjob_selector_missing_or_ambiguous")
    return _uuid(rows[0])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True)
    parser.add_argument("verb", choices=("launch","execution"))
    parser.add_argument("calcjob_uuid")
    parser.add_argument("--maximum-metadata-bytes", type=int, required=True)
    args = parser.parse_args()
    try:
        _Reads(args.maximum_metadata_bytes)
        _uuid(args.calcjob_uuid)
        config_root = os.environ.get("AIIDA_PATH")
        _need(config_root is not None and Path(config_root).is_absolute() and
              (Path(config_root)/".aiida/config.json").is_file(),"existing_profile_config_required")
        from aiida import load_profile
        load_profile(args.profile)  # existing profile only; no setup/bootstrap
        if args.verb == "launch":
            raw = encode(load_native_launch(args.calcjob_uuid,maximum_metadata_bytes=args.maximum_metadata_bytes))
        else:
            raw = read_native_execution(args.calcjob_uuid,maximum_metadata_bytes=args.maximum_metadata_bytes)
        sys.stdout.buffer.write(raw)
        return 0
    except Exception:
        print("native actor unavailable",file=sys.stderr)
        return 75
