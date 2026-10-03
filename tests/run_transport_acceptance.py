"""Explicit trusted-profile rehearsal of one 1 MiB SYNTHETIC negative job.

Invoke with the installed application's `verdi -p PROFILE run` under an external
deadline. This bypasses the production submission service by design: it creates
a separate SYNTHETIC Code, never changes a production Code or release gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

from aiida import orm
from aiida.engine import run_get_node
from aiida.engine.utils import instantiate_process
from aiida.manage import get_manager
from aiida_qcl_negf.calculation import QCLExecutionCalculation
from aiida_qcl_negf.data import plan_data, read_json
from aiida_qcl_negf.workflow import QCLPlanWorkChain
from qcl_negf_results.commits import atomic_write, json_bytes

# `verdi run` sets __file__ to a basename and preserves the full path in argv[0].
SOURCE_DIRECTORY = Path(sys.argv[0] if __name__ == "__main__" else __file__).resolve().parent
sys.path.insert(0, str(SOURCE_DIRECTORY))
from transport_fixture import MODEL, SCOPE, freeze_plan


def main(arguments=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--computer", required=True)
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args(arguments)
    if (not str(args.executable).startswith("/nix/store/")
            or ".." in args.executable.parts
            or not args.executable.name.startswith("qcl-negf-synthetic-transport")
            or not args.label.startswith("SYNTHETIC-")):
        raise ValueError("Require an immutable explicitly SYNTHETIC fixture Code")
    computer = orm.load_computer(args.computer)
    if computer.scheduler_type != "core.slurm":
        raise ValueError("Installed acceptance requires the Slurm Computer")
    if orm.QueryBuilder().append(orm.Code, filters={"label": args.label}).count():
        raise ValueError("Use a fresh SYNTHETIC label for each explicit attempt")
    args.evidence.mkdir(mode=0o700, parents=True, exist_ok=False)
    frozen = args.evidence / "negative-plan.json"
    freeze_plan(SOURCE_DIRECTORY / "fixtures/plan.json", frozen,
                mode="negative", native_mib=1, wait_seconds=0)
    raw = frozen.read_bytes()
    code = orm.InstalledCode(
        label=args.label, computer=computer, filepath_executable=str(args.executable),
        default_calc_job_plugin="qcl_negf.execution", with_mpi=False,
    ).store()
    code.base.extras.set("synthetic_transport", {"schema": MODEL, "scope": SCOPE})
    identity = {"schema": MODEL, "scope": SCOPE, "fixture_code_uuid": code.uuid,
                "computer_uuid": computer.uuid, "executable": str(args.executable),
                "plan_bytes_sha256": hashlib.sha256(raw).hexdigest(),
                "max_attempts": 1, "native_payload_bytes": 1024**2}
    process = instantiate_process(
        get_manager().get_runner(), QCLPlanWorkChain, code=code, plan=plan_data(raw),
        resources=orm.Dict(dict={"num_machines": 1, "num_mpiprocs_per_machine": 1,
            "num_cores_per_mpiproc": 1, "max_memory_kb": 1024**2,
            "max_wallclock_seconds": 300}), max_concurrent=orm.Int(1),
        max_attempts=orm.Int(1), retry_backoff_seconds=orm.Int(0),
        archive_byte_budget=orm.Int(4 * 1024**2), metadata={"label": args.label},
    )
    identity["root_uuid"] = process.node.uuid
    atomic_write(args.evidence / "admission.json", json_bytes(identity), immutable=True)
    print(json.dumps({"event": "synthetic-admitted", **identity}), flush=True)
    _, root = run_get_node(process)
    calculations = [node for node in root.called_descendants
                    if isinstance(node, orm.CalcJobNode)
                    and node.process_type == QCLExecutionCalculation.build_process_type()]
    evidence = {**identity, "root_uuid": root.uuid, "root_exit_status": root.exit_status,
                "children": [], "scientific_accepted": False}
    for node in calculations:
        child = {"uuid": node.uuid, "exit_status": node.exit_status,
                 "job_id": node.get_job_id(), "scheduler_state": str(node.get_scheduler_state()),
                 "attempt": node.inputs.attempt.value}
        if "retrieved" in node.outputs:
            repository = node.outputs.retrieved.base.repository
            child["retrieved_uuid"] = node.outputs.retrieved.uuid
            child["plan_bytes_preserved"] = repository.get_object_content(
                "result/scientific_plan.json", mode="rb") == raw
            if "result" in node.outputs:
                point = read_json(node.outputs.result)["points"][0]
                path = "result/" + point["data"]["result_commit"]
                commit_raw = repository.get_object_content(path, mode="rb")
                commit = json.loads(commit_raw)
                child.update(converged=point["converged"],
                    scientific_accepted=commit["scientific_accepted"],
                    commit_sha256=hashlib.sha256(commit_raw).hexdigest(),
                    identity=commit["identity"], artifacts=commit["artifacts"],
                    selection=node.outputs.selection.get_dict())
        evidence["children"].append(child)
    atomic_write(args.evidence / "evidence.json", json_bytes(evidence), immutable=True)
    print(json.dumps(evidence), flush=True)
    if (root.exit_status != 400 or len(calculations) != 1
            or evidence["children"][0].get("exit_status") != 303
            or evidence["children"][0].get("attempt") != 1
            or evidence["children"][0].get("plan_bytes_preserved") is not True
            or evidence["children"][0].get("scientific_accepted") is not False
            or evidence["children"][0].get("converged") is not False):
        raise RuntimeError("Synthetic transport acceptance failed; inspect retained evidence")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
