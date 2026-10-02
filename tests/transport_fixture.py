"""Bounded infrastructure fixture; never invoke this as a physical solver.

Its frozen identities describe a transport-only envelope, not Julia's canonical
scientific plan. The retained template configuration is never evaluated.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import h5py
import numpy as np
from aiida_qcl_negf.validation import validate_plan
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.artifacts import COMMIT_SCHEMA, CONTRACT_SET
from qcl_negf_contracts.messages import fingerprint
from qcl_negf_results.commits import (
    artifact_row,
    atomic_write,
    json_bytes,
    publish_pointer,
)
from qcl_negf_results.native import (
    MARKER_CHILD_FIELDS,
    MARKER_FIELDS,
    validate_native_handle,
)
from qcl_negf_results.witnesses import PACKED_SCHEMA

MODEL = "synthetic-transport-fixture-v1"
SCOPE = "transport-only; no scientific validation"
MIB = 1024 * 1024
MAX_NATIVE_MIB = 270
MAX_WAIT_SECONDS = 300


def budget(native_mib: int, wait_seconds: int) -> None:
    if type(native_mib) is not int or not 1 <= native_mib <= MAX_NATIVE_MIB:
        raise ValueError("synthetic native payload must be 1..270 MiB")
    if type(wait_seconds) is not int or not 0 <= wait_seconds <= MAX_WAIT_SECONDS:
        raise ValueError("synthetic wait must be 0..300 seconds")


def identities(plan: dict) -> tuple[str, str]:
    body = {
        key: value
        for key, value in plan.items()
        if key not in {"fingerprint", "scientific_fingerprint"}
    }
    transport = fingerprint(
        {"schema": MODEL, "scope": SCOPE, "frozen_transport_plan": body}
    )
    unavailable = fingerprint(
        {
            "schema": MODEL,
            "scientific_validation": "not_performed",
            "transport": transport,
        }
    )
    return transport, unavailable


def freeze_plan(
    template: Path, destination: Path, *, mode: str, native_mib: int, wait_seconds: int
) -> None:
    budget(native_mib, wait_seconds)
    if mode not in {"negative", "cancel"} or (mode == "negative" and wait_seconds != 0):
        raise ValueError("synthetic mode must be negative (no wait) or cancel")
    plan = json.loads(template.read_bytes())
    if len(plan["executions"]) != 1 or len(plan["points"]) != 1:
        raise ValueError("synthetic fixture requires exactly one execution and point")
    plan["name"] = f"SYNTHETIC infrastructure {mode}; no scientific validation"
    plan["maximum_solver_runs"] = 1
    plan["resources"]["maximum_active_executions"] = 1
    plan["executions"][0]["provenance"]["synthetic_transport"] = {
        "schema": MODEL,
        "scope": SCOPE,
        "mode": mode,
        "native_bytes": native_mib * MIB,
        "wait_seconds": wait_seconds,
        "payload_seed": 1729,
        "template_bytes_sha256": hashlib.sha256(template.read_bytes()).hexdigest(),
        "scientific_fingerprint_kind": "synthetic unavailable marker; not Julia canonical identity",
    }
    plan["fingerprint"], plan["scientific_fingerprint"] = identities(plan)
    validate_plan(plan)
    atomic_write(destination, json_bytes(plan), immutable=True)


def empty_columns(parent, names: dict[str, str], *, byte_columns=()) -> None:
    for name, kind in names.items():
        dtype = (
            h5py.string_dtype("utf-8")
            if kind == "s"
            else np.dtype(
                "int8"
                if name in byte_columns
                else "int64"
                if kind == "i"
                else "float64"
            )
        )
        parent.create_dataset(name, shape=(0,), dtype=dtype)


def native_header(handle, provenance: dict) -> None:
    metadata = handle.create_group("metadata")
    metadata.attrs.update(
        schema="qcl-negf-physics-analysis-v4",
        schema_version="4.0",
        artifact_role="physics.analysis",
        contract_set=CONTRACT_SET,
        producer_kind="synthetic-infrastructure-fixture",
        scientific_validation="not_performed",
        synthetic_provenance_json=json_bytes(provenance).decode(),
    )
    diagnostics = handle.create_group("diagnostics")
    diagnostics.create_group("scba").create_dataset(
        "sequence", shape=(0,), dtype="int64"
    )
    markers = diagnostics.create_group("physical_markers")
    markers.attrs["schema"] = "qcl-negf-physical-markers-v3"
    empty_columns(
        markers,
        {"sequence": "i", "available": "i", **MARKER_FIELDS},
        byte_columns={"available", "equilibrium_applicable"},
    )
    for name, fields in MARKER_CHILD_FIELDS.items():
        child = markers.create_group(name)
        child.attrs["schema"] = f"qcl-negf-physical-marker-{name}-v1"
        empty_columns(child, fields)
    psd = diagnostics.create_group("psd_history")
    psd.attrs["schema"] = "qcl-negf-psd-history-v2"
    empty_columns(
        psd,
        {"sequence": "i", "available": "i", "matrix_kind": "s"},
        byte_columns={"available"},
    )
    packed = psd.create_group("selected_blocks")
    packed.attrs.update(schema=PACKED_SCHEMA, index_origin=0)
    empty_columns(
        packed,
        {
            name: "i"
            for name in (
                "record_sequence",
                "record_group_metadata_index",
                "payload_record_index",
                "payload_metadata_index",
            )
        },
    )
    empty_columns(
        packed,
        {
            name: "s"
            for name in (
                "record_attributes_json",
                "group_metadata_json",
                "dataset_metadata_json",
            )
        },
    )
    packed["payload_offsets"] = np.array([0], dtype="int64")
    packed.create_dataset("payload_values", shape=(0,), dtype="float64")


def execute(
    plan_path: Path,
    output: Path,
    identifier: str,
    *,
    native_mib: int,
    scratch_root: str | None,
) -> None:
    if scratch_root is not None:
        raise ValueError(
            "synthetic fixture does not implement Julia scratch staging; omit scratch_root"
        )
    raw = plan_path.read_bytes()
    plan = json.loads(raw)
    validate_plan(plan)
    if len(plan["executions"]) != 1 or len(plan["points"]) != 1:
        raise ValueError(
            "only a marked synthetic single-execution transport plan is allowed"
        )
    execution = plan["executions"][0]
    specification = execution.get("provenance", {}).get("synthetic_transport", {})
    if specification.get("schema") != MODEL or specification.get("scope") != SCOPE:
        raise ValueError("missing synthetic transport provenance")
    budget(native_mib, specification["wait_seconds"])
    if specification["native_bytes"] != native_mib * MIB:
        raise ValueError(
            "explicit fixture payload differs from frozen plan; use --fixture-native-mib"
        )
    if (plan["fingerprint"], plan["scientific_fingerprint"]) != identities(plan):
        raise ValueError("synthetic frozen identity mismatch")
    if identifier != execution["id"] or plan["points"][0]["execution_id"] != identifier:
        raise ValueError("synthetic execution identity mismatch")
    provenance = {
        **specification,
        "producer_kind": "synthetic-infrastructure-fixture",
        "plan_bytes_sha256": hashlib.sha256(raw).hexdigest(),
        "execution_id": identifier,
    }
    output.mkdir()
    atomic_write(
        output / "synthetic-started.json", json_bytes(provenance), immutable=True
    )
    print(
        json.dumps(
            {"event": "synthetic-started", "execution_id": identifier, "scope": SCOPE}
        ),
        flush=True,
    )
    if specification["mode"] == "cancel":
        # Cancellation must target this scheduler job while it is running. No
        # complete result/commit is published before the bounded wait finishes.
        time.sleep(specification["wait_seconds"])
    point = plan["points"][0]
    relative = f"{point['id']}/artifacts/generation-000001"
    generation = output / relative
    generation.mkdir(parents=True)
    native = generation / "synthetic-analysis.h5"
    generator = np.random.default_rng(specification["payload_seed"])
    with h5py.File(native, "w") as handle:
        native_header(handle, provenance)
        payload = handle.create_group("synthetic_transport").create_dataset(
            "payload", shape=(native_mib * MIB,), dtype="uint8", chunks=(MIB,)
        )
        payload.attrs.update(
            semantics="opaque synthetic bytes; not a physical observable", units="byte"
        )
        for start in range(0, len(payload), MIB):
            payload[start : start + MIB] = generator.integers(
                0, 256, size=MIB, dtype="uint8"
            )
        handle.flush()
        validate_native_handle(
            handle, "physics.analysis", "qcl-negf-physics-analysis-v4"
        )
    frozen_plan = generation / "synthetic-plan.json"
    atomic_write(frozen_plan, raw, immutable=True)
    native_row = artifact_row(
        native,
        relative=native.name,
        role="physics.analysis",
        profile="both",
        schema="qcl-negf-physics-analysis-v4",
        media_type="application/x-hdf5",
    )
    native_row["dependencies"] = [frozen_plan.name]
    commit = {
        "schema": COMMIT_SCHEMA,
        "contract_set": CONTRACT_SET,
        "identity": {
            "point_id": point["id"],
            "execution_id": identifier,
            "attempt": 1,
            "plan_fingerprint": plan["fingerprint"],
            "producer_kind": "synthetic-infrastructure-fixture",
            "scientific_validation": "not_performed",
        },
        "generation": 1,
        "scientific_accepted": False,
        "terminal_status": "unconverged",
        "quality": "unconverged",
        "synthetic_provenance": provenance,
        "artifacts": [
            native_row,
            artifact_row(
                frozen_plan,
                relative=frozen_plan.name,
                role="plan",
                profile="both",
                media_type="application/json",
            ),
        ],
    }
    payload = json_bytes(commit)
    atomic_write(generation / "commit.json", payload, immutable=True)
    publish_pointer(
        generation.parent / "current.json", "generation-000001/commit.json", payload, 1
    )
    result_point = {
        "id": point["id"],
        "execution_id": identifier,
        "attempt": 1,
        "coordinates": {
            key: point[key]
            for key in ("temperature_K", "voltage_per_period_V", "branch", "order")
        },
        "initialization": {
            "kind": "synthetic",
            "source_point_id": None,
            "checkpoint": None,
            "fallback_reason": None,
        },
        "status": "completed",
        "quality": "unconverged",
        "converged": False,
        "warnings": [{"kind": "synthetic-infrastructure-fixture", "message": SCOPE}],
        "observables": {},
        "data": {"result_commit": relative + "/commit.json"},
        "postprocessing": {},
    }
    series = {
        "schema": "qcl-negf-series-result-v3",
        "contract_set": CONTRACT_SET,
        "plan_fingerprint": plan["fingerprint"],
        "plan_scientific_fingerprint": plan["scientific_fingerprint"],
        "root_definition_id": plan["root_definition_id"],
        "name": plan["name"],
        "status": "completed",
        "model_version": MODEL,
        "inclusions": plan["inclusions"],
        "executions": [
            {
                key: execution[key]
                for key in (
                    "id",
                    "definition_id",
                    "variant_id",
                    "method_id",
                    "purpose",
                    "label",
                    "operation",
                    "point_ids",
                    "repetition",
                )
            }
        ],
        "expected_point_count": 1,
        "available_point_count": 1,
        "points": [result_point],
        "selected_execution_id": identifier,
    }
    schema_validator("scientific-worker-result.schema.json").validate(series)
    atomic_write(output / "series_result.json", json_bytes(series), immutable=True)
    print(
        json.dumps(
            {
                "event": "synthetic-complete",
                "scientific_accepted": False,
                "native_bytes": native.stat().st_size,
            }
        ),
        flush=True,
    )


def main(arguments=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze-plan")
    freeze.add_argument("template", type=Path)
    freeze.add_argument("destination", type=Path)
    freeze.add_argument("--mode", choices=("negative", "cancel"), required=True)
    freeze.add_argument("--wait-seconds", type=int, default=0)
    freeze.add_argument("--fixture-native-mib", type=int, default=1)
    run = commands.add_parser("run-plan")
    run.add_argument("plan", type=Path)
    run.add_argument("output", type=Path)
    run.add_argument("--execution-id", required=True)
    run.add_argument("--scratch-root")
    run.add_argument("--fixture-native-mib", type=int, default=1)
    args = parser.parse_args(arguments)
    if args.command == "freeze-plan":
        freeze_plan(
            args.template,
            args.destination,
            mode=args.mode,
            native_mib=args.fixture_native_mib,
            wait_seconds=args.wait_seconds,
        )
    else:
        execute(
            args.plan,
            args.output,
            args.execution_id,
            native_mib=args.fixture_native_mib,
            scratch_root=args.scratch_root,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
