"""Read committed results; an operating-system success is not scientific success."""

from __future__ import annotations

import io
import hashlib

from aiida import orm
from aiida.parsers.parser import Parser
from jsonschema import ValidationError
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.messages import decode

from .validation import MAX_RESULT_BYTES
from .data import read_plan
from .recovery import (pin_result_commits, prior_archive_data, read_metadata, recovery_data,
                       select_recovery, verify_pause_receipt, verify_prior_archive)


class QCLExecutionParser(Parser):
    """Store validated result metadata alongside the immutable retrieved artifacts."""

    def parse(self, **kwargs):
        # Cache sizes once after retrieval. Repeated portal polling must not seek
        # through every large compressed HDF5 object in the repository.
        files = []
        complete = True
        repository = self.retrieved.base.repository
        for root, _, names in repository.walk():
            for name in names:
                if len(files) >= 10000:
                    complete = False
                    break
                path = str(root / name)
                with repository.open(path, "rb") as handle:
                    handle.seek(0, 2)
                    size = handle.tell()
                files.append({"path": path, "size": size})
            if not complete:
                break
        self.out("inventory", orm.Dict(dict={"complete": complete, "files": files}))
        plan = read_plan(self.node.inputs.plan)
        execution_id = self.node.inputs.execution_id.value
        attempt = self.node.inputs.attempt.value
        if not complete:
            return self.exit_codes.ERROR_INVALID_RESULT
        paths = [item["path"] for item in files]
        try:
            if "result/pause-receipt.json" in paths:
                pause, _ = read_metadata(repository, "result/pause-receipt.json")
                selected = verify_pause_receipt(repository, pause, execution_id=execution_id,
                    attempt=attempt, plan_fingerprint=plan["fingerprint"])
                rejected = []
            else:
                selected, rejected = select_recovery(repository, paths, execution_id=execution_id,
                    attempt=attempt, plan_fingerprint=plan["fingerprint"],
                    point_order={point["id"]: point["order"] for point in plan["points"]},
                    plan_points={point["id"]: point for point in plan["points"]},
                    archive_byte_budget=self.node.inputs.archive_byte_budget.value)
            if selected is not None:
                archive = selected.get("prior_archive")
                if archive is None:
                    archive = verify_prior_archive(repository, selected["progress"], prefix="result",
                        byte_budget=self.node.inputs.archive_byte_budget.value,
                        plan_points={point["id"]: point for point in plan["points"]})
                self.out("recovery_bundle", recovery_data(repository, selected))
                self.out("recovery_receipt", orm.Dict(dict={**selected, "rejected_generations": rejected}))
                if archive["files"]:
                    self.out("prior_archive", prior_archive_data(repository, archive))
        except (ValueError, OSError, KeyError, TypeError) as exception:
            self.logger.error("Invalid recovery receipt: %s", exception)
            return self.exit_codes.ERROR_INVALID_RECOVERY
        try:
            with self.retrieved.base.repository.open("result/series_result.json", "rb") as handle:
                raw = handle.read(MAX_RESULT_BYTES + 1)
        except (OSError, KeyError, FileNotFoundError):
            return self.exit_codes.ERROR_MISSING_RESULT
        try:
            if len(raw) > MAX_RESULT_BYTES:
                raise ValueError("Result exceeds the metadata size limit")
            result = decode(raw, maximum=MAX_RESULT_BYTES)
            schema_validator("scientific-worker-result.schema.json").validate(result)
            expected = {point["id"] for point in plan["points"] if point["execution_id"] == execution_id}
            actual = [point["id"] for point in result["points"]]
            if result["plan_fingerprint"] != plan["fingerprint"]:
                raise ValueError("Plan fingerprint mismatch")
            if result["plan_scientific_fingerprint"] != plan["scientific_fingerprint"]:
                raise ValueError("Scientific fingerprint mismatch")
            if result.get("selected_execution_id") != execution_id:
                raise ValueError("Execution identity mismatch")
            if result["root_definition_id"] != plan["root_definition_id"] or result["name"] != plan["name"]:
                raise ValueError("Scientific definition identity mismatch")
            execution = next(item for item in plan["executions"] if item["id"] == execution_id)
            fields = ("id", "definition_id", "variant_id", "method_id", "purpose", "label", "operation", "point_ids", "repetition")
            if result["executions"] != [{key: execution[key] for key in fields}]:
                raise ValueError("Execution metadata mismatch")
            if result["expected_point_count"] != len(expected) or result["available_point_count"] != len(actual):
                raise ValueError("Point count mismatch")
            if len(set(actual)) != len(actual) or not set(actual) <= expected:
                raise ValueError("Unexpected or duplicate point results")
            if any(point["execution_id"] != execution_id for point in result["points"]):
                raise ValueError("Cross-execution result")
            plan_points = {point["id"]: point for point in plan["points"]}
            for point in result["points"]:
                if type(point["attempt"]) is not int or not 1 <= point["attempt"] <= attempt:
                    raise ValueError("Point attempt disagrees with the owning CalcJob")
                coordinates = point["coordinates"]
                original = plan_points[point["id"]]
                for key in ("temperature_K", "voltage_per_period_V", "branch", "order"):
                    if isinstance(coordinates[key], bool) or coordinates[key] != original[key]:
                        raise ValueError("Point coordinates disagree with the frozen plan")
            commits = pin_result_commits(repository, result["points"],
                plan_fingerprint=plan["fingerprint"], require_receipt=execution["operation"] == "stationary")
        except (ValueError, TypeError, KeyError, OSError, ValidationError) as exception:
            self.logger.error("Invalid scientific result: %s", exception)
            return self.exit_codes.ERROR_INVALID_RESULT
        self.out("result", orm.SinglefileData(file=io.BytesIO(raw), filename="series_result.json"))
        self.out("selection", orm.Dict(dict={"calcjob_uuid": self.node.uuid,
            "execution_id": execution_id, "attempt": attempt,
            "result_sha256": hashlib.sha256(raw).hexdigest(),
            "commits": commits,
            "recovery_commit_sha256": selected["commit_sha256"] if selected else None}))
        if result["status"] == "paused":
            if "result/pause-receipt.json" not in paths or selected is None:
                return self.exit_codes.ERROR_INVALID_RECOVERY
            return self.exit_codes.PAUSED_WITH_RECOVERY
        if result["status"] not in {"completed", "completed_with_warnings"} or set(actual) != expected:
            return self.exit_codes.ERROR_SCIENTIFIC_FAILURE
        if any(point["status"] not in {"completed", "completed_with_warnings"} for point in result["points"]):
            return self.exit_codes.ERROR_SCIENTIFIC_FAILURE
        if any(not point["converged"] for point in result["points"]):
            return self.exit_codes.ERROR_UNCONVERGED_RESULT
