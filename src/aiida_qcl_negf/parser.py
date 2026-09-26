"""Read committed results; an operating-system success is not scientific success."""

from __future__ import annotations

import io

from aiida import orm
from aiida.parsers.parser import Parser
from jsonschema import ValidationError
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.messages import decode

from .validation import MAX_RESULT_BYTES
from .data import read_plan


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
            plan = read_plan(self.node.inputs.plan)
            execution_id = self.node.inputs.execution_id.value
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
                coordinates = point["coordinates"]
                original = plan_points[point["id"]]
                for key in ("temperature_K", "voltage_per_period_V", "branch", "order"):
                    if isinstance(coordinates[key], bool) or coordinates[key] != original[key]:
                        raise ValueError("Point coordinates disagree with the frozen plan")
        except (ValueError, TypeError, KeyError, ValidationError) as exception:
            self.logger.error("Invalid scientific result: %s", exception)
            return self.exit_codes.ERROR_INVALID_RESULT
        self.out("result", orm.SinglefileData(file=io.BytesIO(raw), filename="series_result.json"))
        if result["status"] not in {"completed", "completed_with_warnings"} or set(actual) != expected:
            return self.exit_codes.ERROR_SCIENTIFIC_FAILURE
        if any(point["status"] not in {"completed", "completed_with_warnings"} for point in result["points"]):
            return self.exit_codes.ERROR_SCIENTIFIC_FAILURE
        if any(not point["converged"] for point in result["points"]):
            return self.exit_codes.ERROR_UNCONVERGED_RESULT
