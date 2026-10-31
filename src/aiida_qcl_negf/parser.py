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
        from .model_help import inventory, build_index, StoredLeafReader
        from plumpy.process_states import KillInterruption, PauseInterruption
        # Optional help is retained before *every* native/ordinary science exit.
        # Enumeration uses repository size metadata, never HDF5 seek/read.
        retrieved=self.retrieved
        files=inventory(retrieved.base.repository,owner=retrieved)
        self.out('inventory',orm.Dict(dict=files))
        index=dict(format='qcl-negf.model-help-index.v1',complete=False,entries=[])
        try:
            index=build_index(StoredLeafReader(retrieved.base.repository,owner=retrieved),files,
                plan=read_plan(self.node.inputs.plan),execution_id=self.node.inputs.execution_id.value)
        except (KillInterruption,PauseInterruption):raise
        except (OSError,ValueError,KeyError,TypeError,ValidationError) as error:
            self.logger.warning('Optional model help index unavailable; raw retained: %s',error)
        self.out('model_help_index',orm.Dict(dict=index))
        if "b0_reference" in self.node.inputs:
            return self._parse_b0()
        if "source_graph" in self.node.inputs or "native_execution" in self.node.inputs:
            return self._parse_native(**kwargs)
        return self._parse_ordinary(inventory=files,**kwargs)

    def _parse_ordinary(self, *, inventory, **kwargs):
        # HELP provider/size availability cannot decide a scientific exit.
        from .model_help import scientific_paths,StoredLeafReader
        retrieved=self.retrieved
        repository=retrieved.base.repository
        namespace=scientific_paths(repository,owner=retrieved)
        complete=namespace['complete']
        repository=StoredLeafReader(repository,owner=retrieved)
        plan = read_plan(self.node.inputs.plan)
        execution_id = self.node.inputs.execution_id.value
        attempt = self.node.inputs.attempt.value
        if not complete:
            return self.exit_codes.ERROR_INVALID_RESULT
        paths=namespace["paths"]
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
            with repository.open("result/series_result.json", "rb") as handle:
                raw = handle.read(MAX_RESULT_BYTES + 1)
        except (OSError, KeyError, FileNotFoundError):
            return self.exit_codes.ERROR_MISSING_RESULT
        except (ValueError,TypeError):
            return self.exit_codes.ERROR_INVALID_RESULT
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
                if type(point["attempt"]) is not int or point["attempt"] < 1:
                    raise ValueError("Point attempt disagrees with the owning CalcJob")
                if point["attempt"] > attempt and not (point.get("data") or {}).get("result_commit"):
                    raise ValueError("Uncommitted point attempt exceeds the owning CalcJob")
                coordinates = point["coordinates"]
                original = plan_points[point["id"]]
                for key in ("temperature_K", "voltage_per_period_V", "branch", "order"):
                    if isinstance(coordinates[key], bool) or coordinates[key] != original[key]:
                        raise ValueError("Point coordinates disagree with the frozen plan")
            commits = pin_result_commits(repository, result["points"],
                plan_fingerprint=plan["fingerprint"], require_receipt=execution["operation"] == "stationary")
            from .science_association import validate_native_collisions
            validate_native_collisions(result["points"], commits, input_attempt=attempt,
                execution_id=execution_id, plan_fingerprint=plan["fingerprint"], calcjob_uuid=self.node.uuid,
                scientific_fingerprint=plan["scientific_fingerprint"],result_scientific_fingerprint=result["plan_scientific_fingerprint"])
        except (ValueError, TypeError, KeyError, OSError, ValidationError) as exception:
            self.logger.error("Invalid scientific result: %s", exception)
            return self.exit_codes.ERROR_INVALID_RESULT
        self.out("result", orm.SinglefileData(file=io.BytesIO(raw), filename="series_result.json"))
        selection = {"calcjob_uuid": self.node.uuid,
            "execution_id": execution_id, "attempt": attempt,
            "result_sha256": hashlib.sha256(raw).hexdigest(),
            "commits": commits,
            "recovery_commit_sha256": selected["commit_sha256"] if selected else None}
        if "performance-binding.json" in paths:
            # The v1 sidecar adapter used retired Results keyword arguments.
            # Keep the original packet in retrieved and refuse it explicitly;
            # optional observability never changes science outputs or exit.
            selection["performance_status"] = "invalid"
            selection["performance_reason"] = "legacy_performance_binding_v1_retired"
            self.logger.warning("legacy_performance_binding_v1_retired; raw retained")
        else:
            selection["performance_status"] = "unavailable"
        self.out("selection", orm.Dict(dict=selection))
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

    def _parse_native(self, **kwargs):
        """Preserve original native bytes; publish only a complete detached pair."""
        from .saved_sources import (_native_operation, _native_result, _prepare_native_outputs,
                                    _NATIVE_METADATA, _NATIVE_IO)
        try:
            with _native_operation(_NATIVE_METADATA,_NATIVE_IO) as ledger:
                raw=_native_result(self.node,ledger)
                ledger.reserve('parser status decoding',metadata=512*(1+sum(raw.count(c) for c in (b'{',b'}',b'[',b']',b',',b':')))+4*len(raw))
                result=decode(raw,maximum=MAX_RESULT_BYTES)
                result_node,binding_node=_prepare_native_outputs(self.node,raw,
                    maximum_metadata_bytes=_NATIVE_METADATA,maximum_io_bytes=_NATIVE_IO)
        except FileNotFoundError:
            return self.exit_codes.ERROR_MISSING_RESULT
        except (ValueError,TypeError,KeyError,OSError,ValidationError) as exception:
            self.logger.error("Native source unavailable: %s; %s",exception,getattr(exception,'detail',None))
            return self.exit_codes.ERROR_INVALID_RESULT
        self.out('result',result_node)
        self.out('native_binding',binding_node)
        # Outputs are retained also for negative scientific outcomes. A complete
        # provenance packet is distinct from convergence/scientific acceptance.
        if result['status']=='paused':
            return self.exit_codes.ERROR_INVALID_RECOVERY
        expected=set(result['executions'][0]['point_ids'])
        if (result['status'] not in {'completed','completed_with_warnings'}
                or {p['id'] for p in result['points']}!=expected
                or any(p['status'] not in {'completed','completed_with_warnings'} for p in result['points'])):
            return self.exit_codes.ERROR_SCIENTIFIC_FAILURE
        if any(not p['converged'] for p in result['points']):
            return self.exit_codes.ERROR_UNCONVERGED_RESULT

    def _parse_b0(self):
        from .b0 import (B0_CONTROL_FILES, B0_REPORT_LIMIT,
                         resume_controller, validate_delivery)
        controller = resume_controller(self.node.inputs.b0_reference.value, self.node.uuid)
        with controller.operation("parse_bound_report"):
            # Native port enumerates at most the admitted exact files and rejects
            # unexpected entries BEFORE opening/transferring any payload. It also
            # verifies worker quiescence, identities, leases, hash and receiver ACK.
            delivery = controller.backend.read_delivery(controller.native,
                self.retrieved.base.repository, B0_CONTROL_FILES, B0_REPORT_LIMIT)
            plan = read_plan(self.node.inputs.plan)
            validate_delivery(delivery, execution_id=self.node.inputs.execution_id.value,
                              plan_fingerprint=plan["fingerprint"], node_uuid=self.node.uuid)
            self.out("delivery", orm.Dict(dict=delivery))
            # A bounded local report does not imply full raw repository retrieval
            # or scientific acceptance. The independent reader owns scientific
            # criteria; this parser never turns a report/exit code into pass.
            if delivery["raw_delivery"] == "raw_delivery_pending":
                return self.exit_codes.B0_LOCAL_RAW_DELIVERY_PENDING
            return self.exit_codes.ERROR_SCIENTIFIC_FAILURE
