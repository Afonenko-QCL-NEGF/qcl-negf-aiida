"""One threaded scientific execution submitted through an AiiDA scheduler."""

from __future__ import annotations

import base64
import io
import json
import os
import re
import shlex

from aiida import orm
from aiida.common.datastructures import CalcInfo, CodeInfo
from aiida.engine import CalcJob

from .validation import MAX_PLAN_BYTES, validate_plan, validate_scratch_root
from .data import read_plan
from .recovery import MAX_ARCHIVE_BYTES


class QCLExecutionCalculation(CalcJob):
    """Execute a selected execution from an immutable scientific plan."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input("plan", valid_type=orm.SinglefileData)
        spec.input("source_graph", valid_type=orm.SinglefileData, required=False)
        spec.input("native_execution", valid_type=orm.SinglefileData, required=False)
        spec.input("execution_id", valid_type=orm.Str)
        spec.input("attempt", valid_type=orm.Int, default=lambda: orm.Int(1))
        spec.input("recovery_bundle", valid_type=orm.FolderData, required=False)
        spec.input("prior_archive", valid_type=orm.FolderData, required=False)
        spec.input("archive_byte_budget", valid_type=orm.Int, default=lambda: orm.Int(MAX_ARCHIVE_BYTES))
        spec.input("release_id", valid_type=orm.Str, required=False)
        spec.input("b0_reference", valid_type=orm.Str, required=False)
        spec.input("scratch_root", valid_type=orm.Str, required=False,
                   help="Trusted node-local directory; the runner stages a verified result tree back")
        spec.input("metadata.options.parser_name", default="qcl_negf.execution")
        spec.input("metadata.options.withmpi", default=False)
        spec.input("metadata.options.output_filename", default="solver.stdout")
        spec.output("result", valid_type=orm.SinglefileData, required=False)
        spec.output("native_submit_script", valid_type=orm.SinglefileData, required=False)
        spec.output("inventory", valid_type=orm.Dict, required=False)
        spec.output("delivery", valid_type=orm.Dict, required=False)
        spec.output("recovery_bundle", valid_type=orm.FolderData, required=False)
        spec.output("recovery_receipt", valid_type=orm.Dict, required=False)
        spec.output("prior_archive", valid_type=orm.FolderData, required=False)
        spec.output("selection", valid_type=orm.Dict, required=False)
        spec.output("performance_binding", valid_type=orm.SinglefileData, required=False)
        spec.exit_code(300, "ERROR_MISSING_RESULT", message="The solver did not write its scientific result")
        spec.exit_code(301, "ERROR_INVALID_RESULT", message="Scientific result is malformed or does not match its input")
        spec.exit_code(302, "ERROR_SCIENTIFIC_FAILURE", message="The solver reported scientific failure or incomplete execution")
        spec.exit_code(303, "ERROR_UNCONVERGED_RESULT", message="One or more scientific points did not converge")
        spec.exit_code(304, "PAUSED_WITH_RECOVERY", message="Attempt paused after a verified recovery publication")
        spec.exit_code(306, "B0_LOCAL_RAW_DELIVERY_PENDING", message="Local raw verified; bounded repository delivery is pending")
        spec.exit_code(305, "ERROR_INVALID_RECOVERY", message="Recovery or pause receipt is missing, corrupt or incompatible")

    def _setup_metadata(self, metadata):
        """Bind Slurm ownership before the immutable CalcJob node is stored."""
        if self.inputs.code.computer.scheduler_type == "core.slurm":
            metadata = dict(metadata)
            options = dict(metadata.get("options", {}))
            descriptor = {"execution_id": self.inputs.execution_id.value, "attempt": self.inputs.attempt.value,
                          "output_directory": "result", "solver_executable": str(self.inputs.code.filepath_executable)}
            encoded = base64.urlsafe_b64encode(json.dumps(descriptor, separators=(",", ":")).encode()).decode()
            commands = options.get("custom_scheduler_commands") or ""
            options["custom_scheduler_commands"] = commands + (
                f"\n#SBATCH --no-requeue\n#SBATCH --comment=qcl-negf-attempt-v1:{encoded}")
            metadata["options"] = options
        super()._setup_metadata(metadata)

    def prepare_for_submission(self, folder):
        if "source_graph" in self.inputs or "native_execution" in self.inputs:
            return self._prepare_native(folder)
        if "b0_reference" in self.inputs:
            from .b0 import resume_controller
            controller = resume_controller(self.inputs.b0_reference.value, self.node.uuid)
            with controller.operation("prepare_bound_calcjob"):
                return self._prepare_for_submission(folder)
        return self._prepare_for_submission(folder)

    def presubmit(self, folder):
        info = super().presubmit(folder)
        if "native_execution" not in self.inputs:
            return info
        from aiida.common.links import LinkType
        from .native_actor import MAX_FILE, _Reads, _related, _need
        budget = _Reads(8*1024*1024)
        with folder.open("_aiidasubmit.sh","rb") as stream:
            budget.charge(8*(MAX_FILE+1))
            raw = stream.read(MAX_FILE+1)
        _need(len(raw)<=MAX_FILE,"submit_script_bound")
        query = orm.QueryBuilder().append(orm.CalcJobNode,filters={"uuid":self.node.uuid},tag="calc")
        query.append(orm.Node,with_incoming="calc",edge_filters={"type":"create","label":"native_submit_script"},project="*")
        existing = budget.query(query)
        _need(len(existing)<=1,"ambiguous_submit_script")
        if not existing:
            script = orm.SinglefileData(file=io.BytesIO(raw),filename="_aiidasubmit.sh")
            self.out("native_submit_script",script)
            self.update_outputs()  # public genuine CREATE; original failure retained
        script = _related(self.node,LinkType.CREATE,"native_submit_script",budget,outgoing=True)
        _need(budget.file(script)==raw,"submit_script_conflict")
        return info

    def _prepare_native(self, folder):
        from .native_actor import (_Reads, MAX_METADATA, NativeActorUnavailable,
                                   _need, _validate_native_inputs, _reviewed_options)
        _need("source_graph" in self.inputs and "native_execution" in self.inputs,
              "native_input_pair_required")
        _need(not any(key in self.inputs for key in ("b0_reference", "recovery_bundle", "prior_archive")),
              "native_legacy_recovery_unavailable")
        # This selector is not authority. Platform must independently match the
        # actual argv against protected policy before its first durable claim.
        try:
            jobs_root = validate_scratch_root(os.environ.get("QCL_NEGF_NATIVE_JOBS_ROOT"))
        except ValueError as error:
            raise NativeActorUnavailable("native_output_mapping_unavailable") from error
        budget = _Reads(MAX_METADATA)
        native, plan, raws = _validate_native_inputs(self.inputs.plan, self.inputs.source_graph,
            self.inputs.native_execution, self.inputs.code, self.inputs.execution_id.value, budget)
        validate_plan(plan)
        _need(type(self.inputs.attempt.value) is int and self.inputs.attempt.value > 0,
              "transport_scalar_inputs")
        _need("release_id" in self.inputs and
              self.inputs.release_id.value == native["code"]["release_id"], "release_input_binding")
        _reviewed_options(self.inputs.code, self.node.get_options())
        resources = self.options.resources
        cores = resources.get("num_cores_per_mpiproc")
        _need(resources.get("num_machines") == resources.get("num_mpiprocs_per_machine") == 1 and
              type(cores) is int and cores > 0, "native_thread_resources")
        _need(self.options.environment_variables ==
              {"JULIA_NUM_THREADS": str(cores), "OPENBLAS_NUM_THREADS": "1"}, "native_environment")
        _need(self.options.output_filename == "solver.stdout", "native_control_filename")
        output = validate_scratch_root(jobs_root + "/" + native["storage"]["relative_execution_root"])
        for filename, raw in zip(("plan.json", "source-graph.json", "native-execution.json"), raws):
            with folder.open(filename, "wb") as stream:
                stream.write(raw)
        code = CodeInfo()
        code.code_uuid = self.inputs.code.uuid
        code.cmdline_params = ["run-plan", "plan.json", output, "--execution-id",
                               self.inputs.execution_id.value, "--source-graph", "source-graph.json"]
        # Transport attempt is not a native point counter; no --attempt alias.
        if "scratch_root" in self.inputs:
            code.cmdline_params.extend(["--scratch-root", validate_scratch_root(self.inputs.scratch_root.value)])
        code.stdout_name, code.stderr_name, code.withmpi = "solver.stdout", "solver.stderr", False
        info = CalcInfo()
        info.prepend_text = (
            "/run/current-system/sw/bin/qcl-negf-release guard --release-id " + shlex.quote(self.inputs.release_id.value)
            + " --solver-executable " + shlex.quote(str(self.inputs.code.filepath_executable))
            + " || exit 78\nunset QCL_NEGF_CAPTURE_SPEC")
        info.codes_info = [code]
        # Stock presubmit adds its two scheduler logs. Q1 still gates all gets.
        info.retrieve_list = ["series_result.json", "native-selection.json", "native-close.json"]
        return info

    def _prepare_for_submission(self, folder):
        plan = validate_plan(read_plan(self.inputs.plan))
        execution_id = self.inputs.execution_id.value
        if execution_id not in {execution["id"] for execution in plan["executions"]}:
            raise ValueError("Selected execution is absent from the frozen plan")
        if self.inputs.attempt.value < 1:
            raise ValueError("attempt must be positive")
        if self.inputs.archive_byte_budget.value < 1:
            raise ValueError("Archive transfer byte budget must be positive")
        if "prior_archive" in self.inputs and "recovery_bundle" not in self.inputs:
            raise ValueError("Prior archive requires an explicit recovery bundle")
        if "release_id" in self.inputs:
            release = self.inputs.release_id.value
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", release):
                raise ValueError("release_id must be a portable immutable identity")
            if (not isinstance(self.inputs.code, orm.InstalledCode)
                    or not str(self.inputs.code.filepath_executable).startswith("/nix/store/")):
                raise ValueError("Release-pinned execution requires an immutable Nix InstalledCode path")
        if self.inputs.metadata.options.withmpi:
            raise ValueError("The solver is threaded; MPI launch is not supported")
        resources = self.options.resources
        if resources.get("num_machines") != 1 or resources.get("num_mpiprocs_per_machine") != 1:
            raise ValueError("Use exactly one node and one threaded process per execution")
        cores = resources.get("num_cores_per_mpiproc")
        if type(cores) is not int or cores < 1:
            raise ValueError("Set a positive num_cores_per_mpiproc")
        if self.options.environment_variables.get("JULIA_NUM_THREADS") != str(cores):
            raise ValueError("JULIA_NUM_THREADS must match num_cores_per_mpiproc")
        if "b0_reference" in self.inputs:
            if (self.inputs.attempt.value != 1 or "release_id" not in self.inputs
                    or "recovery_bundle" in self.inputs or "prior_archive" in self.inputs
                    or self.inputs.code.computer.scheduler_type != "core.slurm"):
                raise ValueError("B0 requires one guarded Slurm attempt without restart inputs")
        with self.inputs.plan.open(mode="rb") as source:
            encoded = source.read(MAX_PLAN_BYTES + 1)
        if len(encoded) > MAX_PLAN_BYTES:
            raise ValueError("Frozen plan exceeds the input size limit")
        with folder.open("plan.json", "wb") as handle:
            handle.write(encoded)
        code = CodeInfo()
        code.code_uuid = self.inputs.code.uuid
        code.cmdline_params = ["run-plan", "plan.json", "result", "--execution-id", execution_id]
        code.cmdline_params.extend(["--attempt", str(self.inputs.attempt.value)])
        if "b0_reference" in self.inputs:
            from .b0 import ROOT_HELPER
            code.cmdline_params[0] = "run-bound-b0"
            code.cmdline_params.extend(["--root-slot-ref", self.inputs.b0_reference.value,
                                        "--root-helper", ROOT_HELPER])

        if "recovery_bundle" in self.inputs:
            from .recovery import read_metadata, verify_bundle, verify_prior_archive
            # Runner rechecks numerical compatibility before importing this state.
            repository = self.inputs.recovery_bundle.base.repository
            source, _ = read_metadata(repository, "receipt.json")
            source_attempt = source.get("identity", {}).get("attempt")
            if type(source_attempt) is not int or source_attempt >= self.inputs.attempt.value:
                raise ValueError("Recovery must belong to an earlier attempt")
            verified = verify_bundle(repository, "", execution_id=execution_id,
                          attempt=source_attempt, plan_fingerprint=plan["fingerprint"])
            recovery_files = {str(root / name) for root, _, names in repository.walk() for name in names}
            if recovery_files != set(verified["files"]):
                raise ValueError("Recovery input contains files outside its bounded portable closure")
            archive_repository = self.inputs.prior_archive.base.repository if "prior_archive" in self.inputs else None
            archive = verify_prior_archive(archive_repository, verified["progress"],
                byte_budget=self.inputs.archive_byte_budget.value,
                plan_points={point["id"]: point for point in plan["points"]})
            if archive_repository is not None:
                actual = {str(root / name) for root, _, names in archive_repository.walk() for name in names}
                if actual != set(archive["files"]):
                    raise ValueError("Prior archive input contains files outside its bounded transfer closure")
                code.cmdline_params.extend(["--archive-bundle", "prior-archive", "--archive-byte-budget",
                                            str(self.inputs.archive_byte_budget.value)])
            code.cmdline_params.extend(["--recovery-bundle", "recovery"])
        if "scratch_root" in self.inputs:
            code.cmdline_params.extend(["--scratch-root", validate_scratch_root(self.inputs.scratch_root.value)])
        code.stdout_name = self.options.output_filename
        code.stderr_name = "solver.stderr"
        code.withmpi = False
        info = CalcInfo()
        if "release_id" in self.inputs:
            info.prepend_text = (
                "/run/current-system/sw/bin/qcl-negf-release guard --release-id " + shlex.quote(self.inputs.release_id.value)
                + " --solver-executable " + shlex.quote(str(self.inputs.code.filepath_executable)) + " || exit 78")
        if "b0_reference" not in self.inputs:
            capability = "unset QCL_NEGF_CAPTURE_SPEC"
            try:
                from .capture_spec import capture_spec
                spec_raw = capture_spec(encoded, plan, calcjob_uuid=self.node.uuid,
                    execution_id=execution_id, attempt=self.inputs.attempt.value,
                    scratch_root=self.inputs.scratch_root.value if "scratch_root" in self.inputs else None)
                if spec_raw is not None:
                    with folder.open("capture-spec.json", "wb") as handle:
                        handle.write(spec_raw)
                    capability = "export QCL_NEGF_CAPTURE_SPEC=capture-spec.json"
            except (ValueError, TypeError, KeyError, OSError, ImportError):
                pass # Failed optional spec is retained in inputs; ordinary solver.
            preceding = getattr(info, "prepend_text", None)
            info.prepend_text = (preceding + "\n" if preceding else "") + capability
        info.codes_info = [code]
        # Operator diagnostics publish their immutable commit and full closure
        # under executions/.../attempt-N/artifacts, outside stationary archives.
        # Preserve execution provenance alongside all referenced payloads.
        info.retrieve_list = [(f"result/{path}", ".", 2) for path in (
            "series_result.json", "scientific_plan.json", "pause-receipt.json",
            "retrieval-manifest.json", "archive", "recovery", "executions")]
        info.retrieve_list.extend([self.options.output_filename, "solver.stderr"])
        # One optional bounded close sidecar; never retrieve raw performance trees.
        info.retrieve_list.append("performance-binding.json")
        if "b0_reference" in self.inputs:
            from .b0 import B0_CONTROL_FILES, validate_retrieval
            # Native transport port gates these exact files on quiescence +
            # pre-reserved sender/receiver leases before transfer. No raw glob.
            info.retrieve_list = list(validate_retrieval(B0_CONTROL_FILES))

        if "recovery_bundle" in self.inputs:
            info.local_copy_list = [(self.inputs.recovery_bundle.uuid, ".", "recovery")]
        if "prior_archive" in self.inputs:
            info.local_copy_list.append((self.inputs.prior_archive.uuid, ".", "prior-archive"))
        return info
