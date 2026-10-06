"""One threaded scientific execution submitted through an AiiDA scheduler."""

from __future__ import annotations

import base64
import json
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
        spec.input("execution_id", valid_type=orm.Str)
        spec.input("attempt", valid_type=orm.Int, default=lambda: orm.Int(1))
        spec.input("recovery_bundle", valid_type=orm.FolderData, required=False)
        spec.input("prior_archive", valid_type=orm.FolderData, required=False)
        spec.input("archive_byte_budget", valid_type=orm.Int, default=lambda: orm.Int(MAX_ARCHIVE_BYTES))
        spec.input("release_id", valid_type=orm.Str, required=False)
        spec.input("scratch_root", valid_type=orm.Str, required=False,
                   help="Trusted node-local directory; the runner stages a verified result tree back")
        spec.input("metadata.options.parser_name", default="qcl_negf.execution")
        spec.input("metadata.options.withmpi", default=False)
        spec.input("metadata.options.output_filename", default="solver.stdout")
        spec.output("result", valid_type=orm.SinglefileData, required=False)
        spec.output("inventory", valid_type=orm.Dict, required=False)
        spec.output("recovery_bundle", valid_type=orm.FolderData, required=False)
        spec.output("recovery_receipt", valid_type=orm.Dict, required=False)
        spec.output("prior_archive", valid_type=orm.FolderData, required=False)
        spec.output("selection", valid_type=orm.Dict, required=False)
        spec.exit_code(300, "ERROR_MISSING_RESULT", message="The solver did not write its scientific result")
        spec.exit_code(301, "ERROR_INVALID_RESULT", message="Scientific result is malformed or does not match its input")
        spec.exit_code(302, "ERROR_SCIENTIFIC_FAILURE", message="The solver reported scientific failure or incomplete execution")
        spec.exit_code(303, "ERROR_UNCONVERGED_RESULT", message="One or more scientific points did not converge")
        spec.exit_code(304, "PAUSED_WITH_RECOVERY", message="Attempt paused after a verified recovery publication")
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
        info.codes_info = [code]
        # Operator diagnostics publish their immutable commit and full closure
        # under executions/.../attempt-N/artifacts, outside stationary archives.
        # Preserve execution provenance alongside all referenced payloads.
        info.retrieve_list = [(f"result/{path}", ".", 2) for path in (
            "series_result.json", "scientific_plan.json", "pause-receipt.json",
            "retrieval-manifest.json", "archive", "recovery", "executions")]
        info.retrieve_list.extend([self.options.output_filename, "solver.stderr"])
        if "recovery_bundle" in self.inputs:
            info.local_copy_list = [(self.inputs.recovery_bundle.uuid, ".", "recovery")]
        if "prior_archive" in self.inputs:
            info.local_copy_list.append((self.inputs.prior_archive.uuid, ".", "prior-archive"))
        return info
