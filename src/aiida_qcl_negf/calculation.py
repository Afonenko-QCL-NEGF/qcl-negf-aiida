"""One threaded scientific execution submitted through an AiiDA scheduler."""

from __future__ import annotations

from aiida import orm
from aiida.common.datastructures import CalcInfo, CodeInfo
from aiida.engine import CalcJob

from .validation import MAX_PLAN_BYTES, validate_plan, validate_scratch_root
from .data import read_plan


class QCLExecutionCalculation(CalcJob):
    """Execute a selected execution from an immutable scientific plan."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input("plan", valid_type=orm.SinglefileData)
        spec.input("execution_id", valid_type=orm.Str)
        spec.input("scratch_root", valid_type=orm.Str, required=False,
                   help="Trusted node-local directory; the runner stages a verified result tree back")
        spec.input("metadata.options.parser_name", default="qcl_negf.execution")
        spec.input("metadata.options.withmpi", default=False)
        spec.input("metadata.options.output_filename", default="solver.stdout")
        spec.output("result", valid_type=orm.SinglefileData, required=False)
        spec.output("inventory", valid_type=orm.Dict, required=False)
        spec.exit_code(300, "ERROR_MISSING_RESULT", message="The solver did not write its scientific result")
        spec.exit_code(301, "ERROR_INVALID_RESULT", message="Scientific result is malformed or does not match its input")
        spec.exit_code(302, "ERROR_SCIENTIFIC_FAILURE", message="The solver reported scientific failure or incomplete execution")
        spec.exit_code(303, "ERROR_UNCONVERGED_RESULT", message="One or more scientific points did not converge")

    def prepare_for_submission(self, folder):
        plan = validate_plan(read_plan(self.inputs.plan))
        execution_id = self.inputs.execution_id.value
        if execution_id not in {execution["id"] for execution in plan["executions"]}:
            raise ValueError("Selected execution is absent from the frozen plan")
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
        if "scratch_root" in self.inputs:
            code.cmdline_params.extend(["--scratch-root", validate_scratch_root(self.inputs.scratch_root.value)])
        code.stdout_name = self.options.output_filename
        code.stderr_name = "solver.stderr"
        code.withmpi = False
        info = CalcInfo()
        info.codes_info = [code]
        info.retrieve_list = ["result", self.options.output_filename, "solver.stderr"]
        return info
