"""A bounded parallel workflow; Slurm remains the resource scheduler."""

from __future__ import annotations

from aiida import orm
from aiida.engine import ToContext, WorkChain, while_

from .calculation import QCLExecutionCalculation
from .validation import scheduler_options, validate_plan, validate_scratch_root
from .data import read_plan


class QCLPlanWorkChain(WorkChain):
    """Run independent executions in parallel and preserve every child result.

    Children use separate working directories. Point continuation stays inside one
    solver invocation. A failed execution does not discard unrelated work.
    """

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input("plan", valid_type=orm.SinglefileData)
        spec.input("code", valid_type=orm.AbstractCode)
        spec.input("resources", valid_type=orm.Dict)
        spec.input("scratch_root", valid_type=orm.Str, required=False)
        spec.input("max_concurrent", valid_type=orm.Int, default=lambda: orm.Int(4))
        spec.output_namespace("results", dynamic=True, valid_type=orm.SinglefileData)
        spec.output_namespace("artifacts", dynamic=True, valid_type=orm.FolderData)
        spec.exit_code(400, "ERROR_EXECUTION_FAILED", message="At least one execution failed; partial outputs are retained")
        spec.exit_code(401, "ERROR_INVALID_PLAN", message="Plan or resources are not supported")
        spec.outline(cls.setup, while_(cls.has_pending)(cls.submit_ready, cls.inspect_batch), cls.finalize)

    def setup(self):
        try:
            plan = validate_plan(read_plan(self.inputs.plan))
            self.ctx.options = scheduler_options(self.inputs.resources.get_dict())
            if "scratch_root" in self.inputs:
                validate_scratch_root(self.inputs.scratch_root.value)
            if not 1 <= self.inputs.max_concurrent.value <= 256:
                raise ValueError("max_concurrent must be between 1 and 256")
        except (ValueError, TypeError, KeyError) as exception:
            self.report(str(exception))
            return self.exit_codes.ERROR_INVALID_PLAN
        self.ctx.pending = [execution["id"] for execution in plan["executions"]]
        self.ctx.failed = []
        self.ctx.batch = {}
        self.ctx.sequence = 0

    def has_pending(self):
        return bool(self.ctx.pending)

    def submit_ready(self):
        self.ctx.batch = {}
        futures = {}
        for identifier in list(self.ctx.pending)[: self.inputs.max_concurrent.value]:
            key = f"execution_{self.ctx.sequence}"
            self.ctx.sequence += 1
            options = {}
            if "scratch_root" in self.inputs:
                options["scratch_root"] = self.inputs.scratch_root
            child = self.submit(
                QCLExecutionCalculation,
                code=self.inputs.code,
                plan=self.inputs.plan,
                execution_id=orm.Str(identifier),
                metadata={"label": identifier, "call_link_label": key, "options": self.ctx.options},
                **options,
            )
            self.ctx.batch[key] = identifier
            self.ctx.pending.remove(identifier)
            futures[key] = child
        return ToContext(**futures)

    def inspect_batch(self):
        for key, identifier in self.ctx.batch.items():
            child = self.ctx[key]
            # Link labels use stable ordinal keys because scientific IDs may contain dots.
            if "result" in child.outputs:
                self.out(f"results.{key}", child.outputs.result)
            if "retrieved" in child.outputs:
                self.out(f"artifacts.{key}", child.outputs.retrieved)
            if not child.is_finished_ok:
                self.ctx.failed.append(identifier)
                self.report(f"Execution {identifier} failed with exit status {child.exit_status}")

    def finalize(self):
        if self.ctx.failed:
            self.report(f"Failed: {self.ctx.failed}")
            return self.exit_codes.ERROR_EXECUTION_FAILED
