"""A bounded parallel workflow; Slurm remains the resource scheduler."""

from __future__ import annotations

from aiida import orm
from aiida.engine import BaseRestartWorkChain, ProcessHandlerReport, ToContext, WorkChain, process_handler, while_
from aiida.schedulers.datastructures import JobState

from .calculation import QCLExecutionCalculation
from .validation import scheduler_options, validate_plan, validate_scratch_root
from .data import read_plan
from .recovery import MAX_ARCHIVE_BYTES


def attempt_decision(exit_status, *, scheduler_stopped, recovery_available):
    """Classify infrastructure outcomes without modifying scientific contracts."""
    if exit_status == 0:
        return "completed"
    if exit_status == 303:
        return "scientific_terminal"
    if exit_status in (301, 305):
        return "invalid_result"
    if exit_status not in (100, 110, 120, 140, 300, 304):
        return "scientific_terminal"
    if not scheduler_stopped:
        return "ambiguous"
    if not recovery_available:
        return "invalid_recovery" if exit_status == 304 else "no_recovery"
    return "retry"


class QCLExecutionRestartWorkChain(BaseRestartWorkChain):
    """One scientific execution, sequential attempts and a finite retry budget."""

    _process_class = QCLExecutionCalculation

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.expose_inputs(QCLExecutionCalculation, exclude=("metadata",))
        spec.input("options", valid_type=orm.Dict)
        spec.inputs["max_iterations"].default = lambda: orm.Int(3)
        spec.input("backoff_seconds", valid_type=orm.Int, default=lambda: orm.Int(10))
        spec.expose_outputs(QCLExecutionCalculation)
        spec.exit_code(410, "ERROR_AMBIGUOUS_OWNER", message="Previous scheduler ownership is not confirmed stopped")
        spec.exit_code(411, "ERROR_NO_RECOVERY", message="No verified portable checkpoint is available")
        spec.exit_code(412, "ERROR_INVALID_RETRY_POLICY", message="Retry policy is not finite or permits uncontrolled restarts")
        spec.outline(cls.setup, while_(cls.should_run_process)(cls.prepare_attempt, cls.run_process, cls.inspect_process), cls.results)

    def setup(self):
        super().setup()
        pause_policy = self.inputs.get("pause_on_max_iterations")
        unhandled_policy = self.inputs.get("on_unhandled_failure")
        overrides = self.inputs.get("handler_overrides")
        if (not 1 <= self.inputs.max_iterations.value <= 100
                or not 0 <= self.inputs.backoff_seconds.value <= 300
                or (pause_policy is not None and pause_policy.value)
                or (unhandled_policy is not None and unhandled_policy.value != "abort")
                or (overrides is not None and overrides.get_dict())):
            return self.exit_codes.ERROR_INVALID_RETRY_POLICY
        self.ctx.inputs = dict(self.exposed_inputs(QCLExecutionCalculation))
        self.ctx.inputs["metadata"] = {"options": self.inputs.options.get_dict()}
        self.ctx.first_attempt = self.inputs.attempt.value

    def prepare_attempt(self):
        self.ctx.inputs["attempt"] = orm.Int(self.ctx.first_attempt + self.ctx.iteration)
        if self.ctx.iteration:
            # Bounded delay runs in the next scheduler job, never blocks the daemon.
            options = dict(self.ctx.inputs["metadata"]["options"])
            original = self.inputs.options.get_dict().get("prepend_text", "")
            options["prepend_text"] = f"sleep {self.inputs.backoff_seconds.value}\n" + original
            self.ctx.inputs["metadata"]["options"] = options

    def inspect_process(self):
        result = super().inspect_process()
        if result is not None and result.status == self.exit_codes.ERROR_MAXIMUM_ITERATIONS_EXCEEDED.status:
            self._attach_outputs(self.ctx.children[-1])
        return result

    @process_handler(priority=100)
    def handle_attempt(self, node):
        decision = attempt_decision(node.exit_status,
            scheduler_stopped=node.is_terminated and node.get_scheduler_state() == JobState.DONE,
            recovery_available="recovery_bundle" in node.outputs and "recovery_receipt" in node.outputs)
        if decision == "completed":
            return None
        if decision == "retry":
            self.ctx.inputs["recovery_bundle"] = node.outputs.recovery_bundle
            self.ctx.inputs.pop("prior_archive", None)
            if "prior_archive" in node.outputs:
                self.ctx.inputs["prior_archive"] = node.outputs.prior_archive
            self.report(f"Attempt {node.inputs.attempt.value} stopped; resuming verified state {node.outputs.recovery_receipt.get_dict()['state_id']}")
            return ProcessHandlerReport(do_break=True)
        self.ctx.is_finished = True
        self._attach_outputs(node)
        if decision == "ambiguous":
            exit_code = self.exit_codes.ERROR_AMBIGUOUS_OWNER
        elif decision in ("no_recovery", "invalid_recovery"):
            exit_code = self.exit_codes.ERROR_NO_RECOVERY
        else:
            exit_code = self.process_class.spec().exit_codes.get(node.exit_status)
            if exit_code is None:
                from aiida.engine import ExitCode
                exit_code = ExitCode(node.exit_status or 302, node.exit_message)
        return ProcessHandlerReport(do_break=True, exit_code=exit_code)


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
        spec.input("max_attempts", valid_type=orm.Int, default=lambda: orm.Int(3))
        spec.input("retry_backoff_seconds", valid_type=orm.Int, default=lambda: orm.Int(10))
        spec.input("archive_byte_budget", valid_type=orm.Int, default=lambda: orm.Int(MAX_ARCHIVE_BYTES))
        spec.input("release_id", valid_type=orm.Str, required=False)
        spec.output_namespace("results", dynamic=True, valid_type=orm.SinglefileData)
        spec.output_namespace("artifacts", dynamic=True, valid_type=orm.FolderData)
        spec.output_namespace("selections", dynamic=True, valid_type=orm.Dict)
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
            if (not 1 <= self.inputs.max_attempts.value <= 100
                    or not 0 <= self.inputs.retry_backoff_seconds.value <= 300
                    or self.inputs.archive_byte_budget.value < 1):
                raise ValueError("Retry and transfer budgets must be finite and positive")
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
            if "release_id" in self.inputs:
                options["release_id"] = self.inputs.release_id
            child = self.submit(
                QCLExecutionRestartWorkChain,
                code=self.inputs.code,
                plan=self.inputs.plan,
                execution_id=orm.Str(identifier),
                options=orm.Dict(dict=self.ctx.options),
                max_iterations=self.inputs.max_attempts,
                backoff_seconds=self.inputs.retry_backoff_seconds,
                archive_byte_budget=self.inputs.archive_byte_budget,
                metadata={"label": identifier, "call_link_label": key},
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
            if "selection" in child.outputs:
                self.out(f"selections.{key}", child.outputs.selection)
            if not child.is_finished_ok:
                self.ctx.failed.append(identifier)
                self.report(f"Execution {identifier} failed with exit status {child.exit_status}")

    def finalize(self):
        if self.ctx.failed:
            self.report(f"Failed: {self.ctx.failed}")
            return self.exit_codes.ERROR_EXECUTION_FAILED
