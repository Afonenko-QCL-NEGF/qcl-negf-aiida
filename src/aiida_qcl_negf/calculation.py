"""One threaded scientific execution submitted through an AiiDA scheduler."""

from __future__ import annotations

import base64
import io
import json
import os
import re
import shlex
import stat
import hashlib

from aiida import orm
from aiida.common.datastructures import CalcInfo, CodeInfo
from aiida.engine import CalcJob, ExitCode

from .validation import MAX_PLAN_BYTES, validate_plan, validate_scratch_root
from .data import read_plan
from .recovery import MAX_ARCHIVE_BYTES


_CAPTURE_EXPORT = ('export QCL_NEGF_CAPTURE_SPEC=capture-spec.json '
                   'QCL_NEGF_NATIVE_CAPTURE_INPUT=native-execution.json')
_CAPTURE_TARGET = 'capture-spec.json'
_CAPTURE_TEMP = 'capture-spec.json.tmp'  # Legacy reserved name: refusal/read only.


def _capture_close(fd,primary,state):
    try:os.close(fd)
    except BaseException as error:
        state['close_uncertain']=True
        if primary is None:raise
        primary.add_note('capture file close failed: '+type(error).__name__)


def _sandbox_stamp(value):
    return value.st_dev,value.st_ino,value.st_uid,value.st_gid,stat.S_IMODE(value.st_mode)


class _CaptureSandbox:
    """One bounded owning presubmit gate, never a general file/publication API."""
    def __init__(self,folder,state):
        from .native_actor import _need
        self.state,self.budget,self.path=state,state['budget'],folder.abspath
        self.fd=None
        self.budget.charge(1024)
        fd=os.open(self.path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        try:
            value=os.fstat(fd)
            _need(stat.S_ISDIR(value.st_mode) and value.st_uid==os.geteuid() and
                  value.st_mode & 0o022==0,'capture_sandbox_owner')
            self.stamp=_sandbox_stamp(value)
            _need(self.stamp==state['stamp'],'capture_sandbox_changed')
            self.fd=fd
            self.check()
        except BaseException as error:
            self.fd=None;_capture_close(fd,error,self.state);raise

    def check(self):
        from .native_actor import _need
        self.budget.charge(1024)
        _need(_sandbox_stamp(os.fstat(self.fd))==self.stamp and
              _sandbox_stamp(os.stat(self.path,follow_symlinks=False))==self.stamp,
              'capture_sandbox_changed')

    def identity(self,name):
        self.check()
        try:value=os.stat(name,dir_fd=self.fd,follow_symlinks=False)
        except FileNotFoundError:self.check();return None
        self.check()
        return value

    def absent(self):
        from .native_actor import _need
        for name in (_CAPTURE_TARGET,_CAPTURE_TEMP):
            _need(self.identity(name) is None,'capture_namespace_uncertain')

    def same(self,candidate):
        from .native_actor import _need
        self.budget.charge(8*16385)
        self.check()
        fd=os.open(_CAPTURE_TARGET,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=self.fd)
        primary=None
        try:
            value=os.fstat(fd)
            _need(stat.S_ISREG(value.st_mode) and value.st_nlink==1 and value.st_uid==os.geteuid()
                  and value.st_mode & 0o022==0 and value.st_size<=16384,'capture_candidate_file')
            stamp=_sandbox_stamp(value)
            _need(self.state['owned'].get(_CAPTURE_TARGET)==(value.st_dev,value.st_ino),
                  'capture_candidate_unknown')
            raw=os.read(fd,16385)
            _need(raw==candidate and os.read(fd,1)==b'','capture_candidate_changed')
            named=self.identity(_CAPTURE_TARGET);after=os.fstat(fd)
            _need(named is not None and _sandbox_stamp(named)==stamp==_sandbox_stamp(after)
                  and after.st_size==value.st_size and after.st_mtime_ns==value.st_mtime_ns
                  and after.st_ctime_ns==value.st_ctime_ns,'capture_candidate_changed')
        except BaseException as error:
            primary=error;raise
        finally:_capture_close(fd,primary,self.state)
        _need(self.identity(_CAPTURE_TEMP) is None,'capture_temporary_uncertain')
        self.check()

    def publish(self,candidate):
        from .native_actor import _need
        self.absent()
        _need(not self.state.get('created'),'capture_previous_unconfirmed')
        self.budget.charge(8*(len(candidate)+1)+4096)
        self.check()
        fd=os.open(_CAPTURE_TARGET,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=self.fd)
        self.state['created']=True
        self.state['confirmed']=False
        primary=None
        try:
            value=os.fstat(fd);self.state['owned'][_CAPTURE_TARGET]=(value.st_dev,value.st_ino)
            _need(stat.S_ISREG(value.st_mode) and value.st_uid==os.geteuid() and value.st_nlink==1,
                  'capture_candidate_owner')
            offset=0
            while offset<len(candidate):
                self.check();written=os.write(fd,memoryview(candidate)[offset:])
                _need(written>0,'capture_partial_write');offset+=written
            os.fsync(fd)
            self.check()
            os.fsync(self.fd)
        except BaseException as error:
            primary=error;raise
        finally:_capture_close(fd,primary,self.state)

    def close(self):
        fd,self.fd=self.fd,None
        if fd is not None:os.close(fd)


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
        spec.output("native_binding", valid_type=orm.SinglefileData, required=False)
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

    def parse_retrieved_output(self, retrieved_temporary_folder: str | None = None) -> ExitCode | None:
        if "source_graph" not in self.inputs and "native_execution" not in self.inputs:
            return super().parse_retrieved_output(retrieved_temporary_folder)
        from .saved_sources import (_native_operation, _native_pin, _native_related,
            _readback_native_outputs, _NATIVE_METADATA, _NATIVE_IO, _native_weight, _fail)
        from aiida.common.links import LinkType
        with _native_operation(_NATIVE_METADATA,_NATIVE_IO) as ledger:
            exit_code=super().parse_retrieved_output(retrieved_temporary_folder)
            # The parser emits only an all-validated unstored pair. The owning
            # Process writes genuine CREATE edges, then reads them back before
            # it constructs the scalar selection. Partial failures never delete
            # or reconstruct existing nodes and preserve the original exit code.
            if 'result' not in self.outputs or 'native_binding' not in self.outputs:
                return exit_code or self.exit_codes.ERROR_INVALID_RESULT
            try:
                expected={}
                for name in ('result','native_binding'):
                    node=self.outputs[name]
                    ledger.reserve('pending output repository copy/readback',metadata=8192)
                    with node.open(mode='rb') as stream:
                        stream.seek(0,2);size=stream.tell()
                        ledger.reserve('pending output exact bytes',metadata=8*(size+1),io=size+1)
                        stream.seek(0);raw=stream.read(size+1)
                    if len(raw)!=size:_fail('source_identity_mismatch','native_pending_output_changed')
                    expected[name]=_native_pin(node,raw)
                self.update_outputs()
                scalar=_readback_native_outputs(self.node,expected_result=expected['result'],
                    expected_binding=expected['native_binding'],maximum_metadata_bytes=_NATIVE_METADATA)
                ledger.reserve('scalar selection constructor/readback',metadata=2*_native_weight(scalar))
                selection=orm.Dict(dict=scalar)
                self.out('selection',selection)
                self.update_outputs()
                actual=_native_related(self.node,LinkType.CREATE,'selection',ledger,outgoing=True)
                if not isinstance(actual,orm.Dict) or actual.uuid!=selection.uuid or actual.get_dict()!=scalar:
                    _fail('source_identity_mismatch','native_selection_CREATE_readback')
            except (ValueError,TypeError,KeyError,AttributeError,OSError) as exception:
                self.logger.error('Native output readback unavailable: %s',exception)
                return exit_code or self.exit_codes.ERROR_INVALID_RESULT
            return exit_code

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
        state=self._native_capture_state
        budget=state['budget']
        budget.charge(8*(MAX_FILE+1))
        with folder.open("_aiidasubmit.sh","rb") as stream:
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
        _need(budget.file(script,role="submit_script")==raw,"submit_script_conflict")
        self._native_capture_gate(folder,raw)
        return info

    def _native_capture_gate(self,folder,raw_script):
        from .native_actor import _load_original,_need
        from .capture_spec import native_capture_spec
        from qcl_negf_contracts.messages import decode
        state=self._native_capture_state;budget=state['budget']
        guard=_CaptureSandbox(folder,state)
        primary=None;optional_error=None;confirmed=False
        try:
            if not state['eligible']:
                guard.absent()
                return
            # Mandatory real graph/CREATE errors never enter optional publication.
            budget.cache.clear()
            calc,code,launch,raw_native,original_script=_load_original(self.node.uuid,budget)
            _need(calc.uuid==self.node.uuid and code.uuid==self.inputs.code.uuid and
                  original_script==raw_script and launch['execution_id']==self.inputs.execution_id.value and
                  launch['transport_attempt']==self.inputs.attempt.value,'capture_original_identity')
            raw_plan=budget.file(self.inputs.plan,role='plan')
            _need(launch['files']['plan']==dict(bytes=len(raw_plan),sha256=hashlib.sha256(raw_plan).hexdigest()),
                  'capture_original_plan')
            try:
                budget.charge(8*(len(raw_plan)+16385))
                token=None if state['candidate'] is None else json.loads(state['candidate'])['capture_token']
                candidate=native_capture_spec(raw_plan,decode(raw_plan,maximum=1024*1024),
                    calcjob_uuid=self.node.uuid,run_uuid=launch['run_uuid'],execution_id=launch['execution_id'],
                    attempt=launch['transport_attempt'],capture_token=token)
                _need(candidate is not None,'capture_candidate_ineligible')
            except Exception as error:
                optional_error=error
                try:
                    _need(not state.get('created'),'capture_postcreate_unconfirmed')
                    guard.absent()
                except BaseException as cleanup:
                    error.add_note('capture generation/namespace unavailable: '+type(cleanup).__name__)
                    raise error
                return
            if state['candidate'] is not None:
                _need(candidate==state['candidate'],'capture_candidate_stale')
            else:state['candidate']=candidate
            if guard.identity(_CAPTURE_TARGET) is not None:
                _need(state.get('confirmed'),'capture_candidate_unconfirmed')
                guard.same(candidate)
                confirmed=True
                return
            try:
                guard.publish(candidate)
                guard.same(candidate)
                confirmed=True
            except BaseException as error:
                optional_error=error
                if not isinstance(error,Exception) or state.get('close_uncertain'):raise
                try:
                    if state.get('created'):
                        # No destructive fallback. A created inode can continue
                        # only after complete same-object bytes and defined close.
                        guard.same(candidate)
                        confirmed=True
                    else:guard.absent()
                except BaseException as proof:
                    error.add_note('capture candidate/namespace unavailable: '+type(proof).__name__)
                    raise error
            guard.check()
        except BaseException as error:
            primary=error
            raise
        finally:
            try:guard.close()
            except BaseException as cleanup:
                state['close_uncertain']=True
                original=primary if primary is not None else optional_error
                if original is None:raise
                original.add_note('capture sandbox close failed: '+type(cleanup).__name__)
                if primary is None:raise original
            else:
                if confirmed:state['confirmed']=True

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
        state=getattr(self,"_native_capture_state",None)
        if state is None or state['path']!=folder.abspath:
            value=os.stat(folder.abspath,follow_symlinks=False)
            _need(stat.S_ISDIR(value.st_mode) and value.st_uid==os.geteuid() and value.st_mode & 0o022==0,
                  "capture_sandbox_owner")
            state=dict(path=folder.abspath,stamp=_sandbox_stamp(value),budget=_Reads(MAX_METADATA),
                       candidate=None,owned={})
            self._native_capture_state=state
        _need(not state.get('close_uncertain'),'capture_previous_close_uncertain')
        _need(not state.get('created') or state.get('confirmed'),'capture_previous_unconfirmed')
        budget=state['budget']
        budget.cache.clear()  # Fresh full original bytes, without refund/reset.
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
                               self.inputs.execution_id.value, "--source-graph", "source-graph.json",
                               "--native-execution", "native-execution.json",
                               "--native-calcjob-uuid", self.node.uuid]
        # Transport attempt is not a native point counter; no --attempt alias.
        if "scratch_root" in self.inputs:
            code.cmdline_params.extend(["--scratch-root", validate_scratch_root(self.inputs.scratch_root.value)])
        code.stdout_name, code.stderr_name, code.withmpi = "solver.stdout", "solver.stderr", False
        info = CalcInfo()
        from .capture_spec import native_capture_eligible
        eligible=native_capture_eligible(raws[0],plan,execution_id=self.inputs.execution_id.value,
            attempt=self.inputs.attempt.value,scratch_root=self.inputs.scratch_root.value if "scratch_root" in self.inputs else None)
        if 'eligible' in state:_need(state['eligible']==eligible,"capture_eligibility_changed")
        state['eligible']=eligible
        info.prepend_text = (
            "/run/current-system/sw/bin/qcl-negf-release guard --release-id " + shlex.quote(self.inputs.release_id.value)
            + " --solver-executable " + shlex.quote(str(self.inputs.code.filepath_executable))
            + " || exit 78\n" + (_CAPTURE_EXPORT if eligible else "unset QCL_NEGF_CAPTURE_SPEC"))
        info.codes_info = [code]
        # Stock presubmit adds its two scheduler logs. Q1 still gates all gets.
        info.retrieve_list = ["series_result.json", "native-selection.json", "native-close.json", "performance-source.json"]
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
