"""Ordinary Slurm/Local extensions; unavailable native gates remain HOLD.

No profile setup, scheduler replacement, retry worker or scientific acceptance.
The installed Platform client alone owns privileged dispatch/journaling. Its
current actor and Q1 prerequisites are deliberately not fabricated here.
"""
from __future__ import annotations

from contextlib import contextmanager
import os
from pathlib import Path
import re
import selectors
import shlex
import signal
import subprocess
import time

import click
from aiida.schedulers.plugins.slurm import SlurmScheduler
from aiida.schedulers.datastructures import JobState
from aiida.transports.plugins.local import LocalTransport
from plumpy.process_states import PauseInterruption
from qcl_negf_contracts.messages import decode, encode

from . import native_actor

_CLIENT = "/run/current-system/sw/bin/qcl-negf-native-attempt-client"
_COMMAND_SECONDS = 15
_CLEANUP_SECONDS = 2
_OUTPUT_BYTES = 262144


def _hold(reason):
    raise PauseInterruption("native attempt HOLD: " + reason)


class _Operation:
    def __init__(self):
        self.deadline = time.monotonic() + 90
        self.calls = 0
        self.reads = native_actor._Reads(8*1024*1024,before=self._deadline)

    def _deadline(self):
        if time.monotonic() >= self.deadline:
            _hold("adapter_budget")

    def before(self, size=0):
        try:
            self.reads.charge(size)
        except native_actor.NativeActorUnavailable as error:
            raise PauseInterruption("native attempt HOLD: adapter_budget") from error


def _owned_command(proc):
    """Observe this unreaped direct child without releasing its PID generation."""
    if proc.returncode is not None:
        raise ChildProcessError("native command leader already reaped")
    # Popen.poll/wait use waitpid and would release an exited session leader.
    # WNOWAIT retains the direct-child identity through the group signal.
    status = os.waitid(os.P_PID,proc.pid,os.WEXITED | os.WNOHANG | os.WNOWAIT)
    if ((status is not None and status.si_pid != proc.pid) or
            os.getsid(proc.pid) != proc.pid or os.getpgid(proc.pid) != proc.pid):
        raise ChildProcessError("native command session ownership unavailable")
    return status


def _cleanup_command(proc, deadline):
    """One owned group signal before reap; unknown cleanup cannot be success."""
    if proc is None:
        return None
    until = min(deadline,time.monotonic() + _CLEANUP_SECONDS)
    failure = None
    try:
        _owned_command(proc)
        os.killpg(proc.pid,signal.SIGKILL)
        # Only now release the leader. Never signal this numeric PGID again.
        proc.wait(timeout=max(0,until-time.monotonic()))
        while True:
            try:
                os.killpg(proc.pid,0)  # observation only; a reused group stays unknown
            except ProcessLookupError:
                break
            remaining = until-time.monotonic()
            if remaining <= 0:
                raise TimeoutError("native command group cleanup unknown")
            time.sleep(min(.01,remaining))
    except BaseException as error:
        failure = error
    finally:
        for stream in (proc.stdin,proc.stdout,proc.stderr):
            try:
                stream.close()
            except BaseException as error:
                if failure is None:
                    failure = error
    return failure


class NativeLocalTransport(LocalTransport):
    """Only local opt-in Computer; controls cannot bypass native/Q1 closure."""
    _valid_auth_options = [("native_computer_uuid", {
        "type": click.STRING, "prompt": "Native Computer UUID", "required": True,
        "help": "Exact ORM selector; not an admission certificate."})]

    def __init__(self, *args, native_computer_uuid, **kwargs):
        self.native_computer_uuid = native_actor._uuid(native_computer_uuid)
        self._native_operation = None
        super().__init__(*args, **kwargs)

    @contextmanager
    def _operation(self):
        outer = self._native_operation is None
        if outer:
            self._native_operation = _Operation()
        try:
            self._native_operation.before()
            yield self._native_operation
        finally:
            if outer:
                self._native_operation = None

    def exec_command_wait_bytes(self, command, stdin=None, workdir=None, **kwargs):
        """Same public Local API, with bounded owned child/output/cleanup."""
        with self._operation() as budget:
            budget.calls += 1
            if budget.calls > 64 or type(command) is not str:
                _hold("adapter_command_bound")
            data = b"" if stdin is None else stdin.encode() if type(stdin) is str else stdin
            if type(data) is not bytes or len(data) > 65536:
                _hold("adapter_input_bound")
            budget.before(len(data) + 2 * _OUTPUT_BYTES)
            cwd = str(workdir) if workdir is not None else self.getcwd()
            until = min(budget.deadline - _CLEANUP_SECONDS, time.monotonic() + _COMMAND_SECONDS)
            proc, primary, result = None, None, None
            try:
                proc = subprocess.Popen(["bash", "--noprofile", "--norc", "-c", command],
                    cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    start_new_session=True)
                buffers = {"stdout":bytearray(), "stderr":bytearray()}
                offset = 0
                with selectors.DefaultSelector() as selector:
                    for stream, name in ((proc.stdout,"stdout"),(proc.stderr,"stderr")):
                        os.set_blocking(stream.fileno(),False)
                        selector.register(stream,selectors.EVENT_READ,name)
                    if data:
                        os.set_blocking(proc.stdin.fileno(),False)
                        selector.register(proc.stdin,selectors.EVENT_WRITE,"stdin")
                    else:
                        proc.stdin.close()
                    while selector.get_map():
                        budget.before()
                        remaining = until - time.monotonic()
                        if remaining <= 0:
                            raise TimeoutError("native adapter command deadline")
                        for key,_ in selector.select(min(.1,remaining)):
                            if key.data == "stdin":
                                try:
                                    offset += os.write(key.fd,data[offset:offset+4096])
                                except BrokenPipeError:
                                    offset = len(data)
                                if offset == len(data):
                                    selector.unregister(key.fileobj);key.fileobj.close()
                            else:
                                chunk = os.read(key.fd,4096)
                                if not chunk:
                                    selector.unregister(key.fileobj)
                                else:
                                    if sum(map(len,buffers.values())) + len(chunk) > _OUTPUT_BYTES:
                                        raise ValueError("native adapter output bound")
                                    buffers[key.data].extend(chunk)
                while _owned_command(proc) is None:
                    budget.before()
                    remaining = until-time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError("native adapter command deadline")
                    time.sleep(min(.01,remaining))
                result = bytes(buffers["stdout"]),bytes(buffers["stderr"])
            except BaseException as error:
                primary = error
            finally:
                cleanup = _cleanup_command(proc,budget.deadline)
            if primary is not None:
                if cleanup is not None:
                    primary.add_note("native cleanup unknown: " + type(cleanup).__name__)
                if isinstance(primary,(OSError,ValueError,TimeoutError,subprocess.TimeoutExpired)):
                    raise PauseInterruption("native attempt HOLD: command_outcome_unknown") from primary
                raise primary
            if cleanup is not None:
                if not isinstance(cleanup,Exception):
                    raise cleanup
                raise PauseInterruption("native attempt HOLD: command_cleanup_unknown") from cleanup
            budget.before()
            return proc.returncode,*result

    def get(self, remotepath, localpath, *args, **kwargs):
        # No public Q1 controls export/close reader exists yet. A path, dict,
        # scheduler DONE or injected callback cannot unlock transfer.
        _hold("q1_controls_unavailable")

    def getfile(self, remotepath, localpath, *args, **kwargs):
        _hold("q1_controls_unavailable")

    def gettree(self, remotepath, localpath, *args, **kwargs):
        _hold("q1_controls_unavailable")


def _reply(raw, calcjob_uuid, *, state):
    value = decode(raw,maximum=_OUTPUT_BYTES)
    if (type(value) is not dict or set(value) != {"version","state","calcjob_uuid","receipt_ref","job","controls","reason"}
            or type(value["version"]) is not int or value["version"] != 1
            or value["calcjob_uuid"] != calcjob_uuid or value["state"] != state
            or value["reason"] is not None or value["controls"] is not None
            or type(value["receipt_ref"]) is not str or not 0 < len(value["receipt_ref"]) <= 256):
        _hold("native_reply_unqualified")
    job = value["job"]
    fields = {"job_id","cluster","submit_time","controller_machine_id","controller_boot_id",
              "dispatch_id","generation","node_name","start_time","worker_machine_id","worker_boot_id"}
    if type(job) is not dict or set(job) != fields:
        _hold("native_job_unqualified")
    if not (type(job["job_id"]) is str and re.fullmatch(r"[1-9][0-9]{0,9}",job["job_id"]) and
            type(job["generation"]) is str and re.fullmatch(r"[0-9a-f]{64}",job["generation"]) and
            type(job["controller_machine_id"]) is str and re.fullmatch(r"[0-9a-f]{32}",job["controller_machine_id"])):
        _hold("native_job_unqualified")
    native_actor._uuid(job["controller_boot_id"])
    native_actor._uuid(job["dispatch_id"])
    if not (type(job["cluster"]) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}",job["cluster"]) and
            type(job["submit_time"]) is str and 0 < len(job["submit_time"]) <= 64):
        _hold("native_job_unqualified")
    allocation = [job[k] for k in ("node_name","start_time","worker_machine_id","worker_boot_id")]
    if not (all(v is None for v in allocation) or all(v is not None for v in allocation)):
        _hold("native_allocation_unqualified")
    if allocation[0] is not None:
        if not (type(allocation[0]) is str and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}",allocation[0]) and
                type(allocation[1]) is str and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}",allocation[1]) and
                type(allocation[2]) is str and re.fullmatch(r"[0-9a-f]{32}",allocation[2])):
            _hold("native_allocation_unqualified")
        native_actor._uuid(allocation[3])
    if state == "started" and allocation[0] is None:
        _hold("native_allocation_unqualified")
    return job


class NativeSlurmScheduler(SlurmScheduler):
    """Slurm remains the queue/resource owner. No direct sbatch retry here."""
    def _request(self, action, calcjob_uuid):
        transport = self.transport
        if not isinstance(transport,NativeLocalTransport):
            _hold("native_transport_required")
        budget = transport._native_operation
        budget.before()
        launch = native_actor._load(calcjob_uuid,budget.reads.remaining,budget=budget.reads)[0]
        budget.before(8*65536)
        request = encode(dict(version=1,action=action,calcjob_uuid=calcjob_uuid,launch=launch,controls=None))
        if len(request)>65536:
            _hold("native_request_bound")
        status,out,_ = transport.exec_command_wait_bytes(shlex.join([_CLIENT,action]),stdin=request)
        budget.before(8*len(out))
        if status != 0:
            # Retain only the existing bounded public HOLD reason. Arbitrary
            # stderr/payload bytes are neither an authority nor a safe log.
            reason = "native_outcome_unavailable"
            try:
                failed = decode(out,maximum=_OUTPUT_BYTES)
                if (type(failed) is dict and
                        set(failed) == {"version","state","calcjob_uuid","receipt_ref","job","controls","reason"} and
                        type(failed["version"]) is int and failed["version"] == 1 and
                        failed["calcjob_uuid"] == calcjob_uuid and failed["state"] in ("hold","rejected") and
                        failed["controls"] is None and type(failed["reason"]) is str and
                        re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}",failed["reason"])):
                    reason += ": " + failed["reason"]
            except (ValueError,TypeError,KeyError):
                pass
            _hold(reason)
        return _reply(out,calcjob_uuid,state="started" if action == "start" else "bound")

    def submit_job(self, working_directory, filename):
        try:
            if filename != "_aiidasubmit.sh" or not isinstance(self.transport,NativeLocalTransport):
                _hold("native_submit_context")
            with self.transport, self.transport._operation() as budget:
                uuid = native_actor._lookup(self.transport.native_computer_uuid,str(working_directory),None,budget.reads)
                return self._request("submit",uuid)["job_id"]
        except PauseInterruption:
            raise
        except Exception as error:
            raise PauseInterruption("native attempt HOLD: actor_or_submit_unavailable") from error

    def get_jobs(self, jobs=None, user=None, as_dict=False):
        try:
            if (not isinstance(self.transport,NativeLocalTransport) or type(jobs) is not list or
                    len(jobs)>256 or user is not None or any(type(j) is not str or
                    re.fullmatch(r"[1-9][0-9]{0,9}",j) is None for j in jobs)):
                _hold("explicit_native_jobs_required")
            if not jobs:
                return {} if as_dict else []
            with self.transport, self.transport._operation() as budget:
                budget.before(8*_OUTPUT_BYTES)  # inherited Slurm UTF-8/JobInfo workspace
                result = super().get_jobs(jobs=jobs,user=None,as_dict=True)
                for identifier,info in result.items():
                    if info.job_state == JobState.RUNNING:
                        uuid = native_actor._lookup(self.transport.native_computer_uuid,None,identifier,budget.reads)
                        job = self._request("start",uuid)
                        if job["job_id"] != identifier:
                            _hold("native_job_changed")
                return result if as_dict else list(result.values())
        except PauseInterruption:
            raise
        except Exception as error:
            raise PauseInterruption("native attempt HOLD: status_unavailable") from error

    def kill_job(self, jobid):
        # Current P1 status can retain an old JobBinding. Until its owner adds a
        # fresh generation/cancel admission, never issue scancel by mutable ID.
        _hold("current_generation_cancel_unavailable")
