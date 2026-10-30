"""Ordinary Slurm/Local extensions; unavailable native gates remain HOLD.

No profile setup, scheduler replacement, retry worker or scientific acceptance.
The installed Platform client alone owns privileged dispatch/journaling. Its
current actor and Q1 prerequisites are deliberately not fabricated here.
"""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
import hashlib
import os
from pathlib import Path
import re
import selectors
import shlex
import signal
import stat
import subprocess
import time
from typing import ContextManager
from uuid import uuid4

import click
from jsonschema import ValidationError
from aiida.schedulers.plugins.slurm import SlurmScheduler
from aiida.schedulers.datastructures import JobState
from aiida.transports.plugins.local import LocalTransport
from plumpy.process_states import PauseInterruption
from qcl_negf_contracts.messages import decode, encode
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.outward import validate_outward_result

from . import native_actor

_CLIENT = "/run/current-system/sw/bin/qcl-negf-native-attempt-client"
_COMMAND_SECONDS = 15
_CLEANUP_SECONDS = 2
_OUTPUT_BYTES = 262144


def _hold(reason):
    raise PauseInterruption("native attempt HOLD: " + reason)


class _Operation:
    def __init__(self, maximum_metadata_bytes=8*1024*1024, maximum_io_bytes=8*1024*1024):
        native_actor._need(type(maximum_metadata_bytes) is int and maximum_metadata_bytes > 0 and
                           type(maximum_io_bytes) is int and maximum_io_bytes > 0, "adapter_budget")
        self.deadline = time.monotonic() + 90
        self.calls = 0
        self.files = 0
        self.io_remaining = maximum_io_bytes
        self.reads = native_actor._Reads(min(maximum_metadata_bytes,8*1024*1024),before=self._deadline)

    def _deadline(self):
        if time.monotonic() >= self.deadline:
            _hold("adapter_budget")

    def before(self, size=0):
        try:
            self.reads.charge(size)
        except native_actor.NativeActorUnavailable as error:
            raise PauseInterruption("native attempt HOLD: adapter_budget") from error

    def io(self, size):
        self.before()
        native_actor._need(type(size) is int and size >= 0, "adapter_io_budget")
        self.io_remaining -= size
        if self.io_remaining < 0:
            _hold("adapter_io_budget")

    def file(self):
        self.before()
        self.files += 1
        if self.files > 2048:
            _hold("adapter_file_budget")


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
        self._q1_budget = self._q1_packet = self._q1_failure = None
        super().__init__(*args, **kwargs)

    @contextmanager
    def _operation(self):
        outer = self._native_operation is None
        if outer:
            self._native_operation = self._q1_budget or _Operation()
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
        self._get_control(remotepath, localpath)

    def getfile(self, remotepath, localpath, *args, **kwargs):
        self._get_control(remotepath, localpath)

    def gettree(self, remotepath, localpath, *args, **kwargs):
        _hold("q1_controls_unavailable")

    def _get_control(self, remotepath, localpath):
        if not self.is_open or self._q1_failure is not None:
            _hold("q1_session_unavailable")
        remote = os.fspath(remotepath)
        native_actor._path(remote)
        leaf = Path(remote).name
        if leaf not in native_actor.CONTROL_FILES:
            _hold("q1_leaf_required")
        if self._q1_budget is None:
            self._q1_budget = self._native_operation or _Operation()
        try:
            with self._operation() as budget:
                if self._q1_packet is None:
                    budget.before(65536)
                    identifier = native_actor.native_calcjob_uuid(computer_uuid=self.native_computer_uuid,
                        remote_workdir=str(Path(remote).parent), maximum_metadata_bytes=65536)
                    self._q1_packet = _Packet(self,identifier,budget)
                packet = self._q1_packet
                if remote != packet.launch["remote_workdir"] + "/" + leaf:
                    _hold("q1_session_target_changed")
                packet.check()
                _deliver(localpath,packet.raw[leaf],budget)
                packet.check()
        except BaseException as error:
            self._q1_failure = error
            if isinstance(error,(OSError,ValueError,KeyError,ValidationError)):
                raise PauseInterruption("native attempt HOLD: q1_source_unavailable") from error
            raise

    def close(self):
        primary = None
        try:
            if self._q1_packet is not None:
                self._q1_packet.close()
        except BaseException as error:
            primary = error
        try:
            super().close()
        except BaseException as error:
            if primary is None:
                primary = error
            else:
                primary.add_note("transport close failed: " + type(error).__name__)
        finally:
            self._q1_budget = self._q1_packet = self._q1_failure = None
        if primary is not None:
            raise primary

    def __exit__(self, kind, value, traceback):
        try:
            return super().__exit__(kind,value,traceback)
        except BaseException as cleanup:
            if value is None:
                raise
            value.add_note("native close failed: " + type(cleanup).__name__)
            return False

    async def __aexit__(self, kind, value, traceback):
        try:
            return await super().__aexit__(kind,value,traceback)
        except BaseException as cleanup:
            if value is None:
                raise
            value.add_note("native close failed: " + type(cleanup).__name__)
            return False


def _reply(raw, calcjob_uuid, *, state, controls=None, job_id=None, budget=None):
    value = decode(raw,maximum=_OUTPUT_BYTES) if budget is None else _json(raw,_OUTPUT_BYTES,budget)
    if (type(value) is not dict or set(value) != {"version","state","calcjob_uuid","receipt_ref","job","controls","reason"}
            or type(value["version"]) is not int or value["version"] != 1
            or value["calcjob_uuid"] != calcjob_uuid or value["state"] != state
            or value["reason"] is not None or value["controls"] != controls
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
    if state in ("started","closed") and allocation[0] is None:
        _hold("native_allocation_unqualified")
    if state == "closed":
        _claims(controls)
        _claims(value["controls"])
        if value["receipt_ref"] != calcjob_uuid + "/close.json" or job["job_id"] != job_id:
            _hold("native_close_unqualified")
    return job


_CLAIM_LEAVES = ("series_result.json","native-selection.json","_scheduler-stdout.txt","_scheduler-stderr.txt")
_CEILINGS = (32*1024*1024,256*1024,65536,65536)


def _claims(value):
    if type(value) is not list or len(value) != 4:
        _hold("native_close_claims")
    for claim,name,cap in zip(value,_CLAIM_LEAVES,_CEILINGS):
        if (type(claim) is not dict or set(claim) != {"path","bytes","sha256"} or
                claim["path"] != name or type(claim["bytes"]) is not int or
                not 0 <= claim["bytes"] <= cap or type(claim["sha256"]) is not str or
                re.fullmatch(r"[0-9a-f]{64}",claim["sha256"]) is None):
            _hold("native_close_claims")


def _signature(info):
    return (info.st_dev,info.st_ino,info.st_uid,info.st_gid,info.st_mode,
            info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns)


def _absolute(path):
    value = os.fspath(path)
    native_actor._path(value)
    if "\\" in value or value == "/":
        _hold("q1_path")
    return value


def _relative(path, *, dot=False):
    if dot and path == ".":
        return ()
    if (type(path) is not str or not path or "\\" in path or
            any(ord(c)<32 or ord(c)==127 for c in path) or
            any(p in ("",".","..") for p in path.split("/"))):
        _hold("q1_relative_path")
    return tuple(path.split("/"))


class _Tree:
    """Finite no-follow directory handles; a path/FD alone grants no access."""
    def __init__(self, root, budget):
        self.root,self.budget,self.ancestors,self.fd = _absolute(root),budget,[],None
        self.bindings = {}
        try:
            budget.file();budget.before(512)
            self.fd = os.open("/",os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            for part in Path(self.root).parts[1:]:
                budget.file();budget.before(512)
                child = os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=self.fd)
                self.ancestors.append((self.fd,part,_signature(os.fstat(child))))
                self.fd = child
            info = os.fstat(self.fd)
            if info.st_uid != os.geteuid() or info.st_mode & 0o022:
                _hold("q1_directory_owner")
            self.identity = _signature(info)[:6]
            self.check()
        except BaseException as primary:
            try:
                self.close()
            except BaseException as cleanup:
                primary.add_note("source FD cleanup failed: "+type(cleanup).__name__)
            raise

    def check(self):
        self.budget.before()
        if _signature(os.fstat(self.fd))[:6] != self.identity:
            _hold("q1_directory_changed")
        # Other entries may change in /tmp or an ancestor. The selected named
        # directory identity/credentials/mode must remain the very same inode.
        for parent,name,before in self.ancestors:
            self.budget.file()
            if _signature(os.stat(name,dir_fd=parent,follow_symlinks=False))[:5] != before[:5]:
                _hold("q1_ancestry_changed")
        for name,before in self.bindings.items():
            self.budget.file()
            if _signature(os.stat(name,dir_fd=self.fd,follow_symlinks=False)) != before:
                _hold("q1_file_changed")

    def read(self,name,cap,expected=None):
        _relative(name)
        if "/" in name:
            _hold("q1_leaf_required")
        self.check();self.budget.file();self.budget.before(1024)
        fd = os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=self.fd)
        try:
            before = os.fstat(fd); size = before.st_size
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or
                    before.st_uid != os.geteuid() or before.st_mode & 0o022 or not 0<=size<=cap):
                _hold("q1_file_identity")
            self.budget.before(8*(size+1));self.budget.io(size+1)
            raw = bytearray()
            while len(raw) <= size:
                self.budget.before()
                chunk = os.read(fd,min(65536,size+1-len(raw)))
                if not chunk:
                    break
                raw.extend(chunk)
            self.budget.file()
            if (len(raw)!=size or _signature(os.fstat(fd))!=_signature(before) or
                    _signature(os.stat(name,dir_fd=self.fd,follow_symlinks=False))!=_signature(before)):
                _hold("q1_file_changed")
            raw = bytes(raw)
            if expected is not None and raw != expected:
                _hold("q1_file_changed")
            self.budget.before(512+2*len(name))
            self.bindings[name] = _signature(before)
            self.check()
            return raw
        finally:
            os.close(fd)

    def close(self):
        failure = None
        descriptors = ([self.fd] if self.fd is not None else []) + [p for p,_,_ in self.ancestors]
        self.fd,self.ancestors = None,[]
        for fd in descriptors:
            try:
                os.close(fd)
            except BaseException as error:
                if failure is None:
                    failure = error
        if failure is not None:
            raise failure


def _proc_bytes(path,budget,cap=65536):
    budget.file();raw=bytearray()
    with open(path,"rb") as stream:
        while len(raw)<=cap:
            count=min(4096,cap+1-len(raw));budget.before(8*count);budget.io(count)
            chunk=stream.read(count)
            if not chunk:
                break
            raw.extend(chunk)
    if len(raw)>cap:
        _hold("q1_namespace_bound")
    return bytes(raw)


def _namespace(root,budget):
    """Actual local RO NFS view. Root still qualifies the finite writer domain."""
    credentials=(os.getresuid(),os.getresgid(),tuple(os.getgroups()))
    if (0 in credentials[0] or 0 in credentials[1] or 0 in credentials[2] or
            len(set(credentials[0]))!=1 or len(set(credentials[1]))!=1):
        _hold("q1_owner_credentials")
    status=_proc_bytes("/proc/self/status",budget).decode().splitlines()
    ids=[]
    for key in ("Uid:","Gid:"):
        rows=[line.split()[1:] for line in status if line.startswith(key)]
        if len(rows)!=1 or len(rows[0])!=4 or not all(v.isdecimal() for v in rows[0]):
            _hold("q1_owner_credentials")
        values=tuple(map(int,rows[0]))
        if len(set(values))!=1 or (key=="Uid:" and values[0]!=os.geteuid()) or (key=="Gid:" and values[0]!=os.getegid()):
            _hold("q1_owner_credentials")
        ids.append(values)
    budget.file(); namespace=os.stat("/proc/self/ns/mnt")
    boot=_proc_bytes("/proc/sys/kernel/random/boot_id",budget,64)
    native_actor._uuid(boot.decode().rstrip("\n"))
    mounts=_proc_bytes("/proc/self/mountinfo",budget).decode().splitlines(); selected=[]
    for line in mounts:
        left,right=line.split(" - ",1);fields=left.split();tail=right.split()
        path=re.sub(r"\\([0-7]{3})",lambda m:chr(int(m[1],8)),fields[4])
        # A directory FD does not constrain mounts below it. Until a nested
        # domain is independently qualified, even a RO/same-export boundary
        # cannot inherit this root's storage and lifetime qualification.
        if path.startswith(root.rstrip("/")+"/"):
            _hold("q1_nested_namespace_unqualified")
        if root == path or root.startswith(path.rstrip("/")+"/"):
            selected.append((len(path),fields,tail))
    if not selected:
        _hold("q1_ro_namespace_unavailable")
    _,fields,tail=max(selected,key=lambda row:row[0])
    if "ro" not in fields[5].split(",") or tail[0] not in ("nfs","nfs4"):
        _hold("q1_ro_namespace_unavailable")
    return credentials,tuple(ids),(namespace.st_dev,namespace.st_ino),boot,tuple(fields),tuple(tail)


def _json(raw,cap,budget):
    # Public validators copy/encode detached JSON. Reserve their conservative
    # string/object workspace before decode, never after a successful parse.
    cost=64*len(raw)+512*(1+sum(raw.count(c) for c in (b"{",b"}",b"[",b"]",b",",b":")))
    budget.before(cost)
    return decode(raw,maximum=cap)


class _Packet:
    def __init__(self,transport,identifier,budget):
        self.transport,self.identifier,self.budget=transport,identifier,budget
        self.trees=[];self.raw={};self.closed=False
        try:
            # Prepay the initial public lookup cap in full. After observing its
            # actual four FILES, use the existing P2 actor envelope for the two
            # remaining calls. Nothing is refunded or charged after a query.
            first_cap=budget.reads.remaining//4
            budget.before(first_cap)
            self.launch=native_actor.load_native_launch(identifier,maximum_metadata_bytes=first_cap)
            counts=[row["bytes"] for row in self.launch["files"].values()]
            if len(counts)!=4 or any(type(n) is not int or not 0<=n<=65536 for n in counts):
                _hold("q1_actor_input_bound")
            self.actor_cap=30*16384+8*sum(min(65537,4096*((n+4095)//4096)+4096) for n in counts)
            budget.before(2*self.actor_cap)
            original=native_actor.read_native_execution(identifier,maximum_metadata_bytes=self.actor_cap)
            claim=self.launch["files"]["native_execution"]
            if len(original)!=claim["bytes"] or hashlib.sha256(original).hexdigest()!=claim["sha256"]:
                _hold("q1_actor_changed")
            self.native=_json(original,65536,budget)
            budget.before(16384)
            calc=native_actor._orm().load_node(identifier)
            self.job_id=calc.get_job_id()
            if (calc.computer.uuid!=transport.native_computer_uuid or type(self.job_id) is not str or
                    not re.fullmatch(r"[1-9][0-9]{0,9}",self.job_id)):
                _hold("q1_actor_job")
            jobs=_absolute(os.environ.get("QCL_NEGF_NATIVE_JOBS_ROOT",""))
            relative=self.launch["relative_execution_root"]
            _relative(relative)
            if self.native["storage"]["relative_execution_root"] != relative:
                _hold("q1_actor_root")
            self.root=jobs+"/"+relative;work=self.launch["remote_workdir"]
            if (not work.startswith(jobs+"/") or self.root==work or
                    self.root.startswith(work+"/") or work.startswith(self.root+"/")):
                _hold("q1_root_alias")
            self.namespace=_namespace(self.root,budget)
            self.namespaces={self.root:self.namespace}
            result=_Tree(self.root,budget);self.trees.append(result)
            upload=_Tree(work,budget);self.trees.append(upload)
            controls=[]
            for name,cap in zip(_CLAIM_LEAVES,_CEILINGS):
                tree=result if name in _CLAIM_LEAVES[:2] else upload
                raw=tree.read(name,cap);self.raw[name]=raw
                controls.append(dict(path=name,bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
            _claims(controls)
            budget.before(8*65536)
            request=encode(dict(version=1,action="close",calcjob_uuid=identifier,launch=self.launch,controls=controls))
            if len(request)>65536:
                _hold("native_request_bound")
            budget.io(len(request)+65537)
            self.check()
            status,out,err=transport.exec_command_wait_bytes(shlex.join([_CLIENT,"close"]),stdin=request)
            budget.before(8*len(out))
            if (status!=0 or err or not out.endswith(b"\n") or out.endswith(b"\n\n") or len(out)>65537):
                _hold("native_close_outcome_unknown")
            projection=out[:-1]
            # The fixed client owns canonical encoding. Never reencode the
            # projection here or accept a file/caller dict instead of this call.
            if b"\n" in projection or not projection.startswith(b"{") or not projection.endswith(b"}"):
                _hold("native_close_framing")
            _reply(projection,identifier,state="closed",controls=controls,job_id=self.job_id,budget=budget)
            self.check()
            for name,cap in zip(_CLAIM_LEAVES,_CEILINGS):
                (result if name in _CLAIM_LEAVES[:2] else upload).read(name,cap,self.raw[name])
            self.raw["native-close.json"]=projection
            self.check();self.closed=True
        except BaseException as primary:
            self._release(primary)
            raise

    def check(self):
        self.budget.before()
        for root,namespace in self.namespaces.items():
            if _namespace(root,self.budget)!=namespace:
                _hold("q1_namespace_changed")
        for tree in self.trees:
            tree.check()
        # Launch12 deliberately omits the scheduler job selector. Query the
        # original CalcJob afresh, with its full metadata allowance prepaid,
        # rather than accepting a newly stable job from a later actor lookup.
        self.budget.before(16384)
        orm=native_actor._orm();calc=orm.load_node(self.identifier)
        if (not isinstance(calc,orm.CalcJobNode) or not calc.is_stored or
                calc.computer.uuid!=self.transport.native_computer_uuid or
                calc.get_job_id()!=self.job_id or
                calc.get_remote_workdir()!=self.launch["remote_workdir"]):
            _hold("q1_actor_job_changed")

    def _release(self,primary=None):
        failure=primary
        for tree in reversed(self.trees):
            try:
                tree.close()
            except BaseException as error:
                if failure is None:
                    failure=error
                else:
                    failure.add_note("source FD cleanup failed: "+type(error).__name__)
        self.trees=[];self.raw.clear()
        if failure is not None and primary is None:
            raise failure

    def close(self):
        primary=None
        try:
            self.check()
            if native_actor.load_native_launch(self.identifier,maximum_metadata_bytes=self.actor_cap)!=self.launch:
                _hold("q1_actor_changed")
            self.check()
        except BaseException as error:
            primary=error
        self._release(primary)
        if primary is not None:
            raise primary


def _deliver(localpath,raw,budget):
    path=_absolute(localpath);directory=_Tree(str(Path(path).parent),budget)
    name=Path(path).name;temporary=".native-get-"+uuid4().hex;fd=None;created=False;primary=None
    try:
        budget.before(8*(len(raw)+1));budget.io(len(raw))
        budget.file();fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory.fd);created=True
        offset=0
        while offset<len(raw):
            budget.before();count=os.write(fd,raw[offset:offset+65536])
            if count<=0:
                _hold("q1_short_write")
            offset+=count
        os.fsync(fd);os.close(fd);fd=None
        budget.file()
        try:
            os.link(temporary,name,src_dir_fd=directory.fd,dst_dir_fd=directory.fd,follow_symlinks=False)
        except FileExistsError:
            directory.read(name,len(raw),raw)
        os.unlink(temporary,dir_fd=directory.fd);created=False
        os.fsync(directory.fd);directory.read(name,len(raw),raw);directory.check()
    except BaseException as error:
        primary=error
    finally:
        for action in ((lambda:os.close(fd)) if fd is not None else None,
                       (lambda:os.unlink(temporary,dir_fd=directory.fd)) if created else None,directory.close):
            if action is not None:
                try:
                    action()
                except BaseException as error:
                    if primary is None:
                        primary=error
                    else:
                        primary.add_note("destination cleanup failed: "+type(error).__name__)
    if primary is not None:
        raise primary


def _selected(packet):
    budget=packet.budget
    budget.before(16*(len(packet.raw["series_result.json"])+len(packet.raw["native-selection.json"])))
    result=validate_outward_result(_json(packet.raw["series_result.json"],32*1024*1024,budget))
    if result["schema"]!="qcl-negf-series-result-v4" or result["contract_set"]!="qcl-negf.results.v2" or result["status"] not in ("completed","completed_with_warnings","failed","cancelled","paused"):
        _hold("q1_result_format")
    if (result.get("selected_execution_id",packet.launch["execution_id"]) != packet.launch["execution_id"] or
            any(row["execution_id"]!=packet.launch["execution_id"] for row in (*result["points"],*result.get("attempt_history",[])))):
        _hold("q1_result_execution")
    selection=_json(packet.raw["native-selection.json"],256*1024,budget)
    if (type(selection) is not dict or set(selection)!={"version","selected_inputs"} or
            type(selection["version"]) is not int or selection["version"]!=1 or type(selection["selected_inputs"]) is not list):
        _hold("q1_selection_format")
    common=schema_validator("result-common.schema.json")
    for row in selection["selected_inputs"]:
        if type(row) is not dict or set(row)!={"artifact_root","selected_state","plan","model"}:
            _hold("q1_selection_format")
        _relative(row["artifact_root"],dot=True)
        for key,schema in (("selected_state","StateRef"),("plan","FileRef"),("model","FileRef")):
            common.evolve(schema={"$ref":"#/$defs/"+schema,"$defs":common.schema["$defs"]}).validate(row[key])
    ordered=[];sources=[]
    for point in (*result["points"],*result.get("attempt_history",[])):
        source=point["data"]["native_source"]
        if source["kind"]=="absent":
            continue
        same=lambda row:row["artifact_root"]==source["artifact_root"] and row["selected_state"]==source["selected_state"]
        matches=[row for row in selection["selected_inputs"] if same(row)]
        if len(matches)!=1:
            _hold("q1_selection_coverage")
        indices=[i for i,row in enumerate(ordered) if same(row)]
        if not indices:
            ordered.append(matches[0]);sources.append(source)
        elif source["kind"]=="final":
            old=sources[indices[0]]
            if old["kind"]=="final" and any(old[k]!=source[k] for k in ("diagnostics","observations")):
                _hold("q1_selection_final_refs")
            sources[indices[0]]=source
    if ordered!=selection["selected_inputs"]:
        _hold("q1_selection_order")
    output=[]
    for row,source in zip(ordered,sources):
        parts=_relative(row["artifact_root"],dot=True)
        root=packet.root+("/"+"/".join(parts) if parts else "")
        namespace=_namespace(root,budget)
        if namespace!=packet.namespace:
            _hold("q1_source_namespace_domain")
        budget.before(8*(len(root)+1))
        packet.namespaces[root]=namespace
        tree=_Tree(root,budget);packet.trees.append(tree)
        output.append((Path(root),deepcopy(source),deepcopy(row["plan"]),deepcopy(row["model"])))
    return tuple(output)


def open_native_selected_sources(calcjob_uuid: str, *, maximum_metadata_bytes: int,
                                 maximum_io_bytes: int) -> ContextManager[tuple[
                                     bytes,tuple[tuple[Path,dict,dict,dict],...],None]]:
    """Current qualified native bytes; no origin/scientific verdict or lease.

    The loaded-profile owner must close its public Results readers and finish
    this context successfully before registering any output. Paths must not
    escape this lifetime. Retained original creator proof remains the B owner.
    """
    native_actor._uuid(calcjob_uuid)
    budget=_Operation(maximum_metadata_bytes,maximum_io_bytes)
    return _selected_context(calcjob_uuid,budget)


@contextmanager
def _selected_context(calcjob_uuid,budget):
    budget.before(16384)
    orm=native_actor._orm();calc=orm.load_node(calcjob_uuid)
    if not isinstance(calc,orm.CalcJobNode) or not calc.is_stored:
        _hold("q1_calcjob_required")
    budget.before(16384)
    transport=calc.get_authinfo().get_transport()
    if not isinstance(transport,NativeLocalTransport) or transport.native_computer_uuid!=calc.computer.uuid:
        _hold("native_transport_required")
    packet=None;primary=None;yielded=False
    try:
        with transport:
            if transport._native_operation is not None or transport._q1_budget is not None:
                _hold("q1_operation_in_use")
            transport._native_operation=budget
            try:
                packet=_Packet(transport,calcjob_uuid,budget)
                budget.io(sum(len(raw)+1 for raw in packet.raw.values()))
                native_actor._retrieved_packet(calcjob_uuid,packet.raw,budget.reads)
                sources=_selected(packet)
                packet.check()
                yielded=True
                yield packet.raw["series_result.json"],sources,None
            except BaseException as error:
                primary=error
                raise
            finally:
                try:
                    if packet is not None:
                        packet.close()
                except BaseException as error:
                    if primary is None:
                        raise
                    primary.add_note("native source close failed: "+type(error).__name__)
                finally:
                    transport._native_operation=None
    except (OSError,ValueError,KeyError,ValidationError) as error:
        if yielded and error is primary:
            raise
        raise PauseInterruption("native attempt HOLD: q1_source_unavailable") from error


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
