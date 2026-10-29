"""Native counters are not CalcJob attempt IDs; only retained science proves owner."""
from __future__ import annotations

from copy import deepcopy
from functools import cache
import json
import math
import re
import struct

from jsonschema import ValidationError
from qcl_negf_contracts import schema_validator
from qcl_negf_contracts.messages import ContractError, encode, scientific_plan
from qcl_negf_contracts.outward import validate_outward_result, validate_outward_native_source

import hashlib

from qcl_negf_contracts.artifacts import relative_path
from qcl_negf_contracts.messages import decode


def validate_native_collisions(points, commits, *, input_attempt, execution_id,
                               plan_fingerprint, calcjob_uuid, scientific_fingerprint=None,
                               result_scientific_fingerprint=None):
    """Narrow greater-than-input exception after existing artifact verification."""
    for row in points:
        native = row["attempt"]
        if type(native) is not int or native < 1:
            raise ValueError("invalid native point attempt")
        if native <= input_attempt:
            continue
        if scientific_fingerprint is None or result_scientific_fingerprint != scientific_fingerprint:
            raise ValueError("native collision scientific fingerprint differs")
        data = row.get("data") or {}
        if (data.get("checkpoint_source_attempt") is not None or not data.get("result_commit")
                or row["execution_id"] != execution_id
                or data.get("artifact_root") != f"executions/{execution_id}/{row['id']}/attempt-{native}"):
            raise ValueError("unproven native attempt collision")
        pin = commits[row["id"]]
        identity = pin["identity"]
        expected = {"point_id": row["id"], "execution_id": execution_id,
                    "attempt": native, "plan_fingerprint": plan_fingerprint}
        if (relative_path(data["result_commit"]) != pin["path"]
                or any(type(identity.get(k)) is not type(v) or identity[k] != v for k, v in expected.items())
                or pin.get("receipt_status") != "verified"
                or not isinstance(pin.get("state_id"), str) or not pin["state_id"]
                or type(pin.get("state_sequence")) is not int or pin["state_sequence"] < 1
                or identity.get("state_id") != pin["state_id"] or identity.get("state_sequence") != pin["state_sequence"]
                or ("calcjob_uuid" in identity and identity["calcjob_uuid"] != calcjob_uuid)):
            raise ValueError("native collision lacks exact verified state identity")
        if row.get("status") in ("completed", "completed_with_warnings") and pin["path"] != f"archive/{execution_id}/{row['id']}/final/commit.json":
            raise ValueError("native collision final is not canonical")


def source_point_owner(child, source_attempt, plan_uuid, *, point_id, commit_sha256,
                       identity, proofs=None, budget=None, scientific_fingerprint=None):
    """Current/direct recovery creators only, cached bounded repository metadata.

    Missing/ambiguous relations stay unknown. Observer data never enters proof.
    Callers may seed current proof with their already verified raw result+pin.
    """
    from . import saved_sources as saved
    proofs = {} if proofs is None else proofs
    budget = saved._Budget(8 * saved.MAX_METADATA) if budget is None else budget

    def proof(candidate):
        if candidate.uuid in proofs:
            return proofs[candidate.uuid]
        try:
            if candidate.inputs.plan.uuid != plan_uuid:
                return None
            result, selection = candidate.outputs.result, candidate.outputs.selection
            if result.creator.uuid != candidate.uuid or selection.creator.uuid != candidate.uuid:
                return None
            pin = selection.get_dict()
            budget.charge(len(saved._canonical(pin)))
            raw = saved._node_bytes(result, saved.MAX_METADATA, budget)
            if (pin["calcjob_uuid"] != candidate.uuid or pin["attempt"] != candidate.inputs.attempt.value
                    or pin["execution_id"] != candidate.inputs.execution_id.value
                    or pin["result_sha256"] != hashlib.sha256(raw).hexdigest()):
                return None
            value = decode(raw, maximum=saved.MAX_METADATA)
            proofs[candidate.uuid] = (value, pin)
            return value, pin
        except (AttributeError, KeyError, ValueError, OSError):
            proofs[candidate.uuid] = None
            return None

    def owns(candidate):
        try:
            if candidate.inputs.plan.uuid != plan_uuid or candidate.inputs.execution_id.value != identity["execution_id"]:
                return False
            pair = proof(candidate)
            if pair is None:
                return False
            series, pin = pair
            rows = [r for r in series["points"] if r["id"] == point_id and r["execution_id"] == identity["execution_id"]]
            if len(rows) != 1 or series["plan_fingerprint"] != identity["plan_fingerprint"]:
                return False
            row = rows[0]; native = pin["commits"][point_id]
            if (row["attempt"] != source_attempt or (row.get("data") or {}).get("checkpoint_source_attempt") is not None
                    or (row.get("data") or {}).get("result_commit") != native["path"]
                    or native["commit_sha256"] != commit_sha256 or native["identity"] != identity
                    or source_attempt < candidate.inputs.attempt.value):
                return False
            if "calcjob_uuid" in identity and identity["calcjob_uuid"] != candidate.uuid:
                return False
            validate_native_collisions([row], pin["commits"], input_attempt=candidate.inputs.attempt.value,
                execution_id=identity["execution_id"], plan_fingerprint=identity["plan_fingerprint"], calcjob_uuid=candidate.uuid,
                scientific_fingerprint=scientific_fingerprint,result_scientific_fingerprint=series.get("plan_scientific_fingerprint"))
            return True
        except (AttributeError, KeyError, ValueError, OSError):
            return False

    candidates = {child.uuid: child}
    for name in ("recovery_bundle", "prior_archive"):
        try:
            prior = getattr(child.inputs, name).creator
            candidates[prior.uuid] = prior
        except (AttributeError, KeyError):
            pass
    owners = {candidate.uuid for candidate in candidates.values() if owns(candidate)}
    return next(iter(owners)) if len(owners) == 1 else None


# Native v5 association is a pure declaration join. It never calls the legacy
# ORM-backed source_point_owner above or grants permission to construct a node.


_PLAN_PROCESS = 'aiida.workflows:qcl_negf.plan'
_RESTART_PROCESS = 'aiida.workflows:qcl_negf.execution_restart'
_CALC_PROCESS = 'aiida.calculations:qcl_negf.execution'
_UUID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
_SELECTION = 'run_uuid execution_id attempt calcjob_uuid'
_STORAGE = ('namespace_id storage_identity relative_execution_root computer_uuid '
            'execution_binding_uuid retention_policy_id retention_owner_id hold_ref access_binding_ref')
_CODE = 'code_uuid computer_uuid solver_executable release_id build_receipt_ref'
_CONTEXT = ('row_key source manifest manifest_ref checkpoint diagnostics observations plan model '
            'parent_manifest parent_checkpoint')
_PROOF = ('row_key mode creator_calcjob_uuid origin_row_key origin_result_uuid origin_binding_uuid '
          'processes codes files links hops')
_DRAFT = 'selection input_plan source_graph storage raw_result points attempt_history'
_BINDING_ROW = 'point_id point_attempt source source_invocation creator plan_point_id coordinates'
_INTEGRITY = dict(scope='selected_metadata', native_contexts='verified', transport_owner='verified',
                  full_core_payloads='not_assessed', current_access='not_checked')


def _binding_require(condition, message, code='corrupt_result'):
    if not condition:
        raise ContractError('native binding: ' + message, code)


def _binding_keys(value, names):
    _binding_require(type(value) is dict and set(value) == set(names.split()), 'closed object keys differ', 'invalid_input')


def _binding_uuid(value):
    _binding_require(type(value) is str and _UUID.fullmatch(value) is not None, 'canonical UUID required', 'invalid_input')


def _binding_positive(value):
    _binding_require(type(value) is int and 1 <= value <= 2**64-1, 'exact positive integer required', 'invalid_input')


@cache
def _binding_validator(resource, fragment=''):
    validator = schema_validator(resource)
    return validator.evolve(schema={'$ref': 'https://qcl-negf.invalid/schema/' + resource + fragment})


def _binding_schema(value, resource, fragment=''):
    try:
        _binding_validator(resource, fragment).validate(value)
    except ValidationError as error:
        raise ContractError('native binding: ' + error.message, 'invalid_input') from error


def _binding_atom(value, name):
    _binding_schema(value, 'result-common.schema.json', '#/$defs/' + name)


def _binding_opaque(value):
    _binding_require(type(value) is str and 1 <= len(value) <= 256 and not value.startswith('/')
                     and all(ord(c) >= 32 and ord(c) != 127 for c in value), 'opaque reference required', 'invalid_input')


def _binding_selection(value):
    _binding_keys(value, _SELECTION)
    _binding_uuid(value['run_uuid']); _binding_uuid(value['calcjob_uuid'])
    _binding_atom(value['execution_id'], 'Id'); _binding_positive(value['attempt'])


def _binding_node_file(value):
    _binding_keys(value, 'node_uuid bytes sha256')
    _binding_uuid(value['node_uuid'])
    _binding_atom(value['bytes'], 'Count'); _binding_atom(value['sha256'], 'Sha256')


def _binding_raw_file(value, raw):
    _binding_keys(value, 'bytes sha256')
    _binding_atom(value['bytes'], 'Count'); _binding_atom(value['sha256'], 'Sha256')
    _binding_require(value == dict(bytes=str(len(raw)), sha256=hashlib.sha256(raw).hexdigest()), 'original byte identity differs')


def _binding_storage(value):
    _binding_keys(value, _STORAGE)
    _binding_atom(value['namespace_id'], 'Id')
    for name in ('computer_uuid', 'execution_binding_uuid'):
        _binding_uuid(value[name])
    relative_path(value['relative_execution_root'])
    for name in ('storage_identity', 'retention_policy_id', 'retention_owner_id', 'hold_ref', 'access_binding_ref'):
        _binding_opaque(value[name])


def _binding_code(value):
    _binding_keys(value, _CODE)
    _binding_uuid(value['code_uuid']); _binding_uuid(value['computer_uuid'])
    _binding_atom(value['release_id'], 'Id'); _binding_opaque(value['build_receipt_ref'])
    _binding_require(type(value['solver_executable']) is str and re.fullmatch(
        r'/nix/store/[A-Za-z0-9][A-Za-z0-9+._?-]*/bin/qcl-negf', value['solver_executable']) is not None,
        'declared solver path differs', 'invalid_input')


def _binding_row_key(value):
    _binding_keys(value, 'execution_id point_id point_attempt')
    _binding_atom(value['execution_id'], 'Id'); _binding_atom(value['point_id'], 'Id')
    _binding_positive(value['point_attempt'])
    return value['execution_id'], value['point_id'], value['point_attempt']


def _binding_admit(args):
    """Count the exact S12 projection before allocating copies or decoding bytes.

    Sorting object keys does not change encoded length. Strings are counted one
    codepoint at a time, including JSON escaping and strict UTF-8 admission.
    Shared objects count again; cycles refuse. This is not an RSS/IO guarantee.
    """
    maximum = args['maximum_metadata_bytes']
    _binding_positive(maximum)
    for key, limit in (('raw_result', 33554432), ('frozen_plan', 16777216)):
        _binding_require(type(args[key]) is bytes and len(args[key]) <= limit, key + ' byte limit/type', 'invalid_input')
    remaining = maximum
    active = set()

    def charge(n):
        nonlocal remaining
        remaining -= n
        _binding_require(remaining >= 0, 'metadata allowance exceeded', 'invalid_input')

    def string_size(value):
        charge(2)
        for c in value:
            n = ord(c)
            _binding_require(not 0xd800 <= n <= 0xdfff, 'invalid UTF-8 string', 'invalid_input')
            charge(2 if c in '\\"\b\f\n\r\t' else 6 if n < 32 else
                   1 if n < 128 else 2 if n < 2048 else 3 if n < 65536 else 4)

    def visit(value, path, depth):
        _binding_require(depth <= 64, 'metadata depth exceeds 64', 'invalid_input')
        kind = type(value)
        if kind is bytes:
            allowed = path in (('raw_result',), ('frozen_plan',), ('source_graph', 'raw')) or (
                len(path) == 5 and path[0] == 'creator_proofs' and type(path[1]) is int
                and path[2] == 'files' and type(path[3]) is int and path[4] == 'raw')
            _binding_require(allowed, 'bytes outside declared raw slot', 'invalid_input')
            charge(len(value) + 4)  # bytes + JSON null in metadata projection
        elif kind is str:
            string_size(value)
        elif value is None:
            charge(4)
        elif kind is bool:
            charge(4 if value else 5)
        elif kind is int:
            try:
                size = len(str(value))
            except ValueError as error:
                raise ContractError('native binding: integer encoding', 'invalid_input') from error
            charge(size)
        elif kind is float:
            _binding_require(math.isfinite(value), 'nonfinite metadata', 'invalid_input')
            charge(len(json.dumps(value, allow_nan=False)))
        elif kind in (dict, list, tuple):
            _binding_require(kind is not tuple or path in (('source_contexts',), ('creator_proofs',)),
                             'tuple outside declared argument', 'invalid_input')
            _binding_require(id(value) not in active, 'metadata cycle', 'invalid_input')
            active.add(id(value))
            charge(2 + max(0, len(value)-1))
            if kind is dict:
                for key, item in value.items():
                    _binding_require(type(key) is str, 'string keys required', 'invalid_input')
                    string_size(key); charge(1); visit(item, path+(key,), depth+1)
            else:
                for index, item in enumerate(value):
                    visit(item, path+(index,), depth+1)
            active.remove(id(value))
        else:
            raise ContractError('native binding: unsupported metadata type', 'invalid_input')
    _binding_require(type(args['source_contexts']) is tuple and type(args['creator_proofs']) is tuple,
                     'exact context/proof tuples required', 'invalid_input')
    visit(args, (), 0)


def _binding_same(a, b):
    return encode(a) == encode(b)


def _binding_rows(result, selection, plan):
    _binding_require(result.get('schema') == 'qcl-negf-series-result-v4', 'outward series v4 required', 'incompatible_contract')
    validate_outward_result(result)
    executions = [e for e in plan['executions'] if e['id'] == selection['execution_id']]
    _binding_require(len(executions) == 1, 'frozen execution absent/ambiguous')
    execution = executions[0]
    _binding_require(len(result['executions']) == 1 and result['executions'][0]['id'] == selection['execution_id']
                     and result['executions'][0]['point_ids'] == execution['point_ids'], 'execution inventory differs')
    _binding_require(result['plan_fingerprint'] == plan['fingerprint'] and
                     result['plan_scientific_fingerprint'] == plan['scientific_fingerprint'], 'plan fingerprint differs')
    points = {p['id']: p for p in plan['points'] if p['execution_id'] == execution['id']}
    _binding_require(set(points) == set(execution['point_ids']), 'frozen point inventory differs')
    rows = {}
    for row in result['points'] + result['attempt_history']:
        _binding_require(row['execution_id'] == selection['execution_id'] and row['id'] in points, 'foreign result row')
        point = points[row['id']]
        for name in ('branch', 'order'):
            _binding_require(type(row['coordinates'][name]) is type(point[name]) and row['coordinates'][name] == point[name], 'plan coordinate differs')
        for name in ('temperature_K', 'voltage_per_period_V'):
            try:
                same = struct.pack('>d', row['coordinates'][name]) == struct.pack('>d', point[name])
            except (OverflowError, struct.error) as error:
                raise ContractError('native binding: binary64 coordinate', 'corrupt_result') from error
            _binding_require(same, 'plan binary64 coordinate differs')
        key = (row['execution_id'], row['id'], row['attempt'])
        _binding_require(key not in rows or _binding_same(rows[key], row), 'duplicate row differs')
        rows[key] = row
    return rows, points


def _binding_stored(value, raw, selection, plan, execution):
    """Structural read operand. Its verified words have no authority here."""
    _binding_keys(value, 'schema ' + _DRAFT + ' integrity')
    _binding_require(value['schema'] == 'qcl-negf.aiida-native-binding.v1', 'stored binding version')
    _binding_selection(value['selection'])
    _binding_require(value['selection'] == selection, 'stored selection differs')
    _binding_keys(value['integrity'], ' '.join(_INTEGRITY))
    _binding_require(value['integrity'] == _INTEGRITY, 'stored integrity marker shape differs')
    for name in ('input_plan', 'source_graph'):
        _binding_node_file(value[name])
        _binding_require(value[name] == execution[name], 'stored original node identity differs')
    _binding_storage(value['storage'])
    _binding_require(value['storage'] == execution['storage'], 'stored storage declaration differs')
    _binding_raw_file(value['raw_result'], raw)
    original = decode(raw, maximum=33554432)
    rows, _ = _binding_rows(original, selection, plan)
    projected = {}
    for name in ('points', 'attempt_history'):
        _binding_require(type(value[name]) is list and len(value[name]) == len(original[name]), 'stored row inventory differs')
        for row, actual in zip(value[name], original[name]):
            _binding_keys(row, _BINDING_ROW)
            key = (selection['execution_id'], row['point_id'], row['point_attempt'])
            _binding_positive(row['point_attempt'])
            _binding_require(row['point_id'] == row['plan_point_id'] == actual['id'] and
                             row['point_attempt'] == actual['attempt'] and _binding_same(row['coordinates'], actual['coordinates']) and
                             _binding_same(row['source'], actual['data']['native_source']), 'stored source projection differs')
            if row['source']['kind'] == 'absent':
                _binding_require(row['creator'] is row['source_invocation'] is None, 'absent stored owner')
            else:
                _binding_keys(row['creator'], 'calcjob_uuid binding_uuid')
                _binding_uuid(row['creator']['calcjob_uuid'])
                if row['creator']['binding_uuid'] is not None:
                    _binding_uuid(row['creator']['binding_uuid'])
                _binding_require((row['creator']['calcjob_uuid'] == selection['calcjob_uuid']) ==
                                 (row['creator']['binding_uuid'] is None), 'stored creator reference differs')
                _binding_schema(row['source_invocation'], 'result-common.schema.json', '#/$defs/Invocation')
            _binding_require(key not in projected or _binding_same(projected[key], row), 'stored duplicate row differs')
            projected[key] = row
    _binding_require(set(projected) == set(rows), 'stored row keys differ')
    return projected


def _binding_source_matches(target, origin):
    return (origin['kind'] != 'absent' and target['artifact_root'] == origin['artifact_root'] and
            target['selected_state'] == origin['selected_state'] and
            (target['kind'] == 'checkpoint' or origin['kind'] == 'final' and
             target['diagnostics'] == origin['diagnostics'] and target['observations'] == origin['observations']))


def _binding_creator(proof, *, selection, execution, plan, graph, context, global_claims):
    """Join a closed, caller-declared typed graph; never query ORM or storage."""
    _binding_keys(proof, _PROOF)
    row_key = _binding_row_key(proof['row_key']); origin_key = _binding_row_key(proof['origin_row_key'])
    _binding_uuid(proof['creator_calcjob_uuid'])
    _binding_require(proof['mode'] in ('current', 'retained'), 'creator mode')
    for name in ('processes', 'codes', 'files', 'links', 'hops'):
        _binding_require(type(proof[name]) is list, 'proof arrays required', 'invalid_input')
    _binding_require(proof['processes'] and proof['codes'] and proof['links'], 'empty proof')
    claims = {}; processes = {}; files = {}; codes = {}

    def claim(uuid, kind, value, *, repeat=False):
        _binding_uuid(uuid)
        record = kind, value
        _binding_require(uuid not in claims or repeat and claims[uuid] == record, 'conflicting/duplicate UUID claim')
        _binding_require(uuid not in global_claims or global_claims[uuid] == record, 'conflicting global UUID claim')
        claims[uuid] = record; global_claims[uuid] = record

    for process in proof['processes']:
        _binding_keys(process, 'uuid process_type attempt_input execution_input')
        _binding_require(process['process_type'] in (_PLAN_PROCESS, _RESTART_PROCESS, _CALC_PROCESS), 'process type')
        claim(process['uuid'], 'process', encode(process)); processes[process['uuid']] = process
        if process['process_type'] == _PLAN_PROCESS:
            _binding_require(process['attempt_input'] is process['execution_input'] is None, 'Plan scalar input differs')
        else:
            for name in ('attempt_input', 'execution_input'):
                scalar = process[name]; _binding_keys(scalar, 'node_uuid value')
                if name == 'attempt_input':
                    _binding_positive(scalar['value'])
                else:
                    _binding_atom(scalar['value'], 'Id')
                    _binding_require(scalar['value'] == selection['execution_id'], 'process execution differs')
                claim(scalar['node_uuid'], name, encode(scalar), repeat=True)
    for code in proof['codes']:
        _binding_code(code); claim(code['code_uuid'], 'code', encode(code)); codes[code['code_uuid']] = code
    for item in proof['files']:
        _binding_keys(item, 'node_uuid raw')
        _binding_require(type(item['raw']) is bytes, 'file bytes required', 'invalid_input')
        claim(item['node_uuid'], 'file', item['raw']); files[item['node_uuid']] = item['raw']
    edges = set()
    for edge in proof['links']:
        _binding_keys(edge, 'source_uuid target_uuid type label')
        _binding_uuid(edge['source_uuid']); _binding_uuid(edge['target_uuid'])
        _binding_require(edge['type'] in ('call_work', 'call_calc', 'create', 'input_work', 'input_calc', 'return') and
                         type(edge['label']) is str and re.fullmatch(r'[A-Za-z](?:[A-Za-z0-9_]*[A-Za-z0-9])?', edge['label']) is not None,
                         'typed link/label required', 'invalid_input')
        _binding_require(edge['source_uuid'] in claims and edge['target_uuid'] in claims, 'unknown link endpoint')
        key = tuple(edge[k] for k in ('source_uuid', 'target_uuid', 'type', 'label'))
        _binding_require(key not in edges, 'duplicate link')
        # Repeated row proofs are partial views of the same declared graph.
        # A target has one call/CREATE owner and one source per typed input label.
        if edge['type'] in ('call_work', 'call_calc'):
            link_key = ('call', edge['target_uuid'])
        elif edge['type'] == 'create':
            link_key = ('create', edge['target_uuid'])
        elif edge['type'] in ('input_work', 'input_calc'):
            link_key = ('input', edge['target_uuid'], edge['type'], edge['label'])
        else:
            link_key = None
        if link_key is not None:
            _binding_require(link_key not in global_claims or global_claims[link_key] == key,
                             'conflicting global link snapshot')
            global_claims[link_key] = key
        edges.add(key)
    used = set(); used_files = set()

    def require_edge(source, target, kind, label):
        key = source, target, kind, label
        _binding_require(key in edges, 'required typed input/CREATE link absent')
        used.add(key)

    def file_claim(node):
        _binding_node_file(node)
        uuid = node['node_uuid']; _binding_require(uuid in files, 'original file snapshot missing')
        _binding_raw_file({k:node[k] for k in ('bytes', 'sha256')}, files[uuid])
        used_files.add(uuid)
        return files[uuid]

    _binding_require(selection['run_uuid'] in processes and processes[selection['run_uuid']]['process_type'] == _PLAN_PROCESS,
                     'selected Plan absent')
    _binding_require(selection['calcjob_uuid'] in processes and processes[selection['calcjob_uuid']]['process_type'] == _CALC_PROCESS,
                     'selected CalcJob absent')
    _binding_require(processes[selection['calcjob_uuid']]['attempt_input']['value'] == selection['attempt'], 'current transport attempt differs')
    _binding_require(codes == {execution['code']['code_uuid']:execution['code']}, 'input code snapshot differs')
    graph_raw = file_claim(execution['source_graph'])
    _binding_require(decode(graph_raw, maximum=len(graph_raw)) == graph, 'original graph snapshot differs')
    _binding_require(decode(file_claim(execution['input_plan']), maximum=16777216) == plan, 'original plan snapshot differs')
    native_uuid = execution['storage']['execution_binding_uuid']
    _binding_require(native_uuid in files and decode(files[native_uuid], maximum=len(files[native_uuid])) == execution, 'execution binding snapshot differs')
    used_files.add(native_uuid)
    for uuid, process in processes.items():
        is_calc = process['process_type'] == _CALC_PROCESS
        kind = 'input_calc' if is_calc else 'input_work'
        for label, node in (('plan', execution['input_plan']['node_uuid']), ('source_graph', execution['source_graph']['node_uuid']),
                            ('code', execution['code']['code_uuid'])):
            require_edge(node, uuid, kind, label)
        if process['process_type'] != _PLAN_PROCESS:
            for label, name in (('attempt', 'attempt_input'), ('execution_id', 'execution_input')):
                require_edge(process[name]['node_uuid'], uuid, kind, label)
            require_edge(native_uuid, uuid, kind, 'native_execution')
        if uuid == selection['run_uuid']:
            continue
        calls = [e for e in edges if e[1] == uuid and e[2] in ('call_work', 'call_calc')]
        _binding_require(len(calls) == 1, 'ambiguous process call path')
        edge = calls[0]; parent = processes.get(edge[0])
        _binding_require(parent is not None, 'call parent is not a process')
        _binding_require((is_calc and edge[2] == 'call_calc' and parent['process_type'] in (_PLAN_PROCESS, _RESTART_PROCESS)) or
                         (process['process_type'] == _RESTART_PROCESS and edge[2] == 'call_work' and edge[0] == selection['run_uuid']),
                         'typed call path differs')
        _binding_require(parent['process_type'] != _PLAN_PROCESS or parent['uuid'] == selection['run_uuid'], 'foreign Plan')
        used.add(edge)
    creator = proof['creator_calcjob_uuid']
    _binding_require(creator in processes and processes[creator]['process_type'] == _CALC_PROCESS, 'creator is not CalcJob')
    if proof['mode'] == 'current':
        _binding_require(creator == selection['calcjob_uuid'] and origin_key == row_key and
                         proof['origin_result_uuid'] is proof['origin_binding_uuid'] is None and not proof['hops'], 'current creator declaration differs')
        allowed_processes = {selection['run_uuid'], selection['calcjob_uuid']}
        for edge in used:
            if edge[1] == selection['calcjob_uuid'] and edge[2] == 'call_calc': allowed_processes.add(edge[0])
        owner = dict(calcjob_uuid=creator, binding_uuid=None)
    else:
        _binding_require(creator != selection['calcjob_uuid'] and proof['hops'], 'retained creator declaration differs')
        _binding_uuid(proof['origin_result_uuid']); _binding_uuid(proof['origin_binding_uuid'])
        current = selection['calcjob_uuid']; visited = {current}; bindings = set(); parents = set()
        origin_seen = False
        for hop in proof['hops']:
            _binding_keys(hop, 'child_calcjob_uuid parent_binding_uuid')
            _binding_uuid(hop['child_calcjob_uuid']); _binding_uuid(hop['parent_binding_uuid'])
            uuid = hop['parent_binding_uuid']
            _binding_require(hop['child_calcjob_uuid'] == current and uuid not in bindings and uuid in files, 'parent hop missing/cyclic')
            bindings.add(uuid); used_files.add(uuid)
            require_edge(uuid, current, 'input_calc', 'parent_binding')
            creates = [e for e in edges if e[1] == uuid and e[2] == 'create' and e[3] == 'native_binding']
            _binding_require(len(creates) == 1, 'binding CREATE creator ambiguous/missing')
            parent = creates[0][0]
            _binding_require(parent not in visited and parent in processes and processes[parent]['process_type'] == _CALC_PROCESS, 'parent process cycle/type')
            visited.add(parent); parents.add(parent); used.add(creates[0])
            result_edges = [e for e in edges if e[0] == parent and e[2:] == ('create', 'result')]
            _binding_require(len(result_edges) == 1 and result_edges[0][1] in files, 'parent original result CREATE absent')
            used.add(result_edges[0]); result_uuid = result_edges[0][1]; used_files.add(result_uuid)
            parent_selection = dict(selection, calcjob_uuid=parent, attempt=processes[parent]['attempt_input']['value'])
            stored = decode(files[uuid], maximum=len(files[uuid]))
            rows = _binding_stored(stored, files[result_uuid], parent_selection, plan, execution)
            matches = [row for row in rows.values() if _binding_source_matches(context['source'], row['source'])]
            _binding_require(matches and all(row['source_invocation'] == context['manifest']['invocation'] for row in matches), 'retained native source/epoch differs')
            if parent == creator:
                _binding_require(uuid == proof['origin_binding_uuid'] and result_uuid == proof['origin_result_uuid'] and origin_key in rows,
                                 'origin files/row key differ')
                row = rows[origin_key]
                _binding_require(_binding_source_matches(context['source'], row['source']) and
                                 row['creator'] == dict(calcjob_uuid=creator, binding_uuid=None), 'original source creator differs')
                origin_seen = True
            else:
                _binding_require(all(row['creator'] == dict(calcjob_uuid=creator, binding_uuid=proof['origin_binding_uuid']) for row in matches),
                                 'intermediate retained creator differs')
            current = parent
        _binding_require(origin_seen and current == creator, 'parent chain does not end at origin')
        allowed_processes = {selection['run_uuid'], selection['calcjob_uuid']} | parents
        for edge in used:
            if edge[1] in allowed_processes and edge[2] == 'call_calc': allowed_processes.add(edge[0])
        owner = dict(calcjob_uuid=creator, binding_uuid=proof['origin_binding_uuid'])
    _binding_require(set(processes) == allowed_processes and set(files) == used_files and edges == used, 'unrelated graph nodes/links')
    return owner


def bind_native_result(raw_result: bytes, *, selection: dict, frozen_plan: bytes,
                       plan_node_uuid: str, source_graph: dict, source_graph_node_uuid: str,
                       execution_binding: dict, source_contexts: tuple[dict, ...],
                       creator_proofs: tuple[dict, ...], maximum_metadata_bytes: int) -> dict:
    """Return a detached declaration draft, never an actor/provenance certificate.

    All supplied identities, bytes, links and prior integrity markers are caller
    assertions. No files/ORM/DB/native data are accessed and no node is created.
    """
    args = dict(raw_result=raw_result, selection=selection, frozen_plan=frozen_plan,
                plan_node_uuid=plan_node_uuid, source_graph=source_graph, source_graph_node_uuid=source_graph_node_uuid,
                execution_binding=execution_binding, source_contexts=source_contexts, creator_proofs=creator_proofs,
                maximum_metadata_bytes=maximum_metadata_bytes)
    _binding_admit(args)
    args = deepcopy(args)
    selection = args['selection']; execution = args['execution_binding']; graph_input = args['source_graph']
    _binding_selection(selection); _binding_uuid(plan_node_uuid); _binding_uuid(source_graph_node_uuid)
    _binding_keys(graph_input, 'value raw')
    _binding_require(type(graph_input['raw']) is bytes, 'original graph bytes required', 'invalid_input')
    graph = graph_input['value']
    _binding_schema(graph, 'model-help.schema.json', '#/$defs/SourceGraph')
    _binding_require(decode(graph_input['raw'], maximum=len(graph_input['raw'])) == graph, 'graph original bytes differ from value')
    paths = [relative_path(component['path']) for component in graph['components']]
    _binding_require(len(set(paths)) == len(paths), 'duplicate graph component path')
    plan = scientific_plan(decode(frozen_plan, maximum=16777216))
    _binding_schema(plan, 'scientific-plan.schema.json')
    _binding_keys(execution, 'schema request_id execution_id storage source_graph code input_plan access_receipt_ref')
    _binding_require(execution['schema'] == 'qcl-negf.aiida-native-execution.v1', 'execution binding version')
    _binding_uuid(execution['request_id']); _binding_opaque(execution['access_receipt_ref'])
    _binding_atom(execution['execution_id'], 'Id'); _binding_storage(execution['storage']); _binding_code(execution['code'])
    _binding_require(execution['execution_id'] == selection['execution_id'] and
                     execution['storage']['computer_uuid'] == execution['code']['computer_uuid'], 'execution/computer declaration differs')
    for name, uuid, raw in (('input_plan', plan_node_uuid, frozen_plan), ('source_graph', source_graph_node_uuid, graph_input['raw'])):
        node = execution[name]; _binding_node_file(node)
        _binding_require(node['node_uuid'] == uuid, 'original node UUID differs')
        _binding_raw_file({k:node[k] for k in ('bytes', 'sha256')}, raw)
    result = decode(raw_result, maximum=33554432)
    rows, points = _binding_rows(result, selection, plan)
    nonabsent = {key for key,row in rows.items() if row['data']['native_source']['kind'] != 'absent'}
    contexts = {}; proofs = {}
    for context in args['source_contexts']:
        _binding_keys(context, _CONTEXT); key = _binding_row_key(context['row_key'])
        _binding_require(key in nonabsent and key not in contexts, 'extra/duplicate native context')
        _binding_require(_binding_same(context['source'], rows[key]['data']['native_source']), 'context source differs from raw row')
        validate_outward_native_source(context['source'], **{k:context[k] for k in
            ('manifest', 'manifest_ref', 'checkpoint', 'diagnostics', 'observations', 'plan', 'model', 'parent_manifest', 'parent_checkpoint')})
        identity = context['source']['selected_state']['identity']
        _binding_require(identity['plan_sha256'] == execution['input_plan']['sha256'] and
                         identity['model_sha256'] == points[key[1]]['effective_inputs']['model_sha256'] and
                         context['manifest']['provenance'] == graph, 'native original plan/model/graph differs')
        if context['source']['kind'] == 'final':
            _binding_require(_binding_same(context['plan'], plan) and
                             context['observations']['request']['plan']['bytes'] == str(len(frozen_plan)), 'final original plan differs')
        contexts[key] = context
    for proof in args['creator_proofs']:
        _binding_keys(proof, _PROOF); key = _binding_row_key(proof['row_key'])
        _binding_require(key in nonabsent and key not in proofs, 'extra/duplicate creator proof')
        proofs[key] = proof
    _binding_require(set(contexts) == set(proofs) == nonabsent, 'missing selected native context/proof')
    owners = {}; global_claims = {}
    for key in nonabsent:
        owners[key] = _binding_creator(proofs[key], selection=selection, execution=execution, plan=plan, graph=graph,
                                      context=contexts[key], global_claims=global_claims)

    def project(row):
        key = row['execution_id'], row['id'], row['attempt']
        return dict(point_id=row['id'], point_attempt=row['attempt'], source=row['data']['native_source'],
                    source_invocation=contexts[key]['manifest']['invocation'] if key in contexts else None,
                    creator=owners.get(key), plan_point_id=row['id'], coordinates=row['coordinates'])
    return deepcopy(dict(selection=selection, input_plan=execution['input_plan'], source_graph=execution['source_graph'],
                         storage=execution['storage'], raw_result=dict(bytes=str(len(raw_result)), sha256=hashlib.sha256(raw_result).hexdigest()),
                         points=[project(row) for row in result['points']], attempt_history=[project(row) for row in result['attempt_history']]))
