"""Independent actual-service agent-file tests; no profile/daemon/scientific IO.

First source freeze only: neither collection nor test bodies have been run.
"""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace as NS
import hashlib
import json

import aiida
import pytest
from qcl_negf_contracts import agent_reports
from aiida_qcl_negf import service

RUN = '11111111-2222-4333-8444-555555555555'
OTHER = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'
CHILD1 = '12345678-1234-4234-8234-123456789abc'
CHILD2 = '23456789-2345-4345-8345-23456789abcd'
CHILD3 = '34567890-3456-4456-8456-34567890abcd'
REPORT = 'cccccccc-dddd-4eee-8fff-000000000001'
STAMP = datetime(2026, 10, 10, 0, 0, 1, tzinfo=timezone.utc)
ANCHOR = {'run_uuid': RUN, 'root_definition_id': 'study.main', 'root_kind': 'study',
          'plan_fingerprint': '0' * 64}
USED = {'run_uuid': RUN, 'plan_fingerprint': '0' * 64, 'execution_id': 'exec-1',
        'definition_id': 'definition-1', 'variant_id': 'variant-1',
        'attempt': 1, 'calcjob_uuid': CHILD1}
VALUES = {'schema': 'qcl-negf-agent-report-v1', 'anchor': ANCHOR,
          'question_snapshot': 'What is supported?', 'used_runs': [USED],
          'conclusion': 'accepted', 'reasoning': 'Author prose only.',
          'limitations': 'No canonical research card is established.'}
RAW = b''' {
"schema":"qcl-negf-agent-report-v1",
"anchor":{"run_uuid":"11111111-2222-4333-8444-555555555555","root_definition_id":"study.main","root_kind":"study","plan_fingerprint":"0000000000000000000000000000000000000000000000000000000000000000"},
"question_snapshot":"What is supported?",
"used_runs":[{"run_uuid":"11111111-2222-4333-8444-555555555555","plan_fingerprint":"0000000000000000000000000000000000000000000000000000000000000000","execution_id":"exec-1","definition_id":"definition-1","variant_id":"variant-1","attempt":1,"calcjob_uuid":"12345678-1234-4234-8234-123456789abc"}],
"conclusion":"accepted","reasoning":"Author prose only.",
"limitations":"No canonical research card is established."
}\n'''
SCIENCE = b'{"status":"failed","quality":"not_converged","converged":false}\n'
PLANS = {
    RUN: {'root_definition_id': 'study.main', 'root_kind': 'study', 'fingerprint': '0' * 64,
          'executions': [{'id': 'exec-1', 'definition_id': 'definition-1', 'variant_id': 'variant-1'}]},
    OTHER: {'root_definition_id': 'study.other', 'root_kind': 'meta', 'fingerprint': '1' * 64,
            'executions': [{'id': 'exec-2', 'definition_id': 'definition-2', 'variant_id': 'variant-2'}]},
}


def encoded(value):
    # Test input builder only, not a validator-derived output oracle.
    return json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8')


def expected_attrs(raw=RAW):
    return {'schema': 'qcl-negf-agent-report-v1', 'anchor_run_uuid': RUN,
            'root_definition_id': 'study.main', 'root_kind': 'study',
            'anchor_plan_fingerprint': '0' * 64, 'bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest()}


def expected_receipt(raw=RAW):
    return {'uuid': REPORT, 'filename': 'agent-report.json', 'bytes': len(raw),
            'sha256': hashlib.sha256(raw).hexdigest(), 'ctime': STAMP.isoformat(),
            'anchor': deepcopy(ANCHOR)}


@pytest.fixture
def source(monkeypatch):
    assert aiida.__version__ == '2.9.2'
    assert Path(service.__file__).resolve() == Path(__file__).resolve().parents[1] / 'src/aiida_qcl_negf/service.py'
    dependency = Path(__file__).resolve().parents[2] / 'qcl-negf-contracts/src/qcl_negf_contracts/agent_reports.py'
    assert Path(agent_reports.__file__).resolve(strict=True) == dependency.resolve(strict=True)
    assert hashlib.sha256(dependency.read_bytes()).hexdigest() == '62cf47cace57a59b9051a6c08634c89a8884fb7fa934be4654496c1a769fe3cc'
    events = []; created = []; query_rows = []; plans = deepcopy(PLANS); registry = {}

    def forbidden(name):
        def refuse(*args, **kwargs):
            events.append(('FORBIDDEN', name));raise AssertionError('forbidden agent-file effect: ' + name)
        return refuse

    class Attrs:
        def __init__(self, node):self.node = node;self.values = {}
        def set(self, key, value):
            assert not self.node.is_stored
            events.append(('attr_set', key));self.values[key] = deepcopy(value)
        def set_many(self, values):
            for key, value in values.items():self.set(key, value)
        def get(self, key, default=None):return deepcopy(self.values.get(key, default))
        @property
        def all(self):return deepcopy(self.values)

    class Reader(BytesIO):
        def __init__(self, data, role):super().__init__(data);self.role = role
        def read(self, maximum=-1):
            events.append(('read', self.role, maximum))
            if self.role == 'report':assert 0 < maximum <= 262145
            else:assert 0 < maximum <= 64 * 1024**2 + 1
            return super().read(maximum)
        def close(self):events.append(('close', self.role));super().close()

    class Plan:
        def __init__(self, uuid):self.uuid = uuid
        @contextmanager
        def open(self, mode):
            assert mode == 'rb';events.append(('plan_open', self.uuid));reader = Reader(encoded(plans[self.uuid]), 'plan')
            try:yield reader
            finally:reader.close()

    class FakeChild:
        def __setattr__(self, name, value):
            if name in self.__dict__:
                events.append(('FORBIDDEN', 'child_field_write'));raise AssertionError('scientific child fields immutable')
            object.__setattr__(self, name, value)
        def __init__(self, uuid, execution, attempt, second):
            self.uuid = uuid;self.ctime = STAMP.replace(second=second)
            self.inputs = NS(execution_id=NS(value=execution), attempt=NS(value=attempt))
            self.outputs = NS(result=NS(open=forbidden('scientific_result_read')))
            self.scientific_bytes = SCIENCE;self.machine_status = {'status': 'failed', 'quality': 'not_converged', 'converged': False}

    class FakeRun:
        def __setattr__(self, name, value):
            if name in self.__dict__:
                events.append(('FORBIDDEN', 'run_field_write'));raise AssertionError('run fields immutable')
            object.__setattr__(self, name, value)
        process_type = service.PROCESS_TYPE
        def __init__(self, uuid, descendants):
            self.uuid = uuid;self.inputs = NS(plan=Plan(uuid));self.called_descendants = descendants
            # Deliberately inconsistent published/latest selection: exact selectors must bypass it.
            self.outputs = NS(selections={'latest': NS(get_dict=forbidden('published_default_selection'))})
            self.machine_status = {'status': 'failed', 'quality': 'not_converged', 'converged': False}
            self.base = NS(extras=NS(set=forbidden('run_extras')), links=NS(add_incoming=forbidden('run_link')))

    child1 = FakeChild(CHILD1, 'exec-1', 1, 1);child2 = FakeChild(CHILD2, 'exec-1', 2, 2);child3 = FakeChild(CHILD3, 'exec-2', 1, 3)
    runs = {RUN: FakeRun(RUN, [child2, child1]), OTHER: FakeRun(OTHER, [child3])};registry.update(runs)

    class FakeFile:
        def __init__(self, file, filename):
            assert isinstance(file, BytesIO) and file.tell() == 0 and filename == 'agent-report.json'
            self.raw = file.getvalue();self.filename = filename;self.uuid = REPORT;self.ctime = STAMP;self.is_stored = False;self.store_count = 0
            self.base = NS(attributes=Attrs(self), extras=NS(set=forbidden('report_extras')), links=NS(add_incoming=forbidden('report_link')))
            self.base.attributes.values['filename'] = filename  # native SinglefileData constructor metadata
            self.base.repository = NS(open=self.open)
            created.append(self);events.append(('file_new', filename, self.raw))
        def store(self):
            assert not self.is_stored and self.store_count == 0
            events.append(('store', deepcopy(self.base.attributes.values)));self.store_count += 1;self.is_stored = True;registry[self.uuid] = self;return self
        @contextmanager
        def open(self, path=None, mode='r'):
            assert path in (None, self.filename) and mode == 'rb';events.append(('report_open', self.uuid));reader = Reader(self.raw, 'report')
            try:yield reader
            finally:reader.close()

    def load_node(identifier):
        events.append(('load', identifier))
        if identifier not in registry:raise service.NotExistent('missing literal node')
        return registry[identifier]

    class Query:
        def __init__(self):self.tag = None;self.limit_value = None;self.offset_value = None;events.append(('query_new',))
        def append(self, cls, *, filters, tag):
            assert cls is FakeFile and filters == {'attributes.schema': 'qcl-negf-agent-report-v1', 'attributes.anchor_run_uuid': RUN}
            assert isinstance(tag, str) and tag
            self.tag = tag;events.append(('query_append', deepcopy(filters), tag));return self
        def order_by(self, order):
            assert order == {self.tag: {'ctime': 'desc'}}
            events.append(('query_order', deepcopy(order)));return self
        def limit(self, value):self.limit_value = value;events.append(('query_limit', value));return self
        def offset(self, value):self.offset_value = value;events.append(('query_offset', value));return self
        def all(self, flat=False):
            assert flat is True and self.limit_value is not None and self.offset_value is not None
            events.append(('query_all', flat));return list(query_rows)

    monkeypatch.setattr(service.orm, 'WorkChainNode', FakeRun)
    monkeypatch.setattr(service.orm, 'CalcJobNode', FakeChild)
    monkeypatch.setattr(service.orm, 'SinglefileData', FakeFile)
    monkeypatch.setattr(service.orm, 'load_node', load_node)
    monkeypatch.setattr(service.orm, 'QueryBuilder', Query)
    monkeypatch.setattr(service.orm, 'Log', NS(collection=NS(find=forbidden('process_log'))))
    for name in ('submit', 'kill_processes', 'get_daemon_client', 'plan_data', 'read_json'):
        monkeypatch.setattr(service, name, forbidden(name))
    monkeypatch.setattr(service, 'Path', forbidden('host_path'))
    # Recording wrappers delegate to ORIGINAL actual helpers, not replacement selection/validation.
    original_select = service._select_child;original_read_plan = service.read_plan
    def selected(node, execution_id, *, attempt=None, calcjob_uuid=None):
        events.append(('select', node.uuid, execution_id, attempt, calcjob_uuid))
        assert attempt is not None and calcjob_uuid is not None
        return original_select(node, execution_id, attempt=attempt, calcjob_uuid=calcjob_uuid)
    def read_plan(node):events.append(('read_plan', node.uuid));return original_read_plan(node)
    monkeypatch.setattr(service, '_select_child', selected);monkeypatch.setattr(service, 'read_plan', read_plan)
    snapshot = (deepcopy([r.machine_status for r in runs.values()]), deepcopy([c.machine_status for c in (child1, child2, child3)]), [c.scientific_bytes for c in (child1, child2, child3)])
    ctx = NS(events=events, created=created, plans=plans, runs=runs, registry=registry, query_rows=query_rows,
             File=FakeFile, children=[child1, child2, child3])
    yield ctx
    assert not any(event[0] == 'FORBIDDEN' for event in events)
    assert snapshot == (list(r.machine_status for r in runs.values()), [c.machine_status for c in ctx.children], [c.scientific_bytes for c in ctx.children])


def test_save_exact_raw_once_before_receipt_and_prose_does_not_accept_science(source):
    assert service.save_agent_report(RUN, RAW) == expected_receipt()
    assert len(source.created) == 1
    node = source.created[0]
    assert node.raw == RAW and node.filename == 'agent-report.json' and node.store_count == 1
    assert node.base.attributes.values == {**expected_attrs(), 'filename': 'agent-report.json'}
    kinds = [e[0] for e in source.events]
    assert kinds.index('select') < kinds.index('file_new') < kinds.index('store')
    assert all(e[1] in expected_attrs() for e in source.events if e[0] == 'attr_set')
    assert [e for e in source.events if e[0] == 'read_plan'] == [('read_plan', RUN)]
    assert [e for e in source.events if e[0] == 'select'] == [('select', RUN, 'exec-1', 1, CHILD1)]


def test_save_utf8_text_and_whitespace_are_lossless(source):
    payload = RAW.replace(b'accepted', 'accepted Δ e\u0301 😀'.encode())
    assert service.save_agent_report(RUN, payload.decode('utf-8')) == expected_receipt(payload)
    assert source.created[0].raw == payload


def test_save_exact_raw_cap_is_permitted(source):
    value = deepcopy(VALUES);value['reasoning'] = ''
    fixed = len(encoded(value));value['reasoning'] = 'x' * (262144 - fixed)
    payload = encoded(value);assert len(payload) == 262144
    assert service.save_agent_report(RUN, payload) == expected_receipt(payload)
    assert source.created[0].raw == payload and source.created[0].store_count == 1


def test_empty_used_runs_stores_without_child_selection(source):
    value = deepcopy(VALUES);value['used_runs'] = [];payload = encoded(value)
    assert service.save_agent_report(RUN, payload) == expected_receipt(payload)
    assert not any(e[0] == 'select' for e in source.events)


def test_distinct_roots_are_read_once_and_both_selectors_bind_each_tuple(source):
    value = deepcopy(VALUES)
    value['used_runs'] += [{**USED, 'attempt': 2, 'calcjob_uuid': CHILD2},
                          {'run_uuid': OTHER, 'plan_fingerprint': '1' * 64, 'execution_id': 'exec-2',
                           'definition_id': 'definition-2', 'variant_id': 'variant-2', 'attempt': 1, 'calcjob_uuid': CHILD3}]
    payload = encoded(value);assert service.save_agent_report(RUN, payload) == expected_receipt(payload)
    assert [e for e in source.events if e[0] == 'read_plan'] == [('read_plan', RUN), ('read_plan', OTHER)]
    assert [e for e in source.events if e[0] == 'select'] == [('select', RUN, 'exec-1', 1, CHILD1), ('select', RUN, 'exec-1', 2, CHILD2), ('select', OTHER, 'exec-2', 1, CHILD3)]
    assert sum(e[0] == 'store' for e in source.events) == 1
    # Every referenced root/attempt must be admitted before even constructing the file.
    first_file = next(i for i, e in enumerate(source.events) if e[0] == 'file_new')
    store_index = next(i for i, e in enumerate(source.events) if e[0] == 'store')
    assert all(i < first_file for i, e in enumerate(source.events) if e[0] in ('read_plan', 'select'))
    assert first_file < store_index


@pytest.mark.parametrize('field,value', [('run_uuid', OTHER), ('root_definition_id', 'wrong'), ('root_kind', 'meta'), ('plan_fingerprint', '2' * 64)])
def test_anchor_mismatch_refuses_before_file_store(source, field, value):
    report = deepcopy(VALUES);report['anchor'][field] = value
    with pytest.raises((ValueError, LookupError)):
        service.save_agent_report(RUN, encoded(report))
    assert source.created == [] and not any(e[0] == 'store' for e in source.events)


@pytest.mark.parametrize('field,value', [('plan_fingerprint', '2' * 64), ('execution_id', 'absent'), ('definition_id', 'wrong'), ('variant_id', 'wrong'), ('attempt', 3), ('calcjob_uuid', CHILD2), ('run_uuid', OTHER)])
def test_used_reference_exact_identity_refuses_before_file_store(source, field, value):
    report = deepcopy(VALUES);report['used_runs'][0][field] = value
    with pytest.raises((ValueError, LookupError)):
        service.save_agent_report(RUN, encoded(report))
    assert source.created == [] and not any(e[0] == 'store' for e in source.events)


@pytest.mark.parametrize('fault', ['fingerprint', 'variant', 'attempt-uuid-conflict', 'foreign-root'])
def test_invalid_last_reference_after_valid_first_never_constructs_or_stores(source, fault):
    report = deepcopy(VALUES)
    # Independent second tuple differs from the first; format/duplicate checks pass.
    last = {'run_uuid': RUN, 'plan_fingerprint': '0' * 64, 'execution_id': 'exec-1',
            'definition_id': 'definition-1', 'variant_id': 'variant-1',
            'attempt': 2, 'calcjob_uuid': CHILD2}
    if fault == 'fingerprint':last['plan_fingerprint'] = '2' * 64
    elif fault == 'variant':last['variant_id'] = 'wrong-last-variant'
    elif fault == 'attempt-uuid-conflict':last['calcjob_uuid'] = CHILD1
    else:
        # The same explicit CalcJob belongs to RUN, not this other root/plan.
        last = {'run_uuid': OTHER, 'plan_fingerprint': '1' * 64, 'execution_id': 'exec-2',
                'definition_id': 'definition-2', 'variant_id': 'variant-2',
                'attempt': 1, 'calcjob_uuid': CHILD1}
    report['used_runs'].append(last)
    with pytest.raises((ValueError, LookupError)):
        service.save_agent_report(RUN, encoded(report))
    assert source.created == []
    assert not any(e[0] in ('file_new', 'store', 'attr_set') for e in source.events)
    # For selector-specific faults, observe the actual failing exact membership/attempt boundary.
    # No first/last traversal order is imposed: all-before-store is the owning requirement.
    if fault in ('attempt-uuid-conflict', 'foreign-root'):
        assert ('select', last['run_uuid'], last['execution_id'], last['attempt'], last['calcjob_uuid']) in source.events


def test_foreign_child_not_in_selected_run_is_refused(source):
    source.runs[RUN].called_descendants[:] = [source.children[1]]
    with pytest.raises(LookupError):service.save_agent_report(RUN, RAW)
    assert not source.created


def test_ambiguous_frozen_execution_id_is_refused_before_store(source):
    source.plans[RUN]['executions'].append(deepcopy(source.plans[RUN]['executions'][0]))
    with pytest.raises(ValueError):service.save_agent_report(RUN, RAW)
    assert not source.created


@pytest.mark.parametrize('payload', [b'\xff', b'\xef\xbb\xbf' + RAW,
    RAW.decode('ascii').encode('utf-16-le'), RAW.decode('ascii').encode('utf-32-be'),
    RAW.replace(b'What is supported?', b'\\ud800'), RAW + b' ' * 262144,
    RAW.replace(b'"schema":', b'"schema":null,"schema":', 1)],
    ids=['invalid-utf8', 'utf8-bom', 'utf16-no-bom', 'utf32-no-bom', 'surrogate', 'raw-overcap', 'duplicate'])
def test_format_is_refused_before_any_orm_access(source, payload):
    with pytest.raises(ValueError):service.save_agent_report(RUN, payload)
    assert source.events == [] and source.created == []


def stored(source, payload=RAW):
    node = source.File(BytesIO(payload), 'agent-report.json')
    node.base.attributes.set_many(expected_attrs(payload));node.is_stored = True
    source.registry[REPORT] = node;source.events.clear();return node


def test_read_exact_raw_bounds_closes_and_never_reads_plans_or_stores(source):
    node = stored(source)
    assert service.read_agent_report(RUN, REPORT) == RAW
    assert ('read', 'report', 262145) in source.events and ('close', 'report') in source.events
    assert not any(e[0] in ('store', 'read_plan', 'select') for e in source.events)
    assert node.raw == RAW


@pytest.mark.parametrize('corruption', ['sha', 'bytes', 'raw-invalid', 'raw-oversize', 'anchor-root-id', 'anchor-kind', 'anchor-fingerprint'])
def test_read_corrupt_file_or_bound_attributes_refuses_without_store(source, corruption):
    node = stored(source)
    if corruption == 'sha':node.base.attributes.values['sha256'] = 'f' * 64
    elif corruption == 'bytes':node.base.attributes.values['bytes'] += 1
    elif corruption == 'raw-invalid':node.raw = b'\xff'
    elif corruption == 'raw-oversize':node.raw = b' ' * 262145
    elif corruption == 'anchor-root-id':node.base.attributes.values['root_definition_id'] = 'wrong'
    elif corruption == 'anchor-kind':node.base.attributes.values['root_kind'] = 'meta'
    else:node.base.attributes.values['anchor_plan_fingerprint'] = 'f' * 64
    with pytest.raises(ValueError):service.read_agent_report(RUN, REPORT)
    assert not any(e[0] in ('store', 'read_plan', 'select') for e in source.events)
    assert ('close', 'report') in source.events


@pytest.mark.parametrize('wrong', ['anchor', 'schema', 'type', 'missing'])
def test_read_wrong_resource_refuses_before_repository_access(source, wrong):
    node = stored(source)
    if wrong == 'anchor':node.base.attributes.values['anchor_run_uuid'] = OTHER
    elif wrong == 'schema':node.base.attributes.values['schema'] = 'foreign-file'
    elif wrong == 'type':source.registry[REPORT] = source.runs[OTHER]
    else:del source.registry[REPORT]
    with pytest.raises((LookupError, ValueError)):service.read_agent_report(RUN, REPORT)
    assert not any(e[0] in ('report_open', 'read_plan', 'select', 'store') for e in source.events)


def test_list_uses_filtered_ordered_limited_db_query_and_returns_metadata_only(source):
    node = stored(source);source.query_rows[:] = [node]
    assert service.list_agent_reports(RUN, limit=1, offset=2) == [expected_receipt()]
    kinds = [e[0] for e in source.events]
    assert kinds.index('query_order') < kinds.index('query_all')
    assert kinds.index('query_limit') < kinds.index('query_all') and kinds.index('query_offset') < kinds.index('query_all')
    assert ('query_limit', 1) in source.events and ('query_offset', 2) in source.events
    assert not any(e[0] in ('report_open', 'read', 'read_plan', 'store', 'select') for e in source.events)


@pytest.mark.parametrize('limit,offset', [(True, 0), (0, 0), (101, 0), (1, True), (1, -1), (1, 0.5)])
def test_list_invalid_pagination_refuses_before_query(source, limit, offset):
    with pytest.raises(ValueError):service.list_agent_reports(RUN, limit=limit, offset=offset)
    assert not any(e[0].startswith('query') for e in source.events)


def test_list_default_is_bounded_twenty(source):
    assert service.list_agent_reports(RUN) == []
    assert ('query_limit', 20) in source.events and ('query_offset', 0) in source.events


def test_list_encoded_envelope_response_bound_is_enforced(source, monkeypatch):
    node = stored(source);source.query_rows[:] = [node];receipt = expected_receipt()
    # Independent actual HTTP collection envelope; UTF8+JSON overhead is counted.
    size = len(encoded({'reports': [receipt]}))
    monkeypatch.setattr(service, 'MAX_RESPONSE_BYTES', size)
    assert service.list_agent_reports(RUN, limit=1) == [receipt]
    monkeypatch.setattr(service, 'MAX_RESPONSE_BYTES', size - 1)
    with pytest.raises(ValueError):service.list_agent_reports(RUN, limit=1)
    assert not any(e[0] in ('store', 'report_open', 'read_plan') for e in source.events)
