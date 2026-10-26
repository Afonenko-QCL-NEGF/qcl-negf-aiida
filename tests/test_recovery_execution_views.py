"""Read-only recovery metadata, fake ORM only: no profile or scheduler access."""
from datetime import datetime, timezone
from types import SimpleNamespace as NS
from uuid import UUID

import pytest
from aiida.common.links import LinkType
from aiida.common.exceptions import NotExistent
from aiida_qcl_negf import service

RESTART = 'aiida.workflows:qcl_negf.execution_restart'
CALC = 'aiida.calculations:qcl_negf.execution'

class Node:
    def __init__(self, pk, process_type=RESTART):
        self.pk, self.uuid, self.process_type = pk, str(UUID(int=pk)), process_type
        self.label, self.process_state = 'metadata', NS(value='waiting')
        self.exit_status, self.is_finished_ok = None, False
        self.ctime = self.mtime = datetime(2026, 10, 26, tzinfo=timezone.utc)
        self._input_links = NS(attempt=NS(value=3), execution_id=NS(value='execution-1'),
                         plan=NS(uuid=str(UUID(int=87))))
        self._output_links = NS()

    @property
    def inputs(self):
        raise AssertionError('unbounded input namespace expansion')
    @property
    def outputs(self):
        raise AssertionError('unbounded output namespace expansion')

class Calc(Node):
    def __init__(self, pk):
        super().__init__(pk, CALC)
    def get_job_id(self):
        return '7'
    def get_scheduler_state(self):
        return NS(value='running')

@pytest.fixture
def graph(monkeypatch):
    root, child = Node(132), Calc(134)
    data = NS(root=root, child=child, callers=[], children=[(child, 'iteration_01')],
              rows=[root], queries=[], plans=[])
    class Query:
        def __init__(self):
            self.parts, self.cap, self.skip, self.order = [], None, None, None
            data.queries.append(self)
        def append(self, cls, **kw):
            self.parts.append((cls, kw)); return self
        def order_by(self, order):
            self.order = order; return self
        def limit(self, cap):
            self.cap = cap; return self
        def offset(self, skip):
            self.skip = skip; return self
        def all(self, flat=False):
            assert self.cap is not None, 'must limit before materialization'
            if len(self.parts) == 1:
                assert self.parts[0][1]['filters'] == {'process_type': RESTART}
                return data.rows[self.skip:self.skip+self.cap]
            kw = self.parts[1][1]
            if self.parts[0][1].get('tag') == 'owner':
                assert self.cap == 2
                owner_pk = self.parts[0][1]['filters']['id']
                owner = root if owner_pk == root.pk else child
                edge = kw['edge_filters']
                assert set(edge) == {'type', 'label'}
                label = edge['label']
                if 'with_outgoing' in kw:
                    assert kw['with_outgoing'] == 'owner'
                    expected = LinkType.INPUT_CALC if isinstance(owner, Calc) else LinkType.INPUT_WORK
                    assert edge['type'] == expected.value
                    assert label in {'plan', 'execution_id', 'attempt', 'recovery_bundle', 'prior_archive'}
                    item = getattr(owner._input_links, label, None)
                else:
                    assert kw['with_incoming'] == 'owner'
                    assert edge['type'] == LinkType.CREATE.value
                    assert label in {'retrieved', 'result'}
                    item = getattr(owner._output_links, label, None)
                rows = item if isinstance(item, list) else ([] if item is None else [item])
                return rows[:self.cap]
            if 'with_outgoing' in kw:
                assert kw['edge_filters'] == {'type': LinkType.CALL_WORK.value}
                assert self.cap == 2
                return data.callers[:self.cap]
            assert kw['with_incoming'] == 'recovery'
            assert kw['edge_filters'] == {'type': LinkType.CALL_CALC.value}
            assert 'filters' not in kw  # foreign edges must not disappear
            assert self.cap == 101
            assert self.order == {'child': [{'ctime': 'asc'}, {'id': 'asc'}]}
            return data.children[:self.cap]
    def load(identifier):
        if identifier == root.uuid:
            return root
        raise NotExistent('missing')
    def plan(node):
        data.plans.append(node); return {'fingerprint': 'saved-fingerprint'}
    monkeypatch.setattr(service.orm, 'WorkChainNode', Node)
    monkeypatch.setattr(service.orm, 'CalcJobNode', Calc)
    monkeypatch.setattr(service.orm, 'QueryBuilder', Query)
    monkeypatch.setattr(service.orm, 'load_node', load)
    monkeypatch.setattr(service, 'read_plan', plan)
    monkeypatch.setattr(service, 'read_json', lambda *a, **k: pytest.fail('payload read'))
    return data

def test_detail_standalone_actual_provenance_and_nulls(graph):
    result = service.get_recovery_execution(graph.root.uuid)
    assert result['kind'] == 'recovery_execution' and result['caller'] is None
    assert result['plan'] == {'uuid': graph.root._input_links.plan.uuid, 'fingerprint': 'saved-fingerprint'}
    assert result['execution_id'] == 'execution-1' and result['attempt'] == 3
    assert result['recovery'] == {'input_bundle_uuid': None, 'input_prior_archive_uuid': None}
    child = result['children'][0]
    assert child['call_label'] == 'iteration_01' and child['attempt'] == 3
    assert child['job_id'] == '7' and child['scheduler_state'] == 'running'
    assert child['retrieved_uuid'] is None and child['result_uuid'] is None
    assert 'results' not in result

def test_list_has_caller_no_plan_io_and_stable_bounded_page(graph):
    caller = Node(1, service.PROCESS_TYPE)
    graph.callers = [caller]
    result = service.list_recovery_executions(limit=1, offset=0)
    assert result[0]['caller'] == {'uuid': caller.uuid, 'process_type': caller.process_type}
    assert graph.plans == []
    assert graph.queries[0].order == {'recovery': [{'ctime': 'desc'}, {'id': 'desc'}]}
    assert service.list_recovery_executions(limit=1, offset=1) == []

@pytest.mark.parametrize('limit,offset', [(0,0),(201,0),(True,0),(1,-1),(1,False)])
def test_list_invalid_page(graph, limit, offset):
    with pytest.raises(ValueError): service.list_recovery_executions(limit, offset)

@pytest.mark.parametrize('identifier', ['132', '00000000000000000000000000000084', '{00000000-0000-0000-0000-000000000084}'])
def test_uuid_requires_canonical_form(graph, identifier):
    with pytest.raises(ValueError): service.get_recovery_execution(identifier)

def test_missing_and_wrong_type_are_not_recovery(graph):
    with pytest.raises(LookupError): service.get_recovery_execution(str(UUID(int=200)))
    graph.root.process_type = service.PROCESS_TYPE
    with pytest.raises(LookupError): service.get_recovery_execution(graph.root.uuid)

def test_ambiguous_caller_refused(graph):
    graph.callers = [Node(1), Node(2)]
    with pytest.raises(ValueError, match='caller'): service.get_recovery_execution(graph.root.uuid)

def test_all_direct_edges_inspected_and_overflow_refused(graph):
    graph.children = [(Node(3, 'foreign'), 'foreign')]
    with pytest.raises(ValueError, match='child'): service.get_recovery_execution(graph.root.uuid)
    graph.children = [(graph.child, 'iteration')] * 101
    with pytest.raises(ValueError, match='100'): service.get_recovery_execution(graph.root.uuid)

def test_optional_metadata_null_and_input_associations(graph):
    del graph.root._input_links.attempt
    del graph.child._input_links.attempt
    graph.child.get_job_id = lambda: None
    graph.child.get_scheduler_state = lambda: None
    graph.root._input_links.recovery_bundle = NS(uuid=str(UUID(int=10)))
    graph.root._input_links.prior_archive = NS(uuid=str(UUID(int=11)))
    graph.child._output_links.retrieved = NS(uuid=str(UUID(int=12)))
    graph.child._output_links.result = NS(uuid=str(UUID(int=13)))
    result = service.get_recovery_execution(graph.root.uuid)
    assert result['attempt'] is None and result['children'][0]['attempt'] is None
    assert result['children'][0]['job_id'] is None and result['children'][0]['scheduler_state'] is None
    assert result['recovery']['input_bundle_uuid'] == str(UUID(int=10))
    assert result['recovery']['input_prior_archive_uuid'] == str(UUID(int=11))
    assert result['children'][0]['result_uuid'] == str(UUID(int=13))

@pytest.mark.parametrize('value', [0, True, '1'])
def test_malformed_attempt_refused(graph, value):
    graph.child._input_links.attempt.value = value
    with pytest.raises(ValueError, match='attempt'): service.get_recovery_execution(graph.root.uuid)

def test_invalid_plan_and_serialized_response_refused(graph, monkeypatch):
    monkeypatch.setattr(service, 'read_plan', lambda node: {})
    with pytest.raises(ValueError, match='plan'): service.get_recovery_execution(graph.root.uuid)
    monkeypatch.setattr(service, 'read_plan', lambda node: {'fingerprint': 'fp'})
    monkeypatch.setattr(service, 'MAX_RESPONSE_BYTES', 50)
    with pytest.raises(ValueError, match='response'): service.get_recovery_execution(graph.root.uuid)
    with pytest.raises(ValueError, match='response'): service.list_recovery_executions()

@pytest.mark.parametrize('name', ['plan', 'execution_id', 'attempt', 'recovery_bundle', 'prior_archive'])
def test_ambiguous_exact_input_refused(graph, name):
    setattr(graph.root._input_links, name, [NS(uuid=str(UUID(int=1))), NS(uuid=str(UUID(int=2)))])
    with pytest.raises(ValueError):
        service.get_recovery_execution(graph.root.uuid)

@pytest.mark.parametrize('name', ['retrieved', 'result'])
def test_ambiguous_exact_output_refused(graph, name):
    setattr(graph.child._output_links, name, [NS(uuid=str(UUID(int=1))), NS(uuid=str(UUID(int=2)))])
    with pytest.raises(ValueError, match='Ambiguous'):
        service.get_recovery_execution(graph.root.uuid)

def test_missing_required_plan_refused(graph):
    del graph.root._input_links.plan
    with pytest.raises(ValueError, match='plan'):
        service.get_recovery_execution(graph.root.uuid)
