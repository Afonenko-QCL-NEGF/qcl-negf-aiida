"""Literal fake-ORM controls, deliberately separate from native persistence.

Source-only addition: collection and bodies require the separately booked pass.
The fake Dict mirrors native metadata living in the same attribute namespace.
"""
import hashlib
import json
from datetime import datetime
from io import BytesIO
from types import SimpleNamespace as NS
from uuid import UUID

import pytest
from aiida.common.exceptions import NotExistent

from aiida_qcl_negf import research_cards as cards
from aiida_qcl_negf import service

C1 = '11111111-1111-4111-8111-111111111111'
C2 = '22222222-2222-4222-8222-222222222222'
V2 = '33333333-3333-4333-8333-333333333333'
R1 = '44444444-4444-4444-8444-444444444444'
R2 = '55555555-5555-4555-8555-555555555555'
REPORT = '66666666-6666-4666-8666-666666666666'
CHILD = '77777777-7777-4777-8777-777777777777'
F1, F2 = 'a' * 64, 'b' * 64
RAW = b'{"schema":"qcl-negf-research-card-v1","title":"Test","goal":" Compare A/B ","question":"Why?\\nNegative results remain."}'
CARD = {'schema':'qcl-negf-research-card-v1','title':'Test','goal':' Compare A/B ', 'question':'Why?\nNegative results remain.'}
STAMP = '2026-10-25T00:00:00+00:00'

def request(n):
    return str(UUID(int=1000 + n))


@pytest.fixture
def source(monkeypatch):
    registry, constructed, stored, reads, selected = {}, [], [], [], []
    identifiers = iter([C1, C2, V2] + [str(UUID(int=i)) for i in range(100, 600)])
    fault = {'store':False}
    class Attrs:
        def __init__(self, node): self.node = node; self.values = {}
        def get(self, key, default=None): return self.values.get(key, default)
        def set_many(self, fields):
            assert not self.node.is_stored
            self.values.update(fields)
    class Data:
        def __init__(self):
            self.uuid = next(identifiers); self.ctime = datetime.fromisoformat(STAMP)
            self.is_stored = False; self.base = NS(attributes=Attrs(self)); constructed.append(self)
        def store(self):
            self.is_stored = True; registry[self.uuid] = self; stored.append(self.uuid)
            if fault['store']: raise RuntimeError('fault AFTER synthetic store')
            return self
    class Singlefile(Data):
        def __init__(self, file, filename):
            super().__init__(); self.raw = file.read(); self.filename = filename
        def open(self, mode):
            assert mode == 'rb'; return BytesIO(self.raw)
    class Dict(Data):
        def __init__(self, dict):
            super().__init__(); self.base.attributes.set_many(dict)
        def get_dict(self): return dict(self.base.attributes.values)
    class WorkChain:
        process_type = service.PROCESS_TYPE
        def __init__(self, identifier):
            self.uuid = identifier; self.inputs = NS(plan=identifier)
    class Query:
        def append(self, cls, filters=None, tag=None): self.cls=cls; self.filters=filters or {}; return self
        def order_by(self, ordering):
            terms = ordering['card']
            assert terms[-1] == {'uuid': 'asc'}
            if 'attributes.version' in terms[0]:
                # AiiDA psql_dos JSON ordering requires an explicit cast. Bare
                # desc is refused before our numeric sort, as the real owner is.
                assert terms[0] == {'attributes.version': {'order': 'desc', 'cast': 'i'}}
            else:
                assert terms[0] == {'ctime': 'desc'}
            self.ordering=ordering
            return self
        def limit(self, limit): self.maximum=limit; return self
        def offset(self, offset): self.start=offset; return self
        def all(self, flat):
            assert flat
            rows = [n for n in registry.values() if isinstance(n, self.cls) and all(
                n.base.attributes.get(k.removeprefix('attributes.')) == v for k,v in self.filters.items())]
            rows.sort(key=lambda n:n.uuid)
            order = getattr(self,'ordering',{}).get('card', [])
            if order:
                field = next(iter(order[0]))
                rows.sort(key=lambda n: n.base.attributes.get('version') if field=='attributes.version' else n.ctime, reverse=True)
            return rows[getattr(self,'start',0):getattr(self,'start',0)+getattr(self,'maximum',10001)]
    def load(identifier):
        if identifier not in registry: raise NotExistent(identifier)
        return registry[identifier]
    for name,value in [('Data',Data),('SinglefileData',Singlefile),('Dict',Dict),('WorkChainNode',WorkChain),('QueryBuilder',Query),('load_node',load)]:
        monkeypatch.setattr(cards.orm, name, value)
    plans = {R1:{'schema':'qcl-negf-scientific-plan-v2','fingerprint':F1,'root_kind':'study','root_definition_id':'study1',
                  'executions':[{'id':'E1','definition_id':'D1','variant_id':'base'}, {'id':'E2','definition_id':'D2','variant_id':'mesh-refined'}]},
             R2:{'schema':'qcl-negf-scientific-plan-v2','fingerprint':F2,'root_kind':'meta','root_definition_id':'study2',
                  'executions':[{'id':'E3','definition_id':'D3','variant_id':'window-refined'}]}}
    registry[R1]=WorkChain(R1); registry[R2]=WorkChain(R2)
    report = {'schema':'qcl-negf-agent-report-v1',
        'anchor':{'run_uuid':R1,'plan_fingerprint':F1,'root_kind':'study','root_definition_id':'study1'},
        'question_snapshot':'An independent author snapshot',
        'used_runs':[{'run_uuid':R2,'plan_fingerprint':F2,'execution_id':'E3','definition_id':'D3',
                      'variant_id':'window-refined','attempt':2,'calcjob_uuid':CHILD}],
        'conclusion':'Negative result', 'reasoning':'No acceptance inference', 'limitations':'Fake persistence only'}
    def read_plan(identifier): reads.append(identifier); return plans[identifier]
    def read_report(anchor, identifier):
        if (anchor,identifier)!=(R1,REPORT): raise LookupError('wrong immutable report')
        return json.dumps(report, separators=(',',':')).encode()
    def child(node, execution, **kwargs):
        selected.append((node.uuid,execution,kwargs))
        if (node.uuid,execution,kwargs)!=(R2,'E3',{'attempt':2,'calcjob_uuid':CHILD}): raise LookupError('wrong exact child')
        return NS(uuid=CHILD)
    monkeypatch.setattr(service,'read_plan',read_plan)
    monkeypatch.setattr(service,'read_agent_report',read_report)
    monkeypatch.setattr(service,'_select_child',child)
    return NS(registry=registry, constructed=constructed, stored=stored, reads=reads, selected=selected,
              plans=plans, report=report, fault=fault)


def test_distinct_identical_cards_raw_bytes_and_idempotency(source):
    first = cards.create_research_card(request(1),RAW)
    assert first == {'status':'stored','kind':'version','receipt':{
        'card_uuid':C1,'version_uuid':C1,'version':1,'previous_version_uuid':None,'title':'Test',
        'bytes':len(RAW),'sha256':hashlib.sha256(RAW).hexdigest(),'ctime':STAMP}}
    assert cards.get_research_card(C1)=={'head':first['receipt'],'card':CARD}
    assert cards.read_research_card_version(C1,C1,raw=True)==RAW
    assert cards.create_research_card(request(2),RAW)['receipt']['card_uuid']==C2
    assert cards.create_research_card(request(1),RAW)=={**first,'status':'replayed'}
    assert cards.create_research_card(request(1),RAW.replace(b'Test',b'Other'))=={'status':'rejected','reason':'request_conflict'}
    assert len(source.stored)==2
    assert cards.get_research_card_request(request(999))=={'status':'not_found'}


def test_queued_base_conflict_and_old_request_replay_before_head_check(source):
    first=cards.create_research_card(request(1),RAW)
    second=cards.edit_research_card(C1,request(2),C1,RAW+b'\n')
    assert second['receipt']['version']==2 and second['receipt']['previous_version_uuid']==C1
    assert cards.edit_research_card(C1,request(3),C1,RAW)=={
        'status':'rejected','reason':'version_conflict','current_version_uuid':second['receipt']['version_uuid']}
    assert cards.create_research_card(request(1),RAW)=={**first,'status':'replayed'}
    assert cards.edit_research_card(C1,request(2),C1,RAW+b'\n')=={**second,'status':'replayed'}
    assert cards.read_research_card_version(C1,C1,raw=True)==RAW
    assert cards.read_research_card_version(C1,second['receipt']['version_uuid'],raw=True)==RAW+b'\n'
    assert [v['version'] for v in cards.list_research_card_versions(C1)]==[2,1]
    assert len(source.stored)==2


def setup_memberships(source):
    cards.create_research_card(request(1),RAW)
    cards.create_research_card(request(2),RAW)
    second=cards.edit_research_card(C1,request(3),C1,RAW+b'\n')['receipt']['version_uuid']
    one=cards.attach_research_card_run(C1,request(4),C1,R1,F1)
    two=cards.attach_research_card_run(C1,request(5),second,R2,F2)
    return second,one,two


def test_whole_run_cross_version_analysis_literal_membership(source):
    second,one,two=setup_memberships(source)
    assert one['receipt']['run_uuid']==R1 and two['receipt']['card_version_uuid']==second
    before=json.dumps(source.plans,sort_keys=True)
    source.reads.clear()
    result=cards.attach_research_card_report(C1,request(6),second,R1,REPORT)
    assert result['kind']=='analysis' and result['status']=='stored'
    assert result['receipt']['card_version_uuid']==second
    assert result['receipt']['report_sha256']==hashlib.sha256(json.dumps(source.report,separators=(',',':')).encode()).hexdigest()
    assert source.reads==[R1,R2]  # one fresh bounded frozen read per member
    assert source.selected==[(R2,'E3',{'attempt':2,'calcjob_uuid':CHILD})]
    assert json.dumps(source.plans,sort_keys=True)==before  # all E1/base E2/refined preserved
    assert cards.list_research_card_runs(C2)==[]  # equal prose gives no association
    assert cards.get_research_card_request(request(6))=={**result,'status':'stored'}
    assert cards.attach_research_card_report(C1,request(6),second,R1,REPORT)=={**result,'status':'replayed'}


@pytest.mark.parametrize('fault',['membership','fingerprint','execution','definition','variant','attempt','child','anchor','report','version','unsupported'])
def test_wrong_binding_refuses_before_constructor(source,fault):
    second,_,_=setup_memberships(source)
    target,anchor,report=C1,R1,REPORT
    if fault=='membership': target=C2; second=C2
    elif fault=='fingerprint':source.plans[R2]['fingerprint']='c'*64
    elif fault=='execution':source.report['used_runs'][0]['execution_id']='absent'
    elif fault=='definition':source.report['used_runs'][0]['definition_id']='wrong'
    elif fault=='variant':source.report['used_runs'][0]['variant_id']='wrong'
    elif fault=='attempt':source.report['used_runs'][0]['attempt']=1
    elif fault=='child':source.report['used_runs'][0]['calcjob_uuid']=C2
    elif fault=='anchor':source.report['anchor']['root_definition_id']='wrong'
    elif fault=='report':report=C2
    elif fault=='version':second=C2
    elif fault=='unsupported':source.plans[R1]['schema']='historical'
    n=len(source.constructed)
    result=cards.attach_research_card_report(target,request(6),second,anchor,report)
    assert result['status']=='rejected'
    assert result['reason'] in ('invalid_binding','unsupported_plan')
    assert len(source.constructed)==n


def test_run_wrong_fp_historical_and_wrong_process_refuse(source):
    cards.create_research_card(request(1),RAW)
    n=len(source.constructed)
    assert cards.attach_research_card_run(C1,request(2),C1,R1,F2)=={'status':'rejected','reason':'invalid_binding'}
    source.plans[R1]['schema']='old'
    assert cards.attach_research_card_run(C1,request(3),C1,R1,F1)=={'status':'rejected','reason':'unsupported_plan'}
    source.registry[R2]=NS(uuid=R2,process_type='other')
    assert cards.attach_research_card_run(C1,request(4),C1,R2,F2)=={'status':'rejected','reason':'not_found'}
    assert len(source.constructed)==n


@pytest.mark.parametrize('raw',[RAW+b' '*65537,RAW.replace(b'"title":"Test"',b'"title":"Test","title":"Second"'),RAW.replace(b'Test',b'\\ud800'),RAW.replace(b'qcl-negf-research-card-v1',b'unknown')])
def test_bad_raw_before_constructor(source,raw):
    assert cards.create_research_card(request(1),raw)=={'status':'rejected','reason':'invalid_binding'}
    assert source.constructed==[] and source.stored==[]


def test_exact_byte_limit_and_corrupt_native_metadata(source):
    raw=RAW+b' '*(65536-len(RAW))
    cards.create_research_card(request(1),raw)
    assert cards.read_research_card_version(C1,C1,raw=True)==raw
    source.registry[C1].raw=RAW
    with pytest.raises(ValueError): cards.get_research_card(C1)


def test_store_fault_propagates_and_receipt_can_exist(source):
    source.fault['store']=True
    with pytest.raises(RuntimeError,match='AFTER synthetic store'):cards.create_research_card(request(1),RAW)
    assert source.stored==[C1]
    assert cards.get_research_card_request(request(1))['receipt']['version_uuid']==C1
    # No catch converts this post-effect exception into a no-effect rejection.


def test_pagination_uuid_catalogue_cap_and_duplicate_request_corruption(source,monkeypatch):
    cards.create_research_card(request(1),RAW);cards.create_research_card(request(2),RAW)
    assert len(cards.list_research_cards(limit=1,offset=1))==1
    assert cards.list_research_card_versions(C1,limit=1,offset=1)==[]
    for limit,offset in [(True,0),(101,0),(1,-1),(1,False)]:
        with pytest.raises(ValueError):cards.list_research_cards(limit,offset)
    with pytest.raises(ValueError):cards.get_research_card('AAAAAAAA-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    monkeypatch.setattr(cards,'MAX_CATALOG',1)
    with pytest.raises(ValueError,match='capacity'):cards.list_research_cards()
    source.registry[C2].base.attributes.values['request_uuid']=request(1)
    with pytest.raises(ValueError,match='Duplicate request'):cards.get_research_card_request(request(1))


def test_ambiguous_head_wrong_parent_and_response_cap(source,monkeypatch):
    cards.create_research_card(request(1),RAW)
    second=cards.edit_research_card(C1,request(2),C1,RAW)['receipt']['version_uuid']
    source.registry[C1].base.attributes.values['version']=2
    with pytest.raises(ValueError):cards.get_research_card(C1)
    source.registry[C1].base.attributes.values['version']=1
    source.registry[second].base.attributes.values['previous_version_uuid']=V2
    with pytest.raises(LookupError):cards.get_research_card(C1)
    source.registry[second].base.attributes.values['previous_version_uuid']=C1
    monkeypatch.setattr(cards,'MAX_RESPONSE',1)
    with pytest.raises(ValueError,match='1 MiB'):cards.get_research_card(C1)


def test_native_version_order_requires_integer_cast_and_sorts_2_9_10(source):
    cards.create_research_card(request(1), RAW)
    for version in range(2, 11):
        base = cards.get_research_card(C1)['head']['version_uuid']
        result = cards.edit_research_card(C1, request(version), base, RAW)
        assert result['status'] == 'stored'
        assert result['receipt']['version'] == version
    # Literal ordering distinguishes numeric 10 > 9 > 2 from text 9 > 2 > 10.
    assert [r['version'] for r in cards.list_research_card_versions(C1)] == [10,9,8,7,6,5,4,3,2,1]
    assert cards.get_research_card(C1)['head']['version'] == 10
    with pytest.raises(AssertionError):
        cards.orm.QueryBuilder().order_by({'card': [{'attributes.version': 'desc'}, {'uuid': 'asc'}]})
