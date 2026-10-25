"""Append-only native research metadata. No scientific execution or inference.

Called only by the existing loaded-profile service owner. Routine refusals are
returned before any constructor; once storage starts exceptions propagate.
"""
from __future__ import annotations

import hashlib
import io
import json
from uuid import UUID

from aiida import orm
from aiida.common.exceptions import NotExistent
from qcl_negf_contracts.research_cards import validate_research_card

VERSION = 'version-v1'
RUN = 'run-link-v1'
REPORT = 'report-link-v1'
MAX_RESPONSE = 1024 * 1024
MAX_CATALOG = 10000


def _owner():
    # Local import keeps the public service boundary free of circular imports.
    from . import service
    return service


def _uuid(value):
    if type(value) is not str or str(UUID(value)) != value:
        raise ValueError('Canonical UUID required')
    return value


def _hash(value):
    if type(value) is not str or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
        raise ValueError('SHA-256 required')
    return value


def _bounded(value):
    if len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')) > MAX_RESPONSE:
        raise ValueError('Research-card response exceeds 1 MiB')
    return value


def _page(limit, offset):
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or offset < 0:
        raise ValueError('Require 1 <= limit <= 100 and offset >= 0')


def _query(kind, fields=None, *, limit=MAX_CATALOG + 1, offset=0, versions=False):
    filters = {'attributes.qcl_card_kind': kind}
    filters.update(fields or {})
    cls = orm.SinglefileData if kind == VERSION else orm.Dict
    query = orm.QueryBuilder().append(cls, filters=filters, tag='card')
    ordering = {'attributes.version': {'order': 'desc', 'cast': 'i'}} if versions else {'ctime': 'desc'}
    query.order_by({'card': [ordering, {'uuid': 'asc'}]})
    return query.limit(limit).offset(offset).all(flat=True)


def _load(identifier, kind):
    _uuid(identifier)
    try:
        node = orm.load_node(identifier)
    except NotExistent as error:
        raise LookupError('Research metadata not found') from error
    cls = orm.SinglefileData if kind == VERSION else orm.Dict
    if not isinstance(node, cls) or node.base.attributes.get('qcl_card_kind') != kind:
        raise LookupError('Research metadata not found')
    return node


def _version(node, *, body=False):
    a = node.base.attributes
    _uuid(a.get('request_uuid')); _hash(a.get('request_sha256'))
    card = _uuid(a.get('card_uuid'))
    version = a.get('version')
    parent = a.get('previous_version_uuid')
    size = a.get('bytes')
    title = a.get('title')
    if (not isinstance(node, orm.SinglefileData) or a.get('qcl_card_kind') != VERSION
            or node.filename != 'research-card.json' or type(version) is not int or version < 1
            or type(size) is not int or not 0 <= size <= 65536 or type(title) is not str
            or not title.strip() or len(title) > 128
            or (version == 1 and (node.uuid != card or parent is not None))
            or (version > 1 and (_uuid(parent) == node.uuid))):
        raise ValueError('Corrupt research-card version')
    receipt = {'card_uuid': card, 'version_uuid': _uuid(node.uuid), 'version': version,
               'previous_version_uuid': parent, 'title': title, 'bytes': size,
               'sha256': _hash(a.get('sha256')), 'ctime': node.ctime.isoformat()}
    if not body:
        return receipt
    with node.open(mode='rb') as handle:
        raw = handle.read(65537)
    decoded = validate_research_card(raw)
    if len(raw) != size or hashlib.sha256(raw).hexdigest() != receipt['sha256'] or decoded['title'] != title:
        raise ValueError('Research-card bytes differ from immutable metadata')
    return receipt, decoded, raw


def _lineage(node, card_uuid):
    """Verify exact immutable parents to the native v1 root; never ctime."""
    receipt = _version(node)
    if receipt['version'] > MAX_CATALOG:
        raise ValueError('Research-card lineage exceeds bounded capacity')
    while True:
        if receipt['card_uuid'] != card_uuid:
            raise ValueError('Corrupt card lineage identity')
        if receipt['version'] == 1:
            if receipt['version_uuid'] != card_uuid:
                raise ValueError('Corrupt card root')
            return
        parent = _version(_load(receipt['previous_version_uuid'], VERSION))
        if parent['version'] != receipt['version'] - 1:
            raise ValueError('Corrupt card parent')
        receipt = parent


def _head(card_uuid):
    root = _load(card_uuid, VERSION)
    r = _version(root)
    if r['card_uuid'] != card_uuid or r['version'] != 1:
        raise LookupError('Stable card UUID required')
    rows = _query(VERSION, {'attributes.card_uuid': card_uuid}, limit=2, versions=True)
    if not rows:
        raise ValueError('Missing card head')
    head = _version(rows[0])
    if len(rows) > 1 and _version(rows[1])['version'] == head['version']:
        raise ValueError('Ambiguous card head')
    _lineage(rows[0], card_uuid)
    return rows[0]


def _pinned(card_uuid, version_uuid):
    _head(card_uuid)
    node = _load(version_uuid, VERSION)
    receipt, _, _ = _version(node, body=True)
    if receipt['card_uuid'] != card_uuid:
        raise LookupError('Version belongs to another card')
    _lineage(node, card_uuid)
    return node


def _relation(node, kind):
    if not isinstance(node, orm.Dict) or node.base.attributes.get('qcl_card_kind') != kind:
        raise ValueError('Corrupt card relation')
    values = node.get_dict()
    keys = ({'card_uuid', 'card_version_uuid', 'run_uuid', 'plan_fingerprint', 'root_kind', 'root_definition_id'}
            if kind == RUN else {'card_uuid', 'card_version_uuid', 'anchor_run_uuid', 'report_uuid', 'report_sha256'})
    if set(values) != keys | {'qcl_card_kind', 'request_uuid', 'request_sha256'}:
        raise ValueError('Corrupt card relation fields')
    data = {key: values[key] for key in keys}
    if any(type(v) is not str for v in data.values()):
        raise ValueError('Corrupt card relation values')
    _uuid(values['request_uuid']); _hash(values['request_sha256'])
    for key in keys:
        if key.endswith('_uuid'):
            _uuid(data[key])
    if kind == RUN:
        _hash(data['plan_fingerprint'])
        if data['root_kind'] not in ('study', 'meta') or not _owner().IDENTIFIER.fullmatch(data['root_definition_id']):
            raise ValueError('Corrupt run binding')
    else:
        _hash(data['report_sha256'])
    return {'association_uuid': _uuid(node.uuid), **data, 'ctime': node.ctime.isoformat()}


def _receipt(node):
    kind = node.base.attributes.get('qcl_card_kind')
    if kind == VERSION:
        receipt, _, _ = _version(node, body=True)
        return {'kind': 'version', 'receipt': receipt}
    if kind not in (RUN, REPORT):
        raise ValueError('Corrupt request kind')
    return {'kind': 'run' if kind == RUN else 'analysis', 'receipt': _relation(node, kind)}


def _request(request_uuid):
    _uuid(request_uuid)
    # Native immutable nodes themselves are the request evidence, no ledger.
    query = orm.QueryBuilder().append(orm.Data, filters={'attributes.request_uuid': request_uuid})
    rows = query.limit(2).all(flat=True)
    if len(rows) > 1:
        raise ValueError('Duplicate request identity')
    return rows[0] if rows else None


def _meaning(operation, **fields):
    return hashlib.sha256(json.dumps({'operation': operation, **fields}, sort_keys=True,
                                    separators=(',', ':'), ensure_ascii=False).encode('utf-8')).hexdigest()


def _replay(request_uuid, digest):
    node = _request(request_uuid)
    if node is None:
        return None
    if node.base.attributes.get('request_sha256') != digest:
        return {'status': 'rejected', 'reason': 'request_conflict'}
    return {'status': 'replayed', **_receipt(node)}


def _rejected(reason, **fields):
    return {'status': 'rejected', 'reason': reason, **fields}


def _store(node, kind, request_uuid, digest, attrs=None):
    node.base.attributes.set_many({'qcl_card_kind': kind, 'request_uuid': request_uuid,
                                   'request_sha256': digest, **(attrs or {})})
    node.store()  # Never catch an exception at or beyond this effect boundary.
    return {'status': 'stored', **_receipt(node)}


def _raw(raw):
    raw = raw.encode('utf-8', 'strict') if isinstance(raw, str) else raw
    return raw, validate_research_card(raw)


def create_research_card(request_uuid, raw):
    try:
        _uuid(request_uuid)
        raw, card = _raw(raw)
    except (ValueError, UnicodeError, TypeError):
        return _rejected('invalid_binding')
    digest = _meaning('create', raw_sha256=hashlib.sha256(raw).hexdigest())
    replay = _replay(request_uuid, digest)
    if replay is not None:
        return replay
    node = orm.SinglefileData(file=io.BytesIO(raw), filename='research-card.json')
    return _store(node, VERSION, request_uuid, digest,
                  {'card_uuid': node.uuid, 'version': 1, 'previous_version_uuid': None,
                   'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'title': card['title']})


def edit_research_card(card_uuid, request_uuid, base_version_uuid, raw):
    try:
        _uuid(card_uuid); _uuid(base_version_uuid); _uuid(request_uuid)
        raw, card = _raw(raw)
    except (ValueError, UnicodeError, TypeError):
        return _rejected('invalid_binding')
    digest = _meaning('edit', card_uuid=card_uuid, base_version_uuid=base_version_uuid,
                      raw_sha256=hashlib.sha256(raw).hexdigest())
    replay = _replay(request_uuid, digest)
    if replay is not None:
        return replay
    try:
        head = _head(card_uuid)
        _pinned(card_uuid, base_version_uuid)
    except LookupError:
        return _rejected('not_found')
    if head.uuid != base_version_uuid:
        return _rejected('version_conflict', current_version_uuid=head.uuid)
    node = orm.SinglefileData(file=io.BytesIO(raw), filename='research-card.json')
    return _store(node, VERSION, request_uuid, digest,
                  {'card_uuid': card_uuid, 'version': _version(head)['version'] + 1,
                   'previous_version_uuid': base_version_uuid, 'bytes': len(raw),
                   'sha256': hashlib.sha256(raw).hexdigest(), 'title': card['title']})


def list_research_cards(limit=20, offset=0):
    _page(limit, offset)
    rows = _query(VERSION)
    if len(rows) > MAX_CATALOG:
        raise ValueError('Research-card catalogue exceeds bounded listing capacity')
    heads = {}
    for node in rows:
        r = _version(node)
        if r['card_uuid'] not in heads:
            heads[r['card_uuid']] = _version(_head(r['card_uuid']))
    ordered = sorted(heads.values(), key=lambda r: r['version_uuid'])
    ordered.sort(key=lambda r: r['ctime'], reverse=True)
    return _bounded(ordered[offset:offset + limit])


def get_research_card(card_uuid):
    receipt, card, _ = _version(_head(_uuid(card_uuid)), body=True)
    return _bounded({'head': receipt, 'card': card})


def list_research_card_versions(card_uuid, limit=20, offset=0):
    _page(limit, offset); _head(_uuid(card_uuid))
    return _bounded([_version(n) for n in _query(VERSION, {'attributes.card_uuid': card_uuid},
                      limit=limit, offset=offset, versions=True)])


def read_research_card_version(card_uuid, version_uuid, raw=False):
    receipt, card, content = _version(_pinned(_uuid(card_uuid), _uuid(version_uuid)), body=True)
    return content if raw else _bounded({'version': receipt, 'card': card})


def _frozen(identifier, cache):
    if identifier not in cache:
        owner = _owner()
        node = owner._node(_uuid(identifier))
        plan = owner.read_plan(node.inputs.plan)
        try:
            if not isinstance(plan, dict) or plan.get('schema') != 'qcl-negf-scientific-plan-v2':
                raise UnsupportedPlan()
            _hash(plan['fingerprint'])
            if (plan.get('root_kind') not in ('study', 'meta')
                    or not isinstance(plan.get('root_definition_id'), str)
                    or not owner.IDENTIFIER.fullmatch(plan['root_definition_id'])):
                raise UnsupportedPlan()
            executions = plan['executions']
            if not isinstance(executions, list) or any(
                    not isinstance(row, dict) or any(not isinstance(row.get(key), str)
                        or not owner.IDENTIFIER.fullmatch(row[key])
                        for key in ('id', 'definition_id', 'variant_id')) for row in executions):
                raise UnsupportedPlan()
            if len({row['id'] for row in executions}) != len(executions):
                raise UnsupportedPlan()
        except (KeyError, TypeError, ValueError) as error:
            raise UnsupportedPlan() from error
        cache[identifier] = node, plan
    return cache[identifier]


class UnsupportedPlan(Exception):
    """Historical plan cannot establish the required frozen association."""


def attach_research_card_run(card_uuid, request_uuid, card_version_uuid, run_uuid, plan_fingerprint):
    try:
        for value in (card_uuid, card_version_uuid, run_uuid, request_uuid):
            _uuid(value)
        _hash(plan_fingerprint)
    except (ValueError, TypeError):
        return _rejected('invalid_binding')
    digest = _meaning('attach_run', card_uuid=card_uuid, card_version_uuid=card_version_uuid,
                      run_uuid=run_uuid, plan_fingerprint=plan_fingerprint)
    replay = _replay(request_uuid, digest)
    if replay is not None:
        return replay
    try:
        _pinned(card_uuid, card_version_uuid)
        _, plan = _frozen(run_uuid, {})
    except LookupError:
        return _rejected('not_found')
    except UnsupportedPlan:
        return _rejected('unsupported_plan')
    if plan['fingerprint'] != plan_fingerprint:
        return _rejected('invalid_binding')
    fields = {'card_uuid': card_uuid, 'card_version_uuid': card_version_uuid,
              'run_uuid': run_uuid, 'plan_fingerprint': plan_fingerprint,
              'root_kind': plan['root_kind'], 'root_definition_id': plan['root_definition_id']}
    # Attributes expose exact association keys to bounded native queries.
    node = orm.Dict(dict=fields)
    return _store(node, RUN, request_uuid, digest, {'card_uuid': card_uuid, 'run_uuid': run_uuid,
                                                   'plan_fingerprint': plan_fingerprint})


def _member(card_uuid, identifier, plan):
    rows = _query(RUN, {'attributes.card_uuid': card_uuid, 'attributes.run_uuid': identifier,
                        'attributes.plan_fingerprint': plan['fingerprint']}, limit=1)
    if not rows:
        raise LookupError('Missing explicit run membership')
    r = _relation(rows[0], RUN)
    if (r['card_uuid'], r['run_uuid'], r['plan_fingerprint'], r['root_kind'], r['root_definition_id']) != (
            card_uuid, identifier, plan['fingerprint'], plan['root_kind'], plan['root_definition_id']):
        raise ValueError('Corrupt run membership')
    _pinned(card_uuid, r['card_version_uuid'])


def attach_research_card_report(card_uuid, request_uuid, card_version_uuid, anchor_run_uuid, report_uuid):
    try:
        for value in (card_uuid, card_version_uuid, anchor_run_uuid, report_uuid, request_uuid):
            _uuid(value)
    except (ValueError, TypeError):
        return _rejected('invalid_binding')
    digest = _meaning('attach_report', card_uuid=card_uuid, card_version_uuid=card_version_uuid,
                      anchor_run_uuid=anchor_run_uuid, report_uuid=report_uuid)
    replay = _replay(request_uuid, digest)
    if replay is not None:
        return replay
    owner = _owner()
    try:
        _pinned(card_uuid, card_version_uuid)
        raw = owner.read_agent_report(anchor_run_uuid, report_uuid)
        report = owner.validate_agent_report(raw)
        roots = {}
        anchor, plan = _frozen(anchor_run_uuid, roots)
        expected = {'run_uuid': anchor.uuid, 'root_definition_id': plan['root_definition_id'],
                    'root_kind': plan['root_kind'], 'plan_fingerprint': plan['fingerprint']}
        if report['anchor'] != expected:
            return _rejected('invalid_binding')
        _member(card_uuid, anchor_run_uuid, plan)
        # Reader verifies immutable bytes; these fresh admissions verify CURRENT
        # frozen plans and exact child selectors as save_agent_report does.
        for ref in report['used_runs']:
            node, plan = _frozen(ref['run_uuid'], roots)
            _member(card_uuid, ref['run_uuid'], plan)
            executions = [e for e in plan['executions'] if e['id'] == ref['execution_id']]
            if (ref['plan_fingerprint'] != plan['fingerprint'] or len(executions) != 1
                    or executions[0]['definition_id'] != ref['definition_id']
                    or executions[0]['variant_id'] != ref['variant_id']):
                return _rejected('invalid_binding')
            owner._select_child(node, ref['execution_id'], attempt=ref['attempt'], calcjob_uuid=ref['calcjob_uuid'])
    except UnsupportedPlan:
        return _rejected('unsupported_plan')
    except (LookupError, ValueError):
        return _rejected('invalid_binding')
    fields = {'card_uuid': card_uuid, 'card_version_uuid': card_version_uuid,
              'anchor_run_uuid': anchor_run_uuid, 'report_uuid': report_uuid,
              'report_sha256': hashlib.sha256(raw).hexdigest()}
    node = orm.Dict(dict=fields)
    return _store(node, REPORT, request_uuid, digest, {'card_uuid': card_uuid})


def _relations(kind, card_uuid, limit, offset):
    _page(limit, offset); _head(_uuid(card_uuid))
    return _bounded([_relation(n, kind) for n in _query(kind, {'attributes.card_uuid': card_uuid},
                                                      limit=limit, offset=offset)])


def list_research_card_runs(card_uuid, limit=20, offset=0):
    return _relations(RUN, card_uuid, limit, offset)


def list_research_card_reports(card_uuid, limit=20, offset=0):
    return _relations(REPORT, card_uuid, limit, offset)


def get_research_card_request(request_uuid):
    node = _request(request_uuid)
    return {'status': 'not_found'} if node is None else {'status': 'stored', **_receipt(node)}
