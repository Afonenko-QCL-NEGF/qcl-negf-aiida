"""Detached read projections for the explicitly supported AiiDA 2.9.2 PostgreSQL schema.

The public backend session supplies an engine only. A separate read-only connection
avoids committing, flushing, or borrowing the actor's ambient ORM transaction.
"""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime
from uuid import UUID, uuid4

import aiida
from aiida import orm
from aiida.manage import get_manager
from aiida.storage.psql_dos.backend import PsqlDosBackend
from sqlalchemy import text

FORMAT = "qcl-negf.aiida-run-projection.v1"
MAX_SNAPSHOT_BYTES = 32 * 1024 * 1024
MAX_GRAPH_NODES = 10000
MAX_GRAPH_LINKS = 40000
PROCESS_TYPE = "aiida.workflows:qcl_negf.plan"

# Schema-specific SQL is deliberately isolated here, not hidden in QueryBuilder.
# UNION deduplicates the CALL closure; CREATE/INPUT provenance is not traversed.
SNAPSHOT_SQL = """WITH RECURSIVE calls(id) AS (
 SELECT id FROM db_dbnode WHERE uuid=CAST(:run_uuid AS uuid)
 UNION
 SELECT l.output_id FROM db_dblink l JOIN calls c ON l.input_id=c.id
 WHERE l.type IN ('call_calc','call_work')
), bounded_nodes AS MATERIALIZED (
 SELECT n.* FROM calls c JOIN db_dbnode n ON n.id=c.id LIMIT :node_limit
), bounded_links AS MATERIALIZED (
 SELECT l.*, i.uuid AS input_uuid, o.uuid AS output_uuid,
 CASE WHEN l.type='input_calc' THEN i.attributes->'value' ELSE NULL END AS value,
 CASE WHEN l.type='return' THEN jsonb_build_object(
 'execution_id',o.attributes->'execution_id','attempt',o.attributes->'attempt',
 'calcjob_uuid',o.attributes->'calcjob_uuid') ELSE NULL END AS selection
 FROM db_dblink l JOIN db_dbnode i ON i.id=l.input_id
 JOIN db_dbnode o ON o.id=l.output_id
 WHERE (l.type IN ('call_calc','call_work') AND l.input_id IN (SELECT id FROM bounded_nodes))
 OR (l.type='input_work' AND o.uuid=CAST(:run_uuid AS uuid) AND l.label='plan')
 OR (l.type='return' AND i.uuid=CAST(:run_uuid AS uuid) AND l.label LIKE 'selections\\_\\_%' ESCAPE '\\')
 OR (l.type='input_calc' AND o.id IN (SELECT id FROM bounded_nodes WHERE node_type='process.calculation.calcjob.CalcJobNode.')
     AND l.label IN ('execution_id','attempt'))
 OR (l.type='create' AND i.id IN (SELECT id FROM bounded_nodes WHERE node_type='process.calculation.calcjob.CalcJobNode.')
     AND l.label IN ('retrieved','result'))
 LIMIT :link_limit
), rows AS MATERIALIZED (
 SELECT 'node' AS kind,n.id AS ordering,jsonb_build_object(
 'pk',n.id,'uuid',n.uuid,'node_type',n.node_type,'process_type',n.process_type,
 'label',n.label,'ctime',n.ctime,'mtime',n.mtime,
 'process_state',n.attributes->'process_state','exit_status',n.attributes->'exit_status') AS payload
 FROM bounded_nodes n
 UNION ALL
 SELECT 'link',l.id,jsonb_build_object('input_uuid',l.input_uuid,'output_uuid',l.output_uuid,
 'label',l.label,'type',l.type,'value',l.value,'selection',l.selection) FROM bounded_links l
), sizes AS (
 SELECT COALESCE(sum(octet_length(jsonb_build_object('kind',kind,'payload',payload)::text) + 2),0) + 2 AS bytes FROM rows
)
SELECT CASE WHEN (SELECT count(*) FROM alembic_version)!=1
 OR (SELECT count(*) FROM alembic_version WHERE version_num=:schema_version)!=1 THEN 'unsupported_schema'
 WHEN (SELECT count(*) FROM bounded_nodes)>=:node_limit
 OR (SELECT count(*) FROM bounded_links)>=:link_limit THEN 'graph_limit'
 WHEN sizes.bytes>:byte_limit THEN 'snapshot_limit' ELSE NULL END AS error,
 CASE WHEN (SELECT count(*) FROM alembic_version)=1
 AND (SELECT count(*) FROM alembic_version WHERE version_num=:schema_version)=1
 AND (SELECT count(*) FROM bounded_nodes)<:node_limit
 AND (SELECT count(*) FROM bounded_links)<:link_limit AND sizes.bytes<=:byte_limit
 THEN (SELECT COALESCE(jsonb_agg(jsonb_build_object('kind',kind,'payload',payload)
 ORDER BY kind,ordering),'[]'::jsonb) FROM rows) ELSE NULL END AS rows
FROM sizes"""


class ProjectionUnavailable(RuntimeError):
    """A current authoritative snapshot could not be obtained."""


class PayloadCache:
    """Bounded detached immutable payloads, owned by one backend/actor scope."""

    def __init__(self, max_entries=32, max_bytes=64 * 1024 * 1024):
        self.max_entries, self.max_bytes = max_entries, max_bytes
        self.entries = OrderedDict()
        self.bytes = 0

    def read(self, kind, uuid, loader):
        key = (kind, uuid)
        if key in self.entries:
            raw = self.entries.pop(key)
            self.entries[key] = raw
            return json.loads(raw)
        value = loader()
        raw = json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode()
        if len(raw) <= self.max_bytes and self.max_entries > 0:
            while self.entries and (
                len(self.entries) >= self.max_entries
                or self.bytes + len(raw) > self.max_bytes
            ):
                _, removed = self.entries.popitem(last=False)
                self.bytes -= len(removed)
            self.entries[key] = raw
            self.bytes += len(raw)
        return deepcopy(value)


_backend = None
_scope = None
_payloads = PayloadCache()


def current_backend():
    manager = get_manager()
    if manager.get_profile() is None:
        return None
    return manager.get_profile_storage()


def supports_backend(backend):
    return backend is not None and backend.profile.storage_backend == "core.psql_dos"


def _cache_scope(backend):
    global _backend, _scope, _payloads
    identity = (backend.profile.uuid, backend.profile.storage_backend)
    if backend is not _backend or _scope is None or _scope[0] != identity:
        _backend, _scope, _payloads = backend, (identity, str(uuid4())), PayloadCache()
    return _scope, _payloads


@contextmanager
def payload_reader(actor):
    """Lazily load immutable Data in a separate read-only backend for one detail call.

    The actor's session is never queried by the loader. QueryBuilder.first is
    deliberate: the AiiDA iterators used by one/all can enter backend.transaction.
    Warm cache hits create no reader. All ORM/repository handles stay in this scope.
    """
    reader = session = None

    def read(uuid, consume):
        nonlocal reader, session
        if reader is None:
            reader = PsqlDosBackend(actor.profile)
            session = reader.get_session()
            session.connection(
                execution_options={
                    "isolation_level": "READ COMMITTED",
                    "postgresql_readonly": True,
                }
            )
            session.execute(text("SET LOCAL statement_timeout = '5000ms'"))
        node = (
            orm.QueryBuilder(backend=reader)
            .append(orm.SinglefileData, filters={"uuid": uuid}, project="*")
            .first(flat=True)
        )
        if node is None:
            raise LookupError("Immutable run payload is unavailable")
        return consume(node)

    try:
        yield read
    finally:
        try:
            if session is not None:
                session.close()
        finally:
            if reader is not None:
                reader.close()


def _summary(row):
    summary = {
        key: row[key]
        for key in (
            "uuid",
            "pk",
            "label",
            "process_state",
            "exit_status",
            "ctime",
            "mtime",
        )
    }
    # PostgreSQL JSON trims trailing fractional zeroes; keep the existing Python
    # datetime.isoformat() representation used by the public detail response.
    for key in ("ctime", "mtime"):
        summary[key] = datetime.fromisoformat(summary[key]).isoformat()
    return summary | {
        "process_state": row["process_state"] or "created",
        "is_finished_ok": row["process_state"] == "finished"
        and row["exit_status"] == 0,
    }


def read_snapshot(identifier):
    try:
        identifier = str(UUID(identifier))
    except (ValueError, TypeError, AttributeError) as exception:
        raise ValueError("A full process UUID is required") from exception
    try:
        backend = current_backend()
    except Exception as exception:
        raise ProjectionUnavailable("snapshot_failed") from exception
    if backend is None:
        raise ProjectionUnavailable("no_profile")
    if not supports_backend(backend):
        raise ProjectionUnavailable("unsupported_backend")
    if aiida.__version__ != "2.9.2":
        raise ProjectionUnavailable("unsupported_aiida_version")
    scope, cache = _cache_scope(backend)
    try:
        engine = backend.get_session().get_bind()
        with engine.connect().execution_options(
            isolation_level="READ COMMITTED", postgresql_readonly=True
        ) as connection:
            # Local transaction setting protects this metadata query, not a scientific campaign.
            connection.execute(text("SET LOCAL statement_timeout = '5000ms'"))
            record = (
                connection.execute(
                    text(SNAPSHOT_SQL),
                    {
                        "run_uuid": identifier,
                        "node_limit": MAX_GRAPH_NODES + 1,
                        "link_limit": MAX_GRAPH_LINKS + 1,
                        "byte_limit": MAX_SNAPSHOT_BYTES - 1024,
                        "schema_version": "main_0002",
                    },
                )
                .mappings()
                .one()
            )
    except Exception as exception:
        raise ProjectionUnavailable("snapshot_failed") from exception
    if record["error"]:
        raise ProjectionUnavailable(record["error"])
    return _snapshot_details(identifier, record["rows"], scope, cache)


def _snapshot_details(identifier, rows, scope, cache):
    if len(json.dumps(rows, ensure_ascii=False).encode()) > MAX_SNAPSHOT_BYTES:
        raise ProjectionUnavailable("snapshot_limit")
    nodes = [r["payload"] for r in rows if r["kind"] == "node"]
    root = next((r for r in nodes if r["uuid"] == identifier), None)
    if (
        root is None
        or root["node_type"] != "process.workflow.workchain.WorkChainNode."
        or root["process_type"] != PROCESS_TYPE
    ):
        raise LookupError("Run not found")
    inputs, outputs, selections = {}, {}, []
    for row in rows:
        if row["kind"] != "link":
            continue
        link = row["payload"]
        kind, label = link["type"], link["label"]
        source, target = link["input_uuid"], link["output_uuid"]
        if kind.startswith("input_"):
            key = (target, label)
            if key in inputs:
                raise ProjectionUnavailable("invalid_links")
            inputs[key] = link
        elif kind == "create":
            by_label = outputs.setdefault(source, {})
            if label in by_label:
                raise ProjectionUnavailable("invalid_links")
            by_label[label] = target
        elif (
            kind == "return"
            and source == identifier
            and label.startswith("selections__")
        ):
            selections.append(link["selection"])
    children = []
    for row in sorted(nodes, key=lambda r: (r["ctime"], r["pk"])):
        if row["node_type"] != "process.calculation.calcjob.CalcJobNode.":
            continue
        execution, attempt = (
            inputs.get((row["uuid"], "execution_id")),
            inputs.get((row["uuid"], "attempt")),
        )
        child_outputs = outputs.get(row["uuid"], {})
        if execution is None or not isinstance(execution["value"], str):
            raise ProjectionUnavailable("invalid_inputs")
        children.append(
            _summary(row)
            | {
                "execution_id": execution["value"],
                "attempt": attempt["value"] if attempt is not None else 1,
                "retrieved_uuid": child_outputs.get("retrieved"),
                "result_uuid": child_outputs.get("result"),
            }
        )
    plan_link = inputs.get((identifier, "plan"))
    stamp = json.dumps(
        [FORMAT, scope, rows], sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return {
        "revision": hashlib.sha256(stamp).hexdigest(),
        "summary": _summary(root),
        "children": children,
        "plan_uuid": plan_link["input_uuid"] if plan_link else None,
        "selections": selections,
        "cache": cache,
    }


def choose_child(
    children, selections, execution_id, *, attempt=None, calcjob_uuid=None
):
    if attempt is None and calcjob_uuid is None:
        chosen = [
            item for item in selections if item.get("execution_id") == execution_id
        ]
        if len(chosen) > 1:
            raise ValueError("Conflicting published attempt selections")
        if chosen:
            attempt, calcjob_uuid = chosen[0]["attempt"], chosen[0]["calcjob_uuid"]
    candidates = [child for child in children if child["execution_id"] == execution_id]
    if attempt is not None:
        if type(attempt) is not int or attempt < 1:
            raise ValueError("Require a positive attempt")
        candidates = [child for child in candidates if child["attempt"] == attempt]
    if calcjob_uuid is not None:
        candidates = [child for child in candidates if child["uuid"] == calcjob_uuid]
    if len(candidates) > 1:
        raise ValueError(
            "Multiple attempts exist; select an exact attempt or CalcJob UUID"
        )
    if not candidates:
        raise LookupError("Selected attempt is unavailable")
    return candidates[0]


def selected_children(children, selections):
    """Resolve each execution once; all scans together are linear in metadata size."""
    groups, publications = {}, {}
    for child in children:
        groups.setdefault(child["execution_id"], []).append(child)
    for selection in selections:
        execution_id = selection.get("execution_id")
        if isinstance(execution_id, str):
            publications.setdefault(execution_id, []).append(selection)
    selected = {}
    for execution_id, group in groups.items():
        if not any(child["result_uuid"] is not None for child in group):
            continue
        try:
            selected[execution_id] = choose_child(
                group, publications.get(execution_id, []), execution_id
            )
        except ValueError, LookupError:
            pass
    return selected


def projection(identifier, known_revision=None, *, maximum=MAX_SNAPSHOT_BYTES):
    response = {
        "format": FORMAT,
        "run_uuid": identifier,
        "revision": None,
        "state": "unavailable",
        "reason": None,
        "summary": None,
        "children": None,
    }
    try:
        snapshot = read_snapshot(identifier)
    except ProjectionUnavailable as exception:
        response["reason"] = str(exception)
    else:
        response["revision"] = snapshot["revision"]
        if known_revision == snapshot["revision"]:
            response["state"] = "unchanged"
        else:
            response.update(
                state="available",
                summary=snapshot["summary"],
                children=[
                    {k: v for k, v in child.items() if k != "result_uuid"}
                    for child in snapshot["children"]
                ],
            )
    if len(json.dumps(response, ensure_ascii=False).encode()) > maximum:
        response.update(
            state="unavailable",
            reason="response_limit",
            revision=None,
            summary=None,
            children=None,
        )
        if len(json.dumps(response, ensure_ascii=False).encode()) > maximum:
            raise ValueError(
                "Run metadata exceeds the service response limit; use AiiDA directly"
            )
    return response
