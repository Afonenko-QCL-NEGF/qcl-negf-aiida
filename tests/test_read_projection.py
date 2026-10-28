"""Independent scalar projection oracles; native backend tests are opt-in."""

from copy import deepcopy
from types import SimpleNamespace as NS

import pytest
from aiida_qcl_negf import service

RUN = "00000000-0000-0000-0000-000000000001"
CHILD = "00000000-0000-0000-0000-000000000002"


def test_projection_unavailable_is_explicit_without_profile(monkeypatch):
    from aiida_qcl_negf import read_projection as projection

    monkeypatch.setattr(projection, "current_backend", lambda: None)
    assert service.get_run_projection(RUN) == {
        "format": "qcl-negf.aiida-run-projection.v1",
        "run_uuid": RUN,
        "revision": None,
        "state": "unavailable",
        "reason": "no_profile",
        "summary": None,
        "children": None,
    }


def test_snapshot_projection_is_detached_and_failure_never_hits_cache(monkeypatch):
    from aiida_qcl_negf import read_projection as projection

    snapshot = {
        "revision": "rev1",
        "summary": {"uuid": RUN, "exit_status": None},
        "children": [{"uuid": CHILD, "execution_id": "e1", "attempt": 1}],
    }
    monkeypatch.setattr(projection, "read_snapshot", lambda _: deepcopy(snapshot))
    actual = service.get_run_projection(RUN)
    assert actual["state"] == "available" and actual["children"] == snapshot["children"]
    actual["children"][0]["attempt"] = 999
    assert (
        service.get_run_projection(RUN, known_revision="rev1")["state"] == "unchanged"
    )
    assert service.get_run_projection(RUN)["children"][0]["attempt"] == 1

    def failure(_):
        raise projection.ProjectionUnavailable("snapshot_failed")

    monkeypatch.setattr(projection, "read_snapshot", failure)
    unavailable = service.get_run_projection(RUN, known_revision="rev1")
    assert unavailable["state"] == "unavailable" and unavailable["revision"] is None


@pytest.mark.parametrize("attempt", [True, 0, -1, "1"])
def test_selector_rejects_invalid_explicit_attempt(attempt):
    from aiida_qcl_negf.read_projection import choose_child

    with pytest.raises(ValueError, match="positive attempt"):
        choose_child(
            [{"uuid": CHILD, "execution_id": "e1", "attempt": 1}],
            [],
            "e1",
            attempt=attempt,
        )


def test_selector_literal_ambiguity_and_publication():
    from aiida_qcl_negf.read_projection import choose_child

    one = {"uuid": CHILD, "execution_id": "e1", "attempt": 1}
    two = {"uuid": "second", "execution_id": "e1", "attempt": 2}
    assert choose_child([one], [], "e1") == one
    with pytest.raises(ValueError, match="Multiple attempts"):
        choose_child([one, two], [], "e1")
    selected = {"execution_id": "e1", "attempt": 2, "calcjob_uuid": "second"}
    assert choose_child([one, two], [selected], "e1") == two
    with pytest.raises(ValueError, match="Conflicting published"):
        choose_child([one, two], [selected, selected], "e1")
    with pytest.raises(LookupError):
        choose_child([one, two], [], "e1", attempt=1, calcjob_uuid="second")


def test_payload_cache_detachment_eviction_and_scope():
    from aiida_qcl_negf.read_projection import PayloadCache

    cache = PayloadCache(max_entries=1, max_bytes=100)
    calls = []

    def load():
        calls.append(1)
        return {"nested": [1]}

    a = cache.read("plan", "uuid1", load)
    a["nested"].append(99)
    assert cache.read("plan", "uuid1", load) == {"nested": [1]} and len(calls) == 1
    cache.read("plan", "uuid2", load)
    cache.read("plan", "uuid1", load)
    assert len(calls) == 3
    cache = PayloadCache(max_entries=1, max_bytes=1)
    cache.read("plan", "uuid1", load)
    cache.read("plan", "uuid1", load)
    assert len(calls) == 5


@pytest.fixture
def postgres_graph(aiida_config_factory, aiida_profile_factory, tmp_path, monkeypatch):
    import io
    import json
    import os
    from uuid import uuid4

    import psycopg
    from aiida import orm
    from aiida.common.links import LinkType
    from aiida.manage import get_manager
    from psycopg import sql

    socket = os.environ.get("QCL_P08_POSTGRES_SOCKET")
    if not socket:
        pytest.skip("Explicit disposable PostgreSQL socket required")
    port = int(os.environ.get("QCL_P08_POSTGRES_PORT", "55488"))
    monkeypatch.setenv("PGHOST", socket)
    monkeypatch.setenv("PGPORT", str(port))
    database = "qcl_p08_" + uuid4().hex
    with psycopg.connect(
        host=socket, port=port, user="tolya", dbname="postgres", autocommit=True
    ) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    with (
        aiida_config_factory(tmp_path) as config,
        aiida_profile_factory(
            config,
            name="p08-" + uuid4().hex,
            storage_backend="core.psql_dos",
            storage_config={
                "database_hostname": "",
                "database_port": port,
                "database_username": "tolya",
                "database_password": "",
                "database_name": database,
                "repository_uri": (tmp_path / "repository").as_uri(),
            },
        ) as profile,
    ):
        plan = orm.SinglefileData(
            file=io.BytesIO(
                json.dumps(
                    {"schema": "synthetic", "fingerprint": "f" * 64, "points": []}
                ).encode()
            ),
            filename="plan.json",
        ).store()
        root = orm.WorkChainNode(process_type="aiida.workflows:qcl_negf.plan")
        root.base.links.add_incoming(
            plan, link_type=LinkType.INPUT_WORK, link_label="plan"
        )
        root.set_process_state("running")
        root.store()
        parent = root
        for index in range(4):
            work = orm.WorkChainNode()
            work.base.links.add_incoming(
                parent, link_type=LinkType.CALL_WORK, link_label=f"work_{index}"
            )
            parent = work.store()

        def add_calc(attempt=1):
            calc = orm.CalcJobNode()
            calc.base.links.add_incoming(
                parent, link_type=LinkType.CALL_CALC, link_label=f"calc_{attempt}"
            )
            calc.base.links.add_incoming(
                orm.Str("e1").store(),
                link_type=LinkType.INPUT_CALC,
                link_label="execution_id",
            )
            if attempt is not None:
                calc.base.links.add_incoming(
                    orm.Int(attempt).store(),
                    link_type=LinkType.INPUT_CALC,
                    link_label="attempt",
                )
            calc.set_process_state("running")
            return calc.store()

        calc = add_calc()
        backend = get_manager().get_profile_storage()
        try:
            yield NS(
                root=root,
                calc=calc,
                backend=backend,
                engine=backend.get_session().get_bind(),
                add_calc=add_calc,
                profile=profile,
                database=database,
            )
        finally:
            # Return checked-out ORM connections before AiiDA disposes its pool.
            backend.get_session().close()
            backend.close()
    # Only this fixture-created synthetic database is removed, never service lifecycle.
    with psycopg.connect(
        host=socket, port=port, user="tolya", dbname="postgres", autocommit=True
    ) as admin:
        admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(database)))


@pytest.mark.integration
def test_postgres_ten_polls_and_committed_transitions(
    postgres_graph, monkeypatch, record_property
):
    import io
    import json
    import time

    from aiida import orm
    from aiida.common.links import LinkType
    from sqlalchemy import event

    graph = postgres_graph
    reads = []
    original_read = service.read_plan
    structures = []
    original_structure = service.project_run_structure
    monkeypatch.setattr(
        service,
        "project_run_structure",
        lambda plan: (structures.append(1), original_structure(plan))[1],
    )
    result_reads = []
    original_result = service.read_json
    monkeypatch.setattr(
        service,
        "read_json",
        lambda node, **kw: (
            result_reads.append(node.uuid),
            original_result(node, **kw),
        )[1],
    )
    monkeypatch.setattr(
        service,
        "read_plan",
        lambda node: (reads.append(node.uuid), original_read(node))[1],
    )

    def forbidden(*args, **kwargs):
        pytest.fail("Forbidden full ORM traversal/scientific action")

    monkeypatch.setattr(orm.ProcessNode, "called_descendants", property(forbidden))
    monkeypatch.setattr(service, "_children", forbidden)
    monkeypatch.setattr(service, "submit", forbidden)
    monkeypatch.setattr(service, "open_artifact", forbidden)
    queries = []

    def count(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("WITH RECURSIVE calls"):
            queries.append(statement)

    event.listen(graph.engine, "before_cursor_execute", count)
    try:
        detail = service.get_run(graph.root.uuid)
        assert detail["children"] == [
            {
                "uuid": graph.calc.uuid,
                "pk": graph.calc.pk,
                "label": "",
                "process_state": "running",
                "exit_status": None,
                "is_finished_ok": False,
                "ctime": graph.calc.ctime.isoformat(),
                "mtime": graph.calc.mtime.isoformat(),
                "execution_id": "e1",
                "attempt": 1,
                "retrieved_uuid": None,
            }
        ]
        with pytest.raises(LookupError, match="Run not found"):
            service.get_run_projection(RUN)
        first = service.get_run_projection(graph.root.uuid)
        assert (
            first["summary"]["process_state"] == "running"
            and first["children"][0]["exit_status"] is None
        )
        before = len(queries)
        started = time.perf_counter()
        for _ in range(10):
            assert (
                service.get_run_projection(
                    graph.root.uuid, known_revision=first["revision"]
                )["state"]
                == "unchanged"
            )
            assert service.get_run(graph.root.uuid) == detail
        elapsed = time.perf_counter() - started
        record_property("unchanged_projection_and_detail_10_cycles_seconds", elapsed)
        record_property("snapshot_selects_for_20_gets", len(queries) - before)
        assert len(queries) - before == 20 and len(reads) == 1 and structures == [1]
        mtime = graph.root.mtime
        graph.calc.set_process_state("finished")
        graph.calc.set_exit_status(303)
        failed = service.get_run_projection(
            graph.root.uuid, known_revision=first["revision"]
        )
        assert (
            failed["state"] == "available" and failed["revision"] != first["revision"]
        )
        assert (
            failed["children"][0]["exit_status"] == 303
            and not failed["children"][0]["is_finished_ok"]
        )
        assert orm.load_node(graph.root.uuid).mtime == mtime
        new_calc = graph.add_calc(2)
        added = service.get_run_projection(
            graph.root.uuid, known_revision=failed["revision"]
        )
        assert len(added["children"]) == 2 and added["state"] == "available"
        result = orm.SinglefileData(
            file=io.BytesIO(
                json.dumps({"status": "failed", "literal": "result2"}).encode()
            ),
            filename="result.json",
        )
        result.base.links.add_incoming(
            new_calc, link_type=LinkType.CREATE, link_label="result"
        )
        result.store()
        result_link = service.get_run_projection(
            graph.root.uuid, known_revision=added["revision"]
        )
        assert result_link["state"] == "available"
        assert service.get_run(graph.root.uuid)["results"] == {}  # Ambiguous attempts.
        selection = orm.Dict(
            dict={"execution_id": "e1", "attempt": 2, "calcjob_uuid": new_calc.uuid}
        ).store()
        selection.base.links.add_incoming(
            graph.root, link_type=LinkType.RETURN, link_label="selections__e1"
        )
        selected = service.get_run_projection(
            graph.root.uuid, known_revision=result_link["revision"]
        )
        assert selected["state"] == "available"
        assert service.get_run(graph.root.uuid)["results"] == {
            "e1": {"status": "failed", "literal": "result2"}
        }
        retrieved = orm.FolderData()
        retrieved.base.links.add_incoming(
            new_calc, link_type=LinkType.CREATE, link_label="retrieved"
        )
        retrieved.store()
        retrieved_link = service.get_run_projection(
            graph.root.uuid, known_revision=selected["revision"]
        )
        assert (
            retrieved_link["state"] == "available"
            and retrieved_link["children"][1]["retrieved_uuid"] == retrieved.uuid
        )
        graph.root.set_process_state("excepted")
        parent_failed = service.get_run_projection(
            graph.root.uuid, known_revision=retrieved_link["revision"]
        )
        assert (
            parent_failed["state"] == "available"
            and parent_failed["summary"]["process_state"] == "excepted"
        )
        for _ in range(10):
            assert service.get_run(graph.root.uuid)["results"] == {
                "e1": {"status": "failed", "literal": "result2"}
            }
        assert len(reads) == 1 and result_reads == [result.uuid]
    finally:
        event.remove(graph.engine, "before_cursor_execute", count)


@pytest.mark.integration
def test_postgres_readonly_ambient_rollback_and_limits(postgres_graph, monkeypatch):
    from aiida_qcl_negf import read_projection as projection
    from sqlalchemy import text

    graph = postgres_graph
    baseline = service.get_run_projection(graph.root.uuid)
    session = graph.backend.get_session()
    session.execute(
        text("UPDATE db_dbnode SET label=:label WHERE id=:pk"),
        {"label": "pending", "pk": graph.calc.pk},
    )
    assert session.in_transaction()
    pending = service.get_run_projection(
        graph.root.uuid, known_revision=baseline["revision"]
    )
    assert pending["state"] == "unchanged" and session.in_transaction()
    session.rollback()
    assert (
        service.get_run_projection(
            graph.root.uuid, known_revision=baseline["revision"]
        )["state"]
        == "unchanged"
    )
    with (
        graph.engine.connect().execution_options(
            isolation_level="READ COMMITTED", postgresql_readonly=True
        ) as connection,
        pytest.raises(Exception, match="read-only"),
    ):
        connection.execute(
            text("UPDATE db_dbnode SET label=label WHERE id=:pk"),
            {"pk": graph.calc.pk},
        )
    monkeypatch.setattr(projection, "MAX_GRAPH_NODES", 2)
    assert service.get_run_projection(graph.root.uuid)["reason"] == "graph_limit"
    monkeypatch.setattr(projection, "MAX_GRAPH_NODES", 10000)
    monkeypatch.setattr(projection, "MAX_GRAPH_LINKS", 1)
    assert service.get_run_projection(graph.root.uuid)["reason"] == "graph_limit"
    monkeypatch.setattr(projection, "MAX_GRAPH_LINKS", 40000)
    monkeypatch.setattr(projection, "MAX_SNAPSHOT_BYTES", 1100)
    overflow = service.get_run_projection(
        graph.root.uuid, known_revision=baseline["revision"]
    )
    assert overflow["state"] == "unavailable" and overflow["reason"] == "snapshot_limit"


@pytest.mark.integration
def test_sqlite_projection_explicitly_unsupported(aiida_profile_clean):
    assert service.get_run_projection(RUN)["reason"] == "unsupported_backend"


def test_backend_loading_failure_is_unavailable(monkeypatch):
    from aiida_qcl_negf import read_projection as projection

    def failed():
        raise RuntimeError("private DB connection details")

    monkeypatch.setattr(projection, "current_backend", failed)
    result = service.get_run_projection(RUN, known_revision="old")
    assert result["state"] == "unavailable" and result["revision"] is None
    assert result["reason"] == "snapshot_failed" and "private" not in str(result)


def test_version_guard_never_queries_or_returns_cached_status(monkeypatch):
    from aiida_qcl_negf import read_projection as projection

    backend = NS(profile=NS(uuid="profile", storage_backend="core.psql_dos"))
    monkeypatch.setattr(projection, "current_backend", lambda: backend)
    monkeypatch.setattr(projection.aiida, "__version__", "2.9.3")
    assert (
        service.get_run_projection(RUN, known_revision="old")["reason"]
        == "unsupported_aiida_version"
    )


def test_backend_identity_invalidates_immutable_payload_scope():
    from aiida_qcl_negf import read_projection as projection

    first = NS(profile=NS(uuid="same", storage_backend="core.psql_dos"))
    second = NS(profile=NS(uuid="same", storage_backend="core.psql_dos"))
    scope1, cache1 = projection._cache_scope(first)
    scope_again, cache_again = projection._cache_scope(first)
    assert scope1 == scope_again and cache1 is cache_again
    scope2, cache2 = projection._cache_scope(second)
    assert scope1 != scope2 and cache1 is not cache2


def test_invalid_or_unknown_uuid_and_projection_response_limit(monkeypatch):
    from aiida_qcl_negf import read_projection as projection

    with pytest.raises(ValueError, match="full process UUID"):
        service.get_run_projection("injection-not-a-uuid")
    snapshot = {
        "revision": "r",
        "summary": {"uuid": RUN, "label": "x" * 1000},
        "children": [],
    }
    monkeypatch.setattr(projection, "read_snapshot", lambda _: snapshot)
    monkeypatch.setattr(service, "MAX_RESPONSE_BYTES", 400)
    result = service.get_run_projection(RUN)
    assert result["state"] == "unavailable" and result["reason"] == "response_limit"
    assert result["summary"] is None and result["revision"] is None


@pytest.mark.integration
def test_postgres_statement_snapshot_and_query_failure(postgres_graph):
    from sqlalchemy import event, text

    graph = postgres_graph
    baseline = service.get_run_projection(graph.root.uuid)
    committed = []

    def commit_after_statement(
        connection, cursor, statement, parameters, context, executemany
    ):
        if statement.startswith("WITH RECURSIVE calls") and not committed:
            committed.append(True)
            with graph.engine.begin() as writer:
                writer.execute(
                    text("UPDATE db_dbnode SET label=:label WHERE id IN (:root,:calc)"),
                    {
                        "label": "concurrent",
                        "root": graph.root.pk,
                        "calc": graph.calc.pk,
                    },
                )

    event.listen(graph.engine, "after_cursor_execute", commit_after_statement)
    try:
        during = service.get_run_projection(
            graph.root.uuid, known_revision=baseline["revision"]
        )
    finally:
        event.remove(graph.engine, "after_cursor_execute", commit_after_statement)
    assert (
        during["state"] == "unchanged"
    )  # Commit happened after its statement snapshot.
    next_poll = service.get_run_projection(
        graph.root.uuid, known_revision=baseline["revision"]
    )
    assert next_poll["state"] == "available"
    assert (
        next_poll["summary"]["label"]
        == next_poll["children"][0]["label"]
        == "concurrent"
    )

    def failed(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("WITH RECURSIVE calls"):
            raise RuntimeError("synthetic connection failure")

    event.listen(graph.engine, "before_cursor_execute", failed)
    try:
        failure = service.get_run_projection(
            graph.root.uuid, known_revision=next_poll["revision"]
        )
        assert (
            failure["state"] == "unavailable"
            and failure["reason"] == "snapshot_failed"
            and failure["revision"] is None
        )
    finally:
        event.remove(graph.engine, "before_cursor_execute", failed)


def test_even_unavailable_envelope_must_obey_response_bound(monkeypatch):
    from aiida_qcl_negf import read_projection as projection

    monkeypatch.setattr(projection, "current_backend", lambda: None)
    monkeypatch.setattr(service, "MAX_RESPONSE_BYTES", 32)
    with pytest.raises(ValueError, match="response limit"):
        service.get_run_projection(RUN)


@pytest.mark.integration
def test_postgres_schema_guard_cannot_serve_previous_revision(postgres_graph):
    from sqlalchemy import text

    graph = postgres_graph
    baseline = service.get_run_projection(graph.root.uuid)
    with graph.engine.begin() as writer:
        writer.execute(
            text("UPDATE alembic_version SET version_num=:version"),
            {"version": "synthetic_other"},
        )
    try:
        result = service.get_run_projection(
            graph.root.uuid, known_revision=baseline["revision"]
        )
        assert (
            result["state"] == "unavailable"
            and result["reason"] == "unsupported_schema"
        )
        assert result["revision"] is None
    finally:
        with graph.engine.begin() as writer:
            writer.execute(
                text("UPDATE alembic_version SET version_num=:version"),
                {"version": "main_0002"},
            )


@pytest.mark.integration
def test_postgres_backend_replacement_and_absent_attempt(postgres_graph, monkeypatch):
    from aiida.storage.psql_dos.backend import PsqlDosBackend
    from aiida_qcl_negf import read_projection as projection

    graph = postgres_graph
    child = graph.add_calc(None)
    before = service.get_run_projection(graph.root.uuid)
    assert [(row["uuid"], row["attempt"]) for row in before["children"]] == [
        (graph.calc.uuid, 1),
        (child.uuid, 1),
    ]
    replacement = PsqlDosBackend(graph.profile)
    try:
        monkeypatch.setattr(projection, "current_backend", lambda: replacement)
        after = service.get_run_projection(
            graph.root.uuid, known_revision=before["revision"]
        )
        assert after["state"] == "available" and after["revision"] != before["revision"]
        assert after["children"] == before["children"]
    finally:
        replacement.get_session().close()
        replacement.close()


def test_large_scalar_detail_does_not_rescan_all_children_or_selections(
    monkeypatch, record_property
):
    from aiida_qcl_negf import read_projection as projection

    class CountedList(list):
        scans = 0

        def __iter__(self):
            self.scans += 1
            return super().__iter__()

    count = 600
    children = CountedList(
        [
            {
                "uuid": f"child-{i}",
                "execution_id": f"e{i}",
                "attempt": 1,
                "retrieved_uuid": None,
                "result_uuid": f"result-{i}",
            }
            for i in range(count)
        ]
    )
    selections = CountedList(
        [
            {"execution_id": f"e{i}", "attempt": 1, "calcjob_uuid": f"child-{i}"}
            for i in range(count)
        ]
    )
    backend = NS(profile=NS(storage_backend="core.psql_dos"))
    monkeypatch.setattr(projection, "current_backend", lambda: backend)
    monkeypatch.setattr(
        projection,
        "read_snapshot",
        lambda _: {
            "summary": {"uuid": RUN},
            "children": children,
            "selections": selections,
            "plan_uuid": "plan",
            "cache": projection.PayloadCache(),
        },
    )
    from contextlib import nullcontext

    monkeypatch.setattr(
        projection,
        "payload_reader",
        lambda _: nullcontext(lambda uuid, consume: consume(uuid)),
    )
    monkeypatch.setattr(service.orm, "load_node", lambda uuid: uuid)
    monkeypatch.setattr(
        service, "read_plan", lambda _: {"fingerprint": "f" * 64, "points": []}
    )
    reads = []
    monkeypatch.setattr(
        service,
        "read_json",
        lambda node, **kwargs: (reads.append(node), {"literal": node})[1],
    )
    actual = service.get_run(RUN)
    assert (
        len(actual["children"]) == len(actual["results"]) == count
        and len(reads) == count
    )
    record_property("full_child_scans_for_600_results", children.scans)
    record_property("full_selection_scans_for_600_results", selections.scans)
    assert children.scans <= 3 and selections.scans <= 2


def test_large_snapshot_links_have_linear_scalar_reads(record_property):
    from aiida_qcl_negf import read_projection as projection

    class CountedLink(dict):
        reads = 0

        def __getitem__(self, key):
            type(self).reads += 1
            return super().__getitem__(key)

    root = {
        "uuid": RUN,
        "pk": 1,
        "node_type": "process.workflow.workchain.WorkChainNode.",
        "process_type": "aiida.workflows:qcl_negf.plan",
        "label": "",
        "ctime": "2026-10-28T00:00:00+00:00",
        "mtime": "2026-10-28T00:00:00+00:00",
        "process_state": "running",
        "exit_status": None,
    }
    rows = [{"kind": "node", "payload": root}]
    count = 600
    for index in range(count):
        child = root | {
            "uuid": f"child-{index}",
            "pk": index + 2,
            "node_type": "process.calculation.calcjob.CalcJobNode.",
        }
        rows.append({"kind": "node", "payload": child})
        for label, value in [("execution_id", f"e{index}"), ("attempt", 1)]:
            rows.append(
                {
                    "kind": "link",
                    "payload": CountedLink(
                        input_uuid=f"data-{index}-{label}",
                        output_uuid=child["uuid"],
                        label=label,
                        type="input_calc",
                        value=value,
                        selection=None,
                    ),
                }
            )
    actual = projection._snapshot_details(
        RUN, rows, ("scope",), projection.PayloadCache()
    )
    assert (
        len(actual["children"]) == count
        and actual["children"][-1]["execution_id"] == "e599"
    )
    record_property("link_scalar_reads_for_1200_links", CountedLink.reads)
    assert CountedLink.reads <= 20 * (2 * count)
    rows.append(rows[-1])
    with pytest.raises(projection.ProjectionUnavailable, match="invalid_links"):
        projection._snapshot_details(RUN, rows, ("scope",), projection.PayloadCache())


@pytest.mark.integration
@pytest.mark.parametrize("cache_state", ["cold", "evicted"])
@pytest.mark.parametrize("payload_failure", [None, "plan", "result"])
def test_pending_orm_survives_plan_and_result_cache_misses(
    postgres_graph, cache_state, payload_failure, monkeypatch, record_property
):
    import io
    import json

    from aiida import orm
    from aiida.common.links import LinkType
    from aiida_qcl_negf import read_projection as projection
    from sqlalchemy import event, text

    graph = postgres_graph
    result = orm.SinglefileData(
        file=io.BytesIO(json.dumps({"status": "failed", "literal": "R1"}).encode()),
        filename="result.json",
    )
    result.base.links.add_incoming(
        graph.calc, link_type=LinkType.CREATE, link_label="result"
    )
    result.store()
    root_uuid = graph.root.uuid
    baseline = service.get_run_projection(root_uuid)
    if cache_state == "evicted":
        assert service.get_run(root_uuid)["results"] == {
            "e1": {"status": "failed", "literal": "R1"}
        }
        cache = projection._cache_scope(graph.backend)[1]
        assert cache.max_entries == 32
        for index in range(33):
            cache.read("synthetic-eviction", str(index), lambda: {"value": "small"})
        assert len(cache.entries) == 32
    session = graph.backend.get_session()
    created, consumed = [], []
    factory = projection.PsqlDosBackend

    def create_reader(profile):
        reader = factory(profile)
        created.append(reader)
        return reader

    monkeypatch.setattr(projection, "PsqlDosBackend", create_reader)
    original_plan, original_result = service.read_plan, service.read_json

    def consume(kind, node, original, **kwargs):
        reader = node.backend
        assert reader is created[0] and reader is not graph.backend
        reader_session = reader.get_session()
        assert reader_session is not session
        assert (
            reader_session.execute(text("SHOW transaction_read_only")).scalar_one()
            == "on"
        )
        consumed.append(kind)
        if payload_failure == kind:
            raise ValueError("synthetic payload failure")
        return original(node, **kwargs)

    monkeypatch.setattr(
        service, "read_plan", lambda node: consume("plan", node, original_plan)
    )
    monkeypatch.setattr(
        service,
        "read_json",
        lambda node, **kwargs: consume("result", node, original_result, **kwargs),
    )
    model = graph.calc.backend_entity.bare_model
    model.label = "pending-R1"
    events = []
    writes = []

    def flushed(*args):
        events.append("flush")

    def committed(*args):
        events.append("commit")

    def rolled_back(*args):
        events.append("rollback")

    def statement(connection, cursor, sql, parameters, context, executemany):
        if sql.lstrip().split(None, 1)[0].upper() in {
            "UPDATE",
            "INSERT",
            "DELETE",
            "MERGE",
            "CREATE",
            "ALTER",
            "DROP",
        }:
            writes.append(sql)

    listeners = [
        ("before_flush", flushed),
        ("after_commit", committed),
        ("after_rollback", rolled_back),
    ]
    for name, callback in listeners:
        event.listen(session, name, callback)
    event.listen(graph.engine, "before_cursor_execute", statement)
    try:
        assert session.is_modified(model)
        assert (
            service.get_run_projection(root_uuid, known_revision=baseline["revision"])[
                "state"
            ]
            == "unchanged"
        )
        if payload_failure is None:
            detail = service.get_run(root_uuid)
            assert detail["children"][0]["label"] == ""
            assert detail["results"] == {"e1": {"status": "failed", "literal": "R1"}}
            for _ in range(10):
                assert service.get_run(root_uuid) == detail
        else:
            with pytest.raises(ValueError, match="synthetic payload failure"):
                service.get_run(root_uuid)
        assert len(created) == 1 and created[0].is_closed
        assert consumed == (
            ["plan"] if payload_failure == "plan" else ["plan", "result"]
        )
        record_property("actor_session_events", events)
        record_property("actor_engine_writes", writes)
        record_property("reader_backends_created_and_closed", len(created))
        assert events == [] and writes == []
        assert session.is_modified(model) and model.label == "pending-R1"
        assert (
            service.get_run_projection(root_uuid, known_revision=baseline["revision"])[
                "state"
            ]
            == "unchanged"
        )
    finally:
        for name, callback in listeners:
            event.remove(session, name, callback)
        event.remove(graph.engine, "before_cursor_execute", statement)
        session.rollback()
