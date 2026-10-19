"""Synthetic repositories exercise exact export binding without an AiiDA profile."""
from contextlib import contextmanager
from io import BytesIO
from types import SimpleNamespace as NS

import pytest

from aiida_qcl_negf import service

RUN = "e7f5313d-22fd-49fa-9be5-85fbe29e1974"
C1 = "10dd2a4c-031b-425c-90b8-2f7ea564d313"
C2 = "20dd2a4c-031b-425c-90b8-2f7ea564d313"


class Outputs(dict):
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc


@pytest.fixture
def source(monkeypatch):
    events = []

    class Result:
        def __init__(self, raw):
            self.raw, self.handles = raw, []

        def open(self, mode):
            assert mode == "rb"
            handle = BytesIO(self.raw)
            self.handles.append(handle)
            return handle

    def child(attempt, uuid, payload, status):
        class Repository:
            @contextmanager
            def open(self, path, mode):
                assert mode == "rb"
                events.append((uuid, path))
                with BytesIO(payload[path]) as handle:
                    yield handle

            def __getattr__(self, name):
                raise AssertionError("Unexpected repository I/O: " + name)

        inventory = {"complete": True, "files": [
            {"path": path, "size": len(raw)} for path, raw in payload.items()]}
        result = Result(('{"selected_execution_id":"E","status":"' + status + '"}').encode())
        return NS(uuid=uuid, inputs=NS(execution_id=NS(value="E"), attempt=NS(value=attempt)),
                  outputs=Outputs(retrieved=NS(base=NS(repository=Repository())),
                                  inventory=NS(get_dict=lambda: inventory), result=result))

    children = [child(1, C1, {"same.bin": b"AAAA", "only1.bin": b"1"}, "completed"),
                child(2, C2, {"same.bin": b"BBBB", "only2.bin": b"2"}, "failed")]
    selection = {"execution_id": "E", "attempt": 2, "calcjob_uuid": C2}
    node = NS(outputs=Outputs(selections={"E": NS(get_dict=lambda: selection)}))
    monkeypatch.setattr(service, "_node", lambda identifier: node)
    monkeypatch.setattr(service, "_children", lambda node: children)
    return children, node, selection, events


def test_export_selection_pins_child(source):
    children, node, selection, events = source
    binding = service.get_export_selection(RUN, "E")
    assert binding == {"execution_id": "E", "attempt": 2, "calcjob_uuid": C2, "status": "failed"}
    assert children[1].outputs.result.handles[0].closed
    selection.update(attempt=1, calcjob_uuid=C1)
    selectors = {"attempt": binding["attempt"], "calcjob_uuid": binding["calcjob_uuid"]}
    assert service.list_execution_artifacts(RUN, "E", **selectors) == [
        {"execution_id": "E", "attempt": 2, "calcjob_uuid": C2, "path": "only2.bin", "size": 1},
        {"execution_id": "E", "attempt": 2, "calcjob_uuid": C2, "path": "same.bin", "size": 4}]
    assert events == []
    with service.open_artifact(RUN, "E", "same.bin", **selectors) as stream:
        assert stream.read() == b"BBBB"
    assert events == [(C2, "same.bin")]


@pytest.mark.parametrize("status", ["running", "completed", "completed_with_warnings", "failed", "cancelled", "paused"])
def test_selected_status_uses_only_selected_result(source, status):
    children, _, _, events = source
    children[0].outputs.result.raw = b"unrelated invalid result"
    children[1].outputs.result.raw = ('{"status":"' + status + '","selected_execution_id":"E"}').encode()
    children[1].exit_status = 0
    assert service.get_export_selection(RUN, "E")["status"] == status
    assert not children[0].outputs.result.handles and events == []


@pytest.mark.parametrize("exit_status", [0, 1])
def test_absent_result_is_unknown_independent_of_process_exit(source, exit_status):
    children = source[0]
    del children[1].outputs["result"]
    children[1].exit_status = exit_status
    assert service.get_export_selection(RUN, "E")["status"] == "unknown"


@pytest.mark.parametrize("raw", [b"\xff", b"{", b"[]", b"{}", b'{"status":"unknown","selected_execution_id":"E"}',
    b'{"status":true,"selected_execution_id":"E"}', b'{"status":"failed"}',
    b'{"status":"failed","selected_execution_id":"other"}',
    b'{"status":"failed","status":"completed","selected_execution_id":"E"}',
    b'{"status":"failed","selected_execution_id":"E","x":NaN}'])
def test_present_invalid_result_rejects_and_closes(source, raw):
    result = source[0][1].outputs.result
    result.raw = raw
    with pytest.raises(ValueError, match="Selected scientific result metadata is invalid"):
        service.get_export_selection(RUN, "E")
    assert result.handles[0].closed and source[-1] == []


def test_result_bound_probes_one_extra_byte(source, monkeypatch):
    result = source[0][1].outputs.result
    monkeypatch.setattr(service, "MAX_RESULT_BYTES", len(result.raw) - 1)
    with pytest.raises(ValueError, match="Selected scientific result metadata is invalid"):
        service.get_export_selection(RUN, "E")
    assert result.handles[0].closed


@pytest.mark.parametrize("operation", ["get_export_selection", "list_execution_artifacts"])
@pytest.mark.parametrize("kwargs,error", [
    ({"attempt": True}, ValueError), ({"attempt": 0}, ValueError), ({"attempt": -1}, ValueError),
    ({"attempt": 1.0}, ValueError), ({"attempt": "2"}, ValueError),
    ({"calcjob_uuid": ""}, ValueError), ({"calcjob_uuid": C2[:8]}, ValueError),
    ({"calcjob_uuid": 1}, ValueError), ({"calcjob_uuid": C1, "attempt": 2}, ValueError),
    ({"calcjob_uuid": RUN, "attempt": 2}, LookupError), ({"attempt": 3}, LookupError)])
def test_invalid_selectors_before_metadata_and_payload(source, operation, kwargs, error):
    with pytest.raises(error):
        getattr(service, operation)(RUN, "E", **kwargs)
    assert all(not child.outputs.result.handles for child in source[0])
    assert source[-1] == []


@pytest.mark.parametrize("identifier,execution", [(1,"E"), (RUN[:8],"E"), (RUN,""), (RUN,1), (RUN,"x"*129)])
def test_invalid_scalar_input_before_node_lookup(source, monkeypatch, identifier, execution):
    def forbidden(identifier):
        raise AssertionError("Invalid input reached node lookup")
    monkeypatch.setattr(service, "_node", forbidden)
    for operation in (service.get_export_selection, service.list_execution_artifacts):
        with pytest.raises(ValueError):
            operation(identifier, execution)


@pytest.mark.parametrize("kwargs", [{}, {"attempt":1}, {"calcjob_uuid":C1}, {"attempt":1,"calcjob_uuid":C1.upper()}])
def test_legacy_and_canonical_uuid_selectors(source, kwargs):
    children, node, _, events = source
    children.pop()
    node.outputs.clear()
    del children[0].inputs.attempt
    assert service.get_export_selection(RUN.upper(), "E", **kwargs) == {
        "execution_id":"E", "attempt":1, "calcjob_uuid":C1, "status":"completed"}
    assert len(service.list_execution_artifacts(RUN, "E", **kwargs)) == 2
    assert events == []


@pytest.mark.parametrize("corruption", ["conflict", "bad_attempt", "bad_uuid", "foreign", "pair", "nonobject"])
def test_invalid_published_selection_never_falls_back(source, corruption):
    children, node, selection, events = source
    if corruption == "conflict":
        node.outputs.selections["second"] = NS(get_dict=lambda: dict(selection))
    elif corruption == "nonobject":
        node.outputs.selections["E"] = NS(get_dict=lambda: ["E"])
    else:
        selection.update({"bad_attempt":{"attempt":True}, "bad_uuid":{"calcjob_uuid":"bad"},
                          "foreign":{"calcjob_uuid":RUN}, "pair":{"attempt":1}}[corruption])
    for operation in (service.get_export_selection, service.list_execution_artifacts):
        with pytest.raises(ValueError):
            operation(RUN, "E")
    assert events == [] and all(not child.outputs.result.handles for child in children)
    assert service.get_export_selection(RUN,"E",attempt=2,calcjob_uuid=C2)["attempt"] == 2


def test_published_record_is_read_once_and_both_selectors_pinned(source, monkeypatch):
    children, node, selection, _ = source
    reads, calls = [], []
    def changing():
        reads.append(1)
        return dict(selection) if len(reads) == 1 else {"execution_id":"E","attempt":1,"calcjob_uuid":C1}
    node.outputs.selections["E"] = NS(get_dict=changing)
    original = service._select_child
    def select(node, execution_id, **kwargs):
        calls.append(kwargs)
        return original(node, execution_id, **kwargs)
    monkeypatch.setattr(service,"_select_child",select)
    assert service.get_export_selection(RUN,"E")["calcjob_uuid"] == C2
    assert reads == [1] and calls == [{"attempt":2,"calcjob_uuid":C2}]


@pytest.mark.parametrize("field,value", [("attempt",True),("attempt",0),("attempt","2"),("uuid","bad")])
def test_invalid_selected_stored_identity(source, field, value):
    children, node, _, _ = source
    children.pop(0)
    node.outputs.clear()
    if field == "attempt":
        children[0].inputs.attempt.value = value
    else:
        children[0].uuid = value
    with pytest.raises(ValueError, match="Selected child identity is invalid"):
        service.get_export_selection(RUN,"E")


@pytest.mark.parametrize("corruption", ["inventory", "retrieved", "incomplete", "marker", "files", "row", "path", "size", "bool_size", "duplicate", "bound"])
def test_export_inventory_rejects_foreign_or_incomplete_child(source, monkeypatch, corruption):
    children, _, _, events = source
    inventory = children[1].outputs.inventory.get_dict()
    error = ValueError
    if corruption in ("inventory", "retrieved"):
        del children[1].outputs[corruption]
        error = LookupError
    elif corruption == "incomplete": inventory["complete"] = False
    elif corruption == "marker": inventory["complete"] = 1
    elif corruption == "files": inventory["files"] = {}
    elif corruption == "row": inventory["files"][0] = []
    elif corruption == "path": inventory["files"][0]["path"] = "../bad"
    elif corruption == "size": inventory["files"][0]["size"] = -1
    elif corruption == "bool_size": inventory["files"][0]["size"] = True
    elif corruption == "duplicate": inventory["files"] *= 2
    elif corruption == "bound": monkeypatch.setattr(service,"MAX_ARTIFACTS",1)
    with pytest.raises(error):
        service.list_execution_artifacts(RUN,"E")
    assert events == [] and all(not child.outputs.result.handles for child in children)


def test_inventory_ignores_unselected_corruption_and_strips_source_identity(source):
    children = source[0]
    children[0].outputs.inventory.get_dict()["files"] *= 2
    children[1].outputs.inventory.get_dict()["files"][0].update(attempt=1,calcjob_uuid=C1,extra="unused")
    rows = service.list_execution_artifacts(RUN,"E")
    assert rows[1] == {"execution_id":"E","attempt":2,"calcjob_uuid":C2,"path":"same.bin","size":4}
    children[1].outputs.inventory.get_dict()["files"].clear()
    assert service.list_execution_artifacts(RUN,"E") == []


@pytest.mark.parametrize("operation", ["get_export_selection", "list_execution_artifacts"])
def test_ambiguous_omitted_and_attempt_only_reject(source, operation):
    children, node, _, events = source
    node.outputs.clear()
    with pytest.raises(ValueError, match="Multiple attempts exist"):
        getattr(service, operation)(RUN,"E")
    children[1].inputs.attempt.value = 1
    with pytest.raises(ValueError, match="Multiple attempts exist"):
        getattr(service, operation)(RUN,"E",attempt=1)
    assert events == []


@pytest.mark.parametrize("operation,output,message", [
    ("get_export_selection","result","Selected scientific result metadata is invalid"),
    ("list_execution_artifacts","inventory","Selected artifact inventory is invalid or incomplete")])
def test_present_unreadable_metadata_fails_closed(source, operation, output, message):
    def unavailable(*args, **kwargs):
        raise OSError("synthetic backend failure")
    metadata = source[0][1].outputs[output]
    if output == "result": metadata.open = unavailable
    else: metadata.get_dict = unavailable
    with pytest.raises(ValueError, match=message):
        getattr(service,operation)(RUN,"E")
    assert source[-1] == []


def test_present_attempt_without_value_is_invalid_not_legacy(source):
    children, node, _, events = source
    children.pop(0)
    children[0].inputs.attempt = NS()
    node.outputs.clear()
    with pytest.raises(ValueError, match="Selected child identity is invalid"):
        service.get_export_selection(RUN, "E")
    assert events == [] and not children[0].outputs.result.handles


@pytest.mark.parametrize("operation", ["get_export_selection", "list_execution_artifacts"])
@pytest.mark.parametrize("invalid_id", [None, 1, "", "x" * 129, "missing"])
def test_malformed_published_execution_id_never_uses_single_child(source, operation, invalid_id):
    children, _, selection, events = source
    children.pop(0)  # A malformed publication must not become legacy fallback.
    if invalid_id == "missing":
        del selection["execution_id"]
    else:
        selection["execution_id"] = invalid_id

    def forbidden_inventory_read():
        raise AssertionError("Invalid publication reached selected inventory")

    children[0].outputs.inventory.get_dict = forbidden_inventory_read
    with pytest.raises(ValueError, match="^Published attempt selection is invalid$"):
        getattr(service, operation)(RUN, "E")
    assert events == [] and not children[0].outputs.result.handles


def test_valid_other_execution_publication_allows_single_child_fallback(source):
    children, _, selection, events = source
    children.pop(0)
    selection["execution_id"] = "other-execution"
    assert service.get_export_selection(RUN, "E") == {
        "execution_id": "E", "attempt": 2, "calcjob_uuid": C2, "status": "failed"}
    assert service.list_execution_artifacts(RUN, "E") == [
        {"execution_id": "E", "attempt": 2, "calcjob_uuid": C2, "path": "only2.bin", "size": 1},
        {"execution_id": "E", "attempt": 2, "calcjob_uuid": C2, "path": "same.bin", "size": 4}]
    assert events == []
