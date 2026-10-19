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
    calls = []

    def child(attempt, uuid, raw):
        class Repository:
            @contextmanager
            def open(self, path, mode):
                calls.append(("open", uuid))

                class Stream(BytesIO):
                    def read(self, size=-1):
                        calls.append(("read", uuid))
                        return super().read(size)

                    def seek(self, offset, whence=0):
                        calls.append(("seek", uuid))
                        return super().seek(offset, whence)

                with Stream(raw) as stream:
                    yield stream

            def __getattr__(self, name):
                calls.append((name, uuid))
                raise AssertionError("Unexpected repository I/O")

        inventory = {
            "complete": True,
            "files": [{"path": "result.bin", "size": len(raw)}],
        }
        return NS(
            uuid=uuid,
            inputs=NS(execution_id=NS(value="E"), attempt=NS(value=attempt)),
            outputs=Outputs(
                retrieved=NS(base=NS(repository=Repository())),
                inventory=NS(get_dict=lambda: inventory),
            ),
        )

    children = [child(1, C1, b"A" * 100), child(2, C2, b"B" * 200)]
    selection = {"execution_id": "E", "attempt": 2, "calcjob_uuid": C2}
    node = NS(outputs=Outputs(selections={"E": NS(get_dict=lambda: selection)}))
    monkeypatch.setattr(service, "_node", lambda _: node)
    monkeypatch.setattr(service, "_children", lambda _: children)
    return children, node, selection, calls


def test_cached_metadata_and_exact_payloads(source):
    children, node, selection, calls = source
    rows = service.list_artifacts(RUN)
    assert [row["size"] for row in rows] == [100, 200]
    assert service.get_artifact_metadata(RUN, "E", "result.bin") == rows[1]
    for row in rows:
        assert (
            service.get_artifact_metadata(
                RUN,
                "E",
                "result.bin",
                attempt=row["attempt"],
                calcjob_uuid=row["calcjob_uuid"],
            )
            == row
        )
        assert not calls
    for row, expected in zip(rows, [b"A" * 100, b"B" * 200]):
        with service.open_artifact(
            RUN,
            "E",
            "result.bin",
            attempt=row["attempt"],
            calcjob_uuid=row["calcjob_uuid"],
        ) as stream:
            assert stream.read() == expected


def test_equal_size_content_oracle_and_selection_pin(source):
    children, node, selection, calls = source

    class Repo:
        def open(self, path, mode):
            return BytesIO(b"B" * 100)

    children[1].outputs.retrieved = NS(base=NS(repository=Repo()))
    children[1].outputs.inventory.get_dict()["files"][0]["size"] = 100
    row = service.get_artifact_metadata(RUN, "E", "result.bin")
    selection.update(attempt=1, calcjob_uuid=C1)
    with service.open_artifact(
        RUN, "E", "result.bin", attempt=row["attempt"], calcjob_uuid=row["calcjob_uuid"]
    ) as stream:
        assert stream.read() == b"B" * 100


@pytest.mark.parametrize(
    "failure",
    [
        "pair",
        "foreign",
        "execution",
        "path",
        "inventory",
        "retrieved",
        "incomplete",
        "duplicate",
        "bound",
        "ambiguous",
        "conflict",
    ],
)
def test_metadata_fail_closed_without_payload(source, monkeypatch, failure):
    children, node, selection, calls = source
    kwargs = {"attempt": 2, "calcjob_uuid": C2}
    execution, path, error = "E", "result.bin", LookupError
    if failure == "pair":
        kwargs["calcjob_uuid"] = C1
    elif failure == "foreign":
        kwargs["calcjob_uuid"] = RUN
    elif failure == "execution":
        execution = "other"
    elif failure == "path":
        path = "absent"
    elif failure in ("inventory", "retrieved"):
        del children[1].outputs[failure]
    elif failure == "incomplete":
        children[1].outputs.inventory.get_dict()["complete"] = False
        error = ValueError
    elif failure == "duplicate":
        children[1].outputs.inventory.get_dict()["files"] *= 2
        error = ValueError
    elif failure == "bound":
        monkeypatch.setattr(service, "MAX_ARTIFACTS", 0)
        error = ValueError
    elif failure == "ambiguous":
        node.outputs.clear()
        kwargs = {}
        error = ValueError
    elif failure == "conflict":
        node.outputs.selections["other"] = NS(get_dict=lambda: selection)
        kwargs = {}
        error = ValueError
    with pytest.raises(error):
        service.get_artifact_metadata(RUN, execution, path, **kwargs)
    assert calls == []


@pytest.mark.parametrize("path", ["", "../x", "/x", "sub//x", "sub\\x", "./x"])
def test_invalid_paths_before_selection(source, path):
    with pytest.raises(ValueError):
        service.get_artifact_metadata(RUN, "E", path)
    assert source[-1] == []


def test_legacy_attempt_and_selector_only(source):
    children, node, selection, calls = source
    children.pop()
    node.outputs.clear()
    del children[0].inputs.attempt
    row = service.get_artifact_metadata(RUN, "E", "result.bin")
    assert row["attempt"] == 1 and row["calcjob_uuid"] == C1
    assert service.get_artifact_metadata(RUN, "E", "result.bin", calcjob_uuid=C1) == row
    assert service.get_artifact_metadata(RUN, "E", "result.bin", attempt=1) == row
    assert calls == []
