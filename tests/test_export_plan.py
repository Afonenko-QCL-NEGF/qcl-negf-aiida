"""Exact frozen-plan reads need neither an AiiDA daemon nor storage profile."""

import hashlib
import json
from contextlib import contextmanager
from copy import deepcopy
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiida_qcl_negf import service


class Outputs(dict):
    def __getattr__(self, name):
        return self[name]


@pytest.fixture
def source(monkeypatch):
    plan = json.loads((Path(__file__).parent / "fixtures/plan.json").read_bytes())
    input_bytes = json.dumps(plan, ensure_ascii=False, indent=3).encode() + b"\n"

    class Input:
        def open(self, mode):
            assert mode == "rb"
            return BytesIO(input_bytes)

    class Repository:
        raw = json.dumps(plan, ensure_ascii=False, indent=1).encode() + b"\n\n"
        absent = False
        def __init__(self):
            self.opened = []

        def get_object(self, path):
            assert path == "result/scientific_plan.json"
            if self.absent:
                raise FileNotFoundError(path)
            return object()

        @contextmanager
        def open(self, path, mode):
            assert (path, mode) == ("result/scientific_plan.json", "rb")
            handle = BytesIO(self.raw)
            self.opened.append(handle)
            try:
                yield handle
            finally:
                handle.close()

    repository = Repository()
    child = SimpleNamespace(
        inputs=SimpleNamespace(execution_id=SimpleNamespace(value="execution-1")),
        outputs=Outputs(
            retrieved=SimpleNamespace(base=SimpleNamespace(repository=repository))
        ),
    )
    node = SimpleNamespace(inputs=SimpleNamespace(plan=Input()))
    monkeypatch.setattr(service, "_node", lambda _: node)
    monkeypatch.setattr(service, "_children", lambda _: [child])
    return plan, input_bytes, repository, child


def test_retrieved_frozen_plan_preserves_exact_bytes_and_closes_reader(source):
    _, _, repository, _ = source
    result = service.get_export_plan("run", "execution-1")
    assert result == {
        "plan": repository.raw,
        "source": "aiida.retrieved:result/scientific_plan.json",
        "sha256": hashlib.sha256(repository.raw).hexdigest(),
        "bytes": len(repository.raw),
    }
    assert repository.opened[0].closed is True


def test_absent_retrieved_plan_falls_back_to_exact_file_backed_input(source):
    _, input_bytes, repository, _ = source
    repository.absent = True
    result = service.get_export_plan("run", "execution-1")
    assert result["plan"] == input_bytes and result["source"] == "aiida.input.plan"
    assert result["sha256"] == hashlib.sha256(input_bytes).hexdigest()
    assert not repository.opened


@pytest.mark.parametrize(
    "corruption", ["utf8", "identity", "configuration", "oversize"]
)
def test_corrupt_present_retrieved_plan_never_falls_back(
    source, monkeypatch, corruption
):
    plan, input_bytes, repository, _ = source
    if corruption == "utf8":
        repository.raw = b"\xff"
    elif corruption == "oversize":
        monkeypatch.setattr(service, "MAX_PLAN_BYTES", len(input_bytes) + 10)
        repository.raw = input_bytes + b" " * 11
    else:
        altered = deepcopy(plan)
        if corruption == "identity":
            altered["fingerprint"] = "c" * 64
        else:
            altered["executions"][0]["resolved_configuration"]["solver"][
                "maximum_scba_iterations"
            ] += 1
        repository.raw = json.dumps(altered).encode()
    with pytest.raises(ValueError):
        service.get_export_plan("run", "execution-1")
    assert repository.opened[0].closed is True


def test_wrong_execution_and_missing_retrieval_are_rejected(source):
    _, _, _, child = source
    with pytest.raises(LookupError):
        service.get_export_plan("run", "other-execution")
    child.outputs.clear()
    with pytest.raises(LookupError):
        service.get_export_plan("run", "execution-1")
