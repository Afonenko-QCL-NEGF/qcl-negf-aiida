"""Literal metadata associations, no profile, scheduler, transport or solver."""
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace as NS

import pytest

from aiida_qcl_negf.performance_binding import point_pins, retained_source, sha, verify_sidecar
from qcl_negf_results.commits import json_bytes


def fixture():
    selection = {"calcjob_uuid": "job3", "execution_id": "E", "attempt": 3, "plan_fingerprint": "a" * 64}
    points = [{"id": "P", "execution_id": "E", "attempt": 2,
               "data": {"result_commit": "archive/E/P/final/commit.json"}}]
    commits = {"P": {"path": points[0]["data"]["result_commit"], "commit_sha256": "b" * 64,
                      "identity": {"point_id": "P", "execution_id": "E", "attempt": 2,
                                   "plan_fingerprint": "a" * 64, "calcjob_uuid": "job2"}}}
    pins = point_pins(points, commits, selection, {"P": "job2"})
    result_raw = b'{"scientific":"unchanged"}\n'
    summary = {"schema": "qcl-negf.performance.v2", "session_id": "session3", "closed": True,
               "measurement_coverage": {"sources": {}}}
    summary_raw = json_bytes(summary)
    commit = {"schema": "qcl-negf.artifact-commit.v2", "contract_set": "qcl-negf.results.v1",
              "identity": {**selection, "session_id": "session3"}, "generation": 1,
              "closed": True, "source_cursors": {}, "artifacts": [{
                  "path": "summary-000001.json", "role": "performance.summary", "profile": "science",
                  "media_type": "application/json", "bytes": len(summary_raw), "sha256": sha(summary_raw)}]}
    commit_raw = json_bytes(commit)
    def embedded(path, raw):
        return {"native_path": "performance/sessions/session3/" + path,
                "bytes": len(raw), "sha256": sha(raw), "raw_utf8": raw.decode()}
    sidecar = {"schema": "qcl-negf.performance-binding.v1", "scope": "execution_attempt",
               "selection": selection, "point_pins": pins,
               "runner_result": {"path": "result/series_result.json", "bytes": len(result_raw), "sha256": sha(result_raw)},
               "commit": embedded("commit-000001.json", commit_raw),
               "summary": embedded("summary-000001.json", summary_raw), "capture_status": {"terminal_tail": "unknown"}}
    return selection, points, commits, result_raw, sidecar


def verify(value=None, **kwargs):
    selection, points, commits, raw, sidecar = fixture()
    return verify_sidecar(json_bytes(sidecar if value is None else value), selection=selection,
                          result_raw=raw, points=points, commits=commits, source_owners=kwargs.pop("source_owners", {"P": "job2"}), **kwargs)


def test_independent_source2_enclosing3_and_exact_raw_result():
    reference = verify()
    assert reference["selection"]["attempt"] == 3
    assert reference["point_pins"][0]["source_attempt"] == 2
    assert reference["scope"] == "execution_attempt" and reference["session_id"] == "session3"
    with pytest.raises(ValueError):
        verify(source_owners={})
    selection, points, commits, raw, sidecar = fixture()
    with pytest.raises(ValueError):
        verify_sidecar(json_bytes(sidecar), selection=selection, result_raw=raw + b" ",
                       points=points, commits=commits, source_owners={"P": "job2"})


@pytest.mark.parametrize("key", ["selection", "point_pins", "summary"])
def test_foreign_or_changed_sidecar_never_associates(key):
    sidecar = deepcopy(fixture()[4])
    if key == "selection":
        sidecar[key]["calcjob_uuid"] = "foreign"
    elif key == "point_pins":
        sidecar[key][0]["source_attempt"] = 3
    else:
        sidecar[key]["raw_utf8"] += " "
    with pytest.raises(ValueError):
        verify(sidecar)


def test_missing_or_rejected_performance_is_unavailable_without_scientific_exit_change():
    class Outputs(dict):
        def __getattr__(self, key):
            try: return self[key]
            except KeyError as error: raise AttributeError(key) from error
    child = NS(outputs=Outputs())
    assert retained_source(child)["reason"] == "performance_binding_missing"
    child.outputs.selection = NS(get_dict=lambda: {"performance_status": "invalid"})
    assert retained_source(child)["status"] == "unavailable"


def test_retained_output_must_keep_exact_creator_uuid_and_bytes():
    selection, _, _, _, sidecar = fixture()
    raw = json_bytes(sidecar)
    reference = {**verify(), "sidecar_uuid": "sidecar"}
    output = NS(uuid="sidecar", creator=NS(uuid="job3"), open=lambda **_: BytesIO(raw))
    pin = NS(creator=NS(uuid="job3"), get_dict=lambda: {
        "performance_binding": reference, "result_sha256": reference["runner_result"]["sha256"]})
    child = NS(uuid="job3", inputs=NS(execution_id=NS(value="E"), attempt=NS(value=3)),
               outputs=NS(selection=pin, performance_binding=output))
    assert retained_source(child)["binding"]["selection"] == selection
    output.creator.uuid = "foreign"
    with pytest.raises(ValueError):
        retained_source(child)
