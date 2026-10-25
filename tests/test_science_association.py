"""Native3/input2 and borrowed source1/input2 use science, never observer proof."""
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace as NS
import hashlib
import json

import pytest

from aiida_qcl_negf.science_association import source_point_owner, validate_native_collisions


class Ports(dict):
    def __getattr__(self, key):
        try: return self[key]
        except KeyError as error: raise AttributeError(key) from error


def fixture(native=3, input_attempt=2, uuid="job2"):
    identity = {"point_id": "P", "execution_id": "E", "attempt": native, "plan_fingerprint": "a" * 64,
                "state_id": "state", "state_sequence": 1, "calcjob_uuid": uuid}
    row = {"id": "P", "execution_id": "E", "attempt": native, "status": "completed",
           "coordinates": {"temperature_K": 70., "voltage_per_period_V": 0., "branch": "B", "order": 1},
           "data": {"result_commit": "archive/E/P/final/commit.json",
                    "artifact_root": f"executions/E/P/attempt-{native}"}}
    commit = {"path": row["data"]["result_commit"], "commit_sha256": "b" * 64, "identity": identity,
              "receipt_status": "verified", "state_id": "state", "state_sequence": 1}
    series = {"plan_fingerprint": "a" * 64, "plan_scientific_fingerprint": "c" * 64, "points": [row]}
    raw = json.dumps(series).encode()
    pin = {"calcjob_uuid": uuid, "execution_id": "E", "attempt": input_attempt,
           "result_sha256": hashlib.sha256(raw).hexdigest(), "commits": {"P": commit}}
    child = NS(uuid=uuid, inputs=Ports(plan=NS(uuid="plan"), attempt=NS(value=input_attempt), execution_id=NS(value="E")),
               outputs=Ports())
    child.outputs.result = NS(creator=child, open=lambda **_: BytesIO(raw))
    child.outputs.selection = NS(creator=child, get_dict=lambda: pin)
    return child, row, pin


def check(row, pin, uuid="job2"):
    validate_native_collisions([row], pin["commits"], input_attempt=2,
        execution_id="E", plan_fingerprint="a" * 64, calcjob_uuid=uuid,
        scientific_fingerprint="c" * 64,result_scientific_fingerprint="c" * 64)


@pytest.mark.parametrize("native", [3, 4])
def test_committed_native_collision_has_current_owner_and_keeps_input_counter(native):
    child, row, pin = fixture(native=native)
    check(row, pin)
    assert pin["attempt"] == 2 and pin["commits"]["P"]["identity"]["attempt"] == native
    assert source_point_owner(child, native, "plan", point_id="P", commit_sha256="b" * 64,
                              identity=pin["commits"]["P"]["identity"],scientific_fingerprint="c" * 64) == child.uuid
    assert source_point_owner(child, native, "plan", point_id="P", commit_sha256="c" * 64,
                              identity=pin["commits"]["P"]["identity"],scientific_fingerprint="c" * 64) is None


@pytest.mark.parametrize("change", ["root", "identity", "fingerprint", "foreign_uuid", "uncommitted", "historical_checkpoint", "receipt"])
def test_collision_override_is_narrow(change):
    _, row, pin = fixture()
    if change == "root": row["data"]["artifact_root"] = "executions/E/P/attempt-4"
    if change == "identity": pin["commits"]["P"]["identity"]["attempt"] = 4
    if change == "fingerprint": pin["commits"]["P"]["identity"]["plan_fingerprint"] = "c" * 64
    if change == "foreign_uuid": pin["commits"]["P"]["identity"]["calcjob_uuid"] = "foreign"
    if change == "uncommitted": del row["data"]["result_commit"]
    if change == "historical_checkpoint": row["data"]["checkpoint_source_attempt"] = 1
    if change == "receipt": pin["commits"]["P"]["receipt_status"] = "not_available"
    with pytest.raises(ValueError): check(row, pin)


def test_collision_must_keep_scientific_fingerprint():
    _, row, pin = fixture()
    with pytest.raises(ValueError):
        validate_native_collisions([row], pin["commits"], input_attempt=2, execution_id="E",
            plan_fingerprint="a" * 64,calcjob_uuid="job2",scientific_fingerprint="c" * 64,
            result_scientific_fingerprint="d" * 64)


def test_borrowed_source_requires_exact_prior_creator_raw_and_pin():
    prior, row, prior_pin = fixture(native=1, input_attempt=1, uuid="job1")
    child, _, pin = fixture(native=1, input_attempt=2, uuid="job1")
    child.uuid = "job2"
    pin["calcjob_uuid"] = "job2"
    child.inputs.prior_archive = NS(creator=prior)
    identity = deepcopy(prior_pin["commits"]["P"]["identity"])
    assert source_point_owner(child, 1, "plan", point_id="P", commit_sha256="b" * 64, identity=identity) == "job1"
    del child.inputs.prior_archive
    assert source_point_owner(child, 1, "plan", point_id="P", commit_sha256="b" * 64, identity=identity) is None
