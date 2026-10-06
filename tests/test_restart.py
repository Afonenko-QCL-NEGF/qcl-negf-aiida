"""Finite infrastructure retries never change the scientific problem."""

from types import SimpleNamespace

import pytest
from aiida.engine import BaseRestartWorkChain
from aiida_qcl_negf import workflow
from test_export_plan import source


def test_execution_restart_uses_aiida_framework():
    assert hasattr(workflow, "QCLExecutionRestartWorkChain")
    assert issubclass(workflow.QCLExecutionRestartWorkChain, BaseRestartWorkChain)


@pytest.mark.parametrize("status, stopped, recovery, expected", [
    (304, True, True, "retry"),
    (304, True, False, "invalid_recovery"),
    (300, True, True, "retry"),
    (300, False, True, "ambiguous"),
    (300, True, False, "no_recovery"),
    (140, True, True, "retry"),
    (140, False, True, "ambiguous"),
    (303, True, True, "scientific_terminal"),
    (301, True, True, "invalid_result"),
    (0, True, False, "completed"),
])
def test_attempt_decision_requires_stopped_owner_and_verified_state(status, stopped, recovery, expected):
    assert hasattr(workflow, "attempt_decision")
    assert workflow.attempt_decision(status, scheduler_stopped=stopped, recovery_available=recovery) == expected


def test_exact_attempt_selection_rejects_ambiguous_execution(source):
    from aiida_qcl_negf import service
    _, _, _, child = source
    child.uuid = "first"
    child.inputs.attempt = SimpleNamespace(value=1)
    later = SimpleNamespace(uuid="second", inputs=SimpleNamespace(
        execution_id=SimpleNamespace(value="execution-1"), attempt=SimpleNamespace(value=2)), outputs=child.outputs)
    original = service._children
    service._children = lambda _: [child, later]
    try:
        with pytest.raises(ValueError, match="attempt"):
            service.get_export_plan("run", "execution-1")
        assert service.get_export_plan("run", "execution-1", attempt=2)["source"].startswith("aiida.retrieved")
    finally:
        service._children = original
