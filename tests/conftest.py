"""Isolated AiiDA storage and a synthetic external scientific executable."""

import json
import sys
from pathlib import Path

import pytest
from aiida.tools.pytest_fixtures.configuration import aiida_config_factory, aiida_profile_factory
from aiida.tools.pytest_fixtures.orm import (
    aiida_code, aiida_code_installed, aiida_computer, aiida_computer_local, aiida_localhost,
)


@pytest.fixture
def aiida_profile_clean(aiida_config_factory, aiida_profile_factory, tmp_path):
    with aiida_config_factory(tmp_path) as config:
        with aiida_profile_factory(config) as profile:
            yield profile


@pytest.fixture
def plan():
    # Synthetic transport identities: not a solver-generated physical reference.
    return json.loads((Path(__file__).parent / "fixtures/plan.json").read_text())


@pytest.fixture
def code(aiida_profile_clean, aiida_code_installed, tmp_path):
    script = tmp_path / "synthetic-solver"
    program = (Path(__file__).parent / "synthetic_solver.py").read_text()
    script.write_text(f"#!{sys.executable}\n" + program)
    script.chmod(0o755)
    return aiida_code_installed(filepath_executable=str(script), default_calc_job_plugin="qcl_negf.execution")
