# Contributing

For documentation-only text or link corrections, review the diff and check the
changed links, anchors and formatting. This does not require Julia, package
installation, a full test suite or documentation generation. Record numerical and
rendering checks that were not run. Executable examples, documentation build
recipes and public API changes need the relevant checks for their behavior.

Keep scientific inputs immutable. Changes to workflow orchestration must preserve
the exact plan JSON bytes, and never rewrite physical parameters to make a failed
calculation pass. Numerical algorithms belong to `QCLNEGF.jl`; shared schemas
belong to `qcl-negf-contracts`.

Use the Python 3.14 workspace in
[`qcl-negf`](https://github.com/Afonenko-QCL-NEGF/qcl-negf), which owns the dependency lock
and complete component source graph. For code changes, select the relevant pytest
cases in that prepared environment, including failure and healthy-control cases.
For standalone environment preparation, install the matching contracts/results
wheels and this package with `uv pip install -e '.[test,build]'`; installation is a
separately assigned stage with a finite shared budget.

Run `pytest` for the complete suite in its assigned validation stage. For work on
pure boundaries, `pytest -m 'not integration'` selects tests that need no AiiDA
profile; it is still a test group, not a source-only check. Integration tests use
temporary SQLite storage and a synthetic external executable. A normal Linux
process namespace and writable temporary directory are required for AiiDA's
profile locks and local scheduler. Bind the selected checks and resource limits
before execution; a missing environment remains a reported limitation.

The integration repository owns typed CI orchestration and self-hosted runner
workflows. This package keeps no second set of component commit pins or Python
locks. For an assigned distribution build, build wheel and source distributions
with `python -m build` after the relevant tests, within the declared build budget.

Check scientific-failure semantics, malformed result rejection, continuation
boundaries, and precision preservation when changing the plugin. A valid schema
alone is insufficient: result identities and operating conditions must also
agree with their input plan.

Node-local scratch and verified staging belong to `QCLNEGFRunner.jl`. The AiiDA
plugin passes a trusted path as a separate argument and stores it as provenance;
it must not recreate staging or generate another shell-based resource manager.
