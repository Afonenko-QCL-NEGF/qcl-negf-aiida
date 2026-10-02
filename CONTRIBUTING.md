# Contributing

Keep scientific inputs immutable. Changes to workflow orchestration must preserve
the exact plan JSON bytes, and never rewrite physical parameters to make a failed
calculation pass. Numerical algorithms belong to `QCLNEGF.jl`; shared schemas
belong to `qcl-negf-contracts`.

Use the Python 3.14 workspace in
[`qcl-negf`](https://github.com/Afonenko-QCL-NEGF/qcl-negf), which owns the dependency lock
and complete component source graph. For standalone development, install the
matching contracts/results wheels and this package with `uv pip install -e '.[test,build]'`.
Run `pytest` for the complete suite. For work on
pure boundaries, `pytest -m 'not integration'` runs tests that need no AiiDA
profile. Integration tests use temporary SQLite storage and a synthetic external
executable. A normal Linux process namespace and writable temporary directory
are required for AiiDA's profile locks and local scheduler.

The integration repository owns typed CI orchestration and self-hosted runner
workflows. This package keeps no second set of component commit pins or Python
locks. Build wheel and source distributions with `python -m build` after tests.

Check scientific-failure semantics, malformed result rejection, continuation
boundaries, and precision preservation when changing the plugin. A valid schema
alone is insufficient: result identities and operating conditions must also
agree with their input plan.

Node-local scratch and verified staging belong to `QCLNEGFRunner.jl`. The AiiDA
plugin passes a trusted path as a separate argument and stores it as provenance;
it must not recreate staging or generate another shell-based resource manager.
