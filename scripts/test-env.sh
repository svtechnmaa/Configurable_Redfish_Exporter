#!/usr/bin/env bash
# Shared local validation environment for Claude, Codex, and human contributors.

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="$PROJECT_DIR/.venv-test"
VENV_PYTHON="$VENV_DIR/bin/python"
SYSTEM_PYTHON="${REDFISH_TEST_PYTHON:-python3}"

usage() {
    cat <<'EOF'
Usage: scripts/test-env.sh <command> [args]

Commands:
  doctor          Check Python, shared venv, and required imports
  setup           Create/update .venv-test and install test dependencies
  syntax          Parse Python source/tests without importing or writing bytecode
  pytest          Run pytest; remaining arguments are passed to pytest
  pytest-collect  Collect and list tests without executing them
  verify          Run doctor, syntax, and the full pytest suite

Only setup installs dependencies. Run setup only with explicit user approval.
EOF
}

require_shared_env() {
    if [[ ! -x "$VENV_PYTHON" ]]; then
        echo "ERROR: shared test environment is missing: $VENV_DIR" >&2
        echo "Run 'scripts/test-env.sh setup' only after explicit setup approval." >&2
        return 2
    fi
}

doctor() {
    # Every fallible step below is explicitly checked and `return`s (never
    # a bare `exit`, and never left to an implicit `set -e` trigger) so
    # `doctor` behaves identically whether invoked standalone (where an
    # unhandled failure should end the whole script, which `set -e` still
    # does once this function itself returns non-zero) or as `doctor ||
    # overall_status=$?` inside `verify` (where `errexit` is suspended for
    # this call's whole execution, so only an explicit `return` stops the
    # function at the point of failure instead of silently falling through
    # to later checks against a broken/absent environment).
    "$SYSTEM_PYTHON" - <<'PY'
import sys

if sys.version_info < (3, 12):
    raise SystemExit(f"ERROR: Python 3.12+ is required; found {sys.version.split()[0]}")
print(f"system Python: {sys.version.split()[0]}")
PY
    [[ $? -eq 0 ]] || return 2

    require_shared_env || return $?
    "$VENV_PYTHON" - <<'PY'
from importlib import import_module
import sys

required = (
    "aiohttp",
    "fastapi",
    "jinja2",
    "jsonpath_ng",
    "prometheus_client",
    "pytest",
    "redfish_collector.main",
    # Round-of-repair: `requests` was removed from this list — neither
    # `setup.py`'s `install_requires` nor `requirements-test.txt` declares
    # it (the in-process HTTP contract tests use `httpx2` via starlette's
    # TestClient instead, per `requirements-test.txt`'s own comment), and
    # no source/test in this repo imports it. A genuinely fresh
    # `scripts/test-env.sh setup` would never install it, so requiring it
    # importable here was itself stale — `requests 2.25.1` only exists in
    # the current shared `.venv-test` as leftover drift from some earlier
    # environment state, and is exactly what the `pip check` step below
    # flags as incoherent (`idna<3` vs. the newer `idna` actually
    # installed).
    "uvicorn",
    "yaml",
)
missing = []
for name in required:
    try:
        import_module(name)
    except Exception as exc:
        missing.append(f"{name}: {type(exc).__name__}: {exc}")

if missing:
    print("ERROR: shared environment has missing/broken imports:", file=sys.stderr)
    for item in missing:
        print(f"  - {item}", file=sys.stderr)
    raise SystemExit(2)

print(f"shared Python: {sys.version.split()[0]}")
print("shared imports: OK")
PY
    [[ $? -eq 0 ]] || return 2

    # Round-of-repair: importability alone does not prove the environment's
    # own declared dependencies are mutually coherent — `requests==2.25.1`
    # importing fine says nothing about it silently requiring `idna<3`
    # while `idna 3.19` is what's actually installed (observed evidence).
    # `pip check` is the one command that verifies every installed
    # distribution's own declared requirements against what else is
    # installed, so run it here too instead of leaving that gap for
    # `verify`/production to discover first.
    set +e
    pip_check_output="$("$VENV_PYTHON" -m pip check 2>&1)"
    pip_check_status=$?
    set -e
    if [[ $pip_check_status -ne 0 ]]; then
        echo "ERROR: pip check found dependency incoherence in $VENV_DIR:" >&2
        echo "$pip_check_output" >&2
        echo "This session may not run 'scripts/test-env.sh setup' without explicit user authorization." >&2
        echo "Report this as pip check: OPEN/FAIL — only an explicitly authorized setup can reconcile it." >&2
        return 2
    fi
    echo "pip check: OK"
}

setup_env() {
    "$SYSTEM_PYTHON" -m venv "$VENV_DIR"
    "$VENV_PYTHON" -m pip install --upgrade pip
    "$VENV_PYTHON" -m pip install --editable "$PROJECT_DIR"
    "$VENV_PYTHON" -m pip install --requirement "$PROJECT_DIR/requirements-test.txt"
    doctor
}

syntax_check() {
    "$SYSTEM_PYTHON" - "$PROJECT_DIR" <<'PY'
import ast
from pathlib import Path
import sys

root = Path(sys.argv[1])
files = sorted((root / "src").rglob("*.py")) + sorted((root / "tests").rglob("*.py"))
if not files:
    raise SystemExit("ERROR: no Python files found")

for file_path in files:
    ast.parse(file_path.read_text(encoding="utf-8"), filename=str(file_path))
print(f"syntax: OK ({len(files)} Python files)")
PY
}

run_pytest() {
    require_shared_env || return $?
    cd "$PROJECT_DIR"
    "$VENV_PYTHON" -m pytest "$@"
}

command="${1:-}"
if [[ -z "$command" ]]; then
    usage
    exit 2
fi
shift

case "$command" in
    doctor)
        doctor
        ;;
    setup)
        setup_env
        ;;
    syntax)
        syntax_check
        ;;
    pytest)
        run_pytest "$@"
        ;;
    pytest-collect)
        run_pytest --collect-only "$@"
        ;;
    verify)
        # Round-of-repair: `doctor` now exits non-zero on a `pip check`
        # dependency-incoherence finding, not only on a broken/missing
        # import. Under the old unconditional `set -e` sequencing, that
        # made `verify` abort BEFORE ever running syntax/pytest at all —
        # so an unrelated, already-diagnosed environment-drift issue (an
        # orphan `requests` install nothing in this repo declares or uses)
        # would silently prevent the user from getting any real test
        # evidence from a `verify` run, the opposite of "never report a
        # skipped/interrupted class as passed." Every stage still runs and
        # is still reported; the FINAL exit status is non-zero if ANY
        # stage (doctor, syntax, or pytest) failed.
        overall_status=0
        doctor || overall_status=$?
        syntax_check || overall_status=$?
        run_pytest || overall_status=$?
        if [[ $overall_status -ne 0 ]]; then
            echo "ERROR: verify FAILED — see the doctor/syntax/pytest output above for which stage(s)." >&2
        fi
        exit "$overall_status"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "ERROR: unknown command: $command" >&2
        usage >&2
        exit 2
        ;;
esac
