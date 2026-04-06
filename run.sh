#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_ACTIVATE="$SCRIPT_DIR/.venv/bin/activate"

if [[ ! -f "$VENV_ACTIVATE" ]]; then
  echo "Missing virtual environment at $SCRIPT_DIR/.venv"
  echo "Create it with:"
  echo "  python3 -m venv .venv"
  echo "  source .venv/bin/activate"
  echo "  pip install -e ."
  exit 1
fi

source "$VENV_ACTIVATE"
cd "$SCRIPT_DIR"

fix_duckdb_signature_if_needed() {
  local duckdb_ext
  duckdb_ext="$(find "$SCRIPT_DIR/.venv/lib" -path '*/site-packages/_duckdb*.so' -print -quit 2>/dev/null || true)"

  if [[ -z "$duckdb_ext" ]] || [[ ! -f "$duckdb_ext" ]]; then
    return 0
  fi

  local probe_output
  if probe_output="$("$SCRIPT_DIR/.venv/bin/python" -c 'import duckdb' 2>&1)"; then
    return 0
  fi

  if [[ "$probe_output" == *"library load denied by system policy"* ]] || [[ "$probe_output" == *"code signature"* ]]; then
    echo "DuckDB was blocked by macOS system policy. Re-signing local extension..."
    codesign --force --sign - "$duckdb_ext"
    "$SCRIPT_DIR/.venv/bin/python" -c 'import duckdb' >/dev/null
    return 0
  fi

  echo "$probe_output" >&2
  return 1
}

fix_duckdb_signature_if_needed
exec streamlit run src/trailintel/streamlit_app.py "$@"
