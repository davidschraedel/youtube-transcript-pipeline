#!/usr/bin/env bash
# Run the full test suite.
# Usage: bash scripts/test.sh [pytest args]
set -euo pipefail

uv run pytest tests/ -v "$@"
