#!/usr/bin/env bash
# Run linting. Requires ruff: uv add --dev ruff
# Usage: bash scripts/lint.sh
set -euo pipefail

uv run ruff check pipeline/ tests/
