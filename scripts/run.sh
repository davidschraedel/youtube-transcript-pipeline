#!/usr/bin/env bash
# Run the ingest pipeline.
# Usage: bash scripts/run.sh [--playlist URL] [args...]
set -euo pipefail

uv run python pipeline/ingest.py "$@"
