#!/usr/bin/env bash
# Sync project dependencies.
# Usage: bash scripts/build.sh
set -euo pipefail

uv sync
echo '{"action":"build","result":{"status":"ok"}}'
