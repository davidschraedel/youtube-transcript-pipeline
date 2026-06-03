#!/usr/bin/env bash
# Phase 4 validation: silver row counts and VTT artifact scan.
# Usage: bash scripts/validate_transform.sh
set -euo pipefail

uv run python scripts/validate_transform.py
