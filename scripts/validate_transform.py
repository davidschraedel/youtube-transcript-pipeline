"""
Phase 4 validation: Bronze → Silver row parity and prose quality checks.

Usage:
    uv run python scripts/validate_transform.py
    bash scripts/validate_transform.sh

Exits 0 and prints JSON on success; exits 1 with error details on failure.
Requires DB_PATH in environment (or .env via python-dotenv).
"""

from __future__ import annotations

import json
import os
import re
import sys

import duckdb
from dotenv import load_dotenv

# VTT artifacts that must not appear in Silver full_text
_ARTIFACT_PATTERNS: list[tuple[str, str]] = [
    ("webvtt_header", r"WEBVTT"),
    ("timestamp_arrow", r"\d{2}:\d{2}:\d{2}\.\d{3}\s*-->"),
    ("caption_tag", r"</?c>"),
    ("inline_timestamp", r"<\d{2}:\d{2}:\d{2}\.\d+>"),
    ("vtt_kind_header", r"Kind:"),
]


def validate_transform(db_path: str) -> dict:
    """Run count and artifact checks; return result dict."""
    errors: list[str] = []
    artifact_hits: dict[str, int] = {}

    with duckdb.connect(db_path, read_only=True) as con:
        bronze_count = con.execute("SELECT COUNT(*) FROM transcripts_bronze").fetchone()[0]
        silver_count = con.execute("SELECT COUNT(*) FROM transcripts_silver").fetchone()[0]
        ok_count = con.execute(
            "SELECT COUNT(*) FROM videos WHERE fetch_status = 'ok'"
        ).fetchone()[0]
        empty_silver = con.execute(
            "SELECT COUNT(*) FROM transcripts_silver "
            "WHERE full_text IS NULL OR trim(full_text) = ''"
        ).fetchone()[0]

        if silver_count != ok_count:
            errors.append(
                f"silver count ({silver_count}) != ok videos ({ok_count})"
            )
        if bronze_count != ok_count:
            errors.append(
                f"bronze count ({bronze_count}) != ok videos ({ok_count})"
            )
        if empty_silver:
            errors.append(f"{empty_silver} silver row(s) have empty full_text")

        for name, pattern in _ARTIFACT_PATTERNS:
            hits = con.execute(
                "SELECT COUNT(*) FROM transcripts_silver "
                "WHERE regexp_matches(full_text, ?)",
                [pattern],
            ).fetchone()[0]
            artifact_hits[name] = hits
            if hits:
                errors.append(f"{hits} silver row(s) contain {name}")

    status = "ok" if not errors else "failed"
    return {
        "action": "validate_transform",
        "result": {
            "status": status,
            "bronze_count": bronze_count,
            "silver_count": silver_count,
            "ok_videos_count": ok_count,
            "empty_silver": empty_silver,
            "artifact_hits": artifact_hits,
            "errors": errors,
        },
    }


def main() -> None:
    load_dotenv()
    db_path = os.getenv("DB_PATH", "youtube.duckdb")

    if not os.path.exists(db_path):
        print(
            json.dumps(
                {
                    "action": "validate_transform",
                    "error": "database not found",
                    "db_path": db_path,
                }
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    report = validate_transform(db_path)
    print(json.dumps(report))

    if report["result"]["status"] != "ok":
        sys.exit(1)


if __name__ == "__main__":
    main()
