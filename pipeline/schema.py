"""
Apply DDL to the local DuckDB database.

Run directly to create/verify all tables:
    uv run python pipeline/schema.py

Idempotent — uses CREATE TABLE IF NOT EXISTS throughout.
"""

import os
import sys

import duckdb
from dotenv import load_dotenv

from pipeline import log

load_dotenv()

DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS videos (
        video_id        VARCHAR PRIMARY KEY,
        title           VARCHAR,
        channel_name    VARCHAR,
        channel_id      VARCHAR,
        upload_date     DATE,
        duration        INTEGER,
        view_count      BIGINT,
        fetch_status    VARCHAR NOT NULL CHECK (
                            fetch_status IN ('ok', 'no_subtitles', 'unavailable', 'rate_limited')
                        ),
        ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS playlist_video_membership (
        playlist_id     VARCHAR NOT NULL,
        video_id        VARCHAR NOT NULL,
        first_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        removed_at      TIMESTAMPTZ,
        PRIMARY KEY (playlist_id, video_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS transcripts_bronze (
        video_id        VARCHAR PRIMARY KEY,
        raw_vtt         VARCHAR NOT NULL,
        source_language VARCHAR NOT NULL,
        fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS transcripts_silver (
        video_id        VARCHAR PRIMARY KEY,
        full_text       VARCHAR NOT NULL,
        source_language VARCHAR NOT NULL,
        transformed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
]

EXPECTED_TABLES = {
    "videos",
    "playlist_video_membership",
    "transcripts_bronze",
    "transcripts_silver",
}


def apply_schema(db_path: str) -> None:
    with duckdb.connect(db_path) as con:
        for stmt in DDL_STATEMENTS:
            con.execute(stmt)
    log.info({"action": "apply_schema", "result": {"status": "ok", "db_path": db_path}})


def smoke_test(db_path: str) -> None:
    with duckdb.connect(db_path) as con:
        result = con.execute("SELECT 1").fetchone()
        assert result == (1,), f"SELECT 1 returned unexpected result: {result}"

        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        missing = EXPECTED_TABLES - tables
        if missing:
            log.error({"action": "smoke_test", "error": "missing tables", "tables": sorted(missing)})
            sys.exit(1)

    log.info({"action": "smoke_test", "result": {"status": "ok", "tables": sorted(tables)}})


if __name__ == "__main__":
    db_path = os.getenv("DB_PATH", "youtube.duckdb")
    apply_schema(db_path)
    smoke_test(db_path)
