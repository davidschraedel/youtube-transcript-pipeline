"""
Bronze → Silver transform.

Reads transcripts_bronze rows that do not yet have a corresponding
transcripts_silver row and writes cleaned, deduplicated plain text to
transcripts_silver.

Usage:
    uv run python pipeline/transform.py            # process only new rows
    uv run python pipeline/transform.py --force    # reprocess all rows
    uv run python pipeline/transform.py --help
"""

import argparse
import json
import os
import sys

import duckdb
from dotenv import load_dotenv

from pipeline import log
from pipeline.utils import dedupe_repeated_phrases, parse_vtt

load_dotenv()


def _get_rows(
    con: duckdb.DuckDBPyConnection,
    force: bool,
    video_ids: list[str] | None = None,
) -> list[tuple[str, str, str]]:
    """Return (video_id, raw_vtt, source_language) tuples to process."""
    if video_ids:
        placeholders = ", ".join(["?"] * len(video_ids))
        return con.execute(
            f"""
            SELECT video_id, raw_vtt, source_language
            FROM transcripts_bronze
            WHERE video_id IN ({placeholders})
            """,
            video_ids,
        ).fetchall()

    if force:
        return con.execute(
            "SELECT video_id, raw_vtt, source_language FROM transcripts_bronze"
        ).fetchall()

    return con.execute(
        """
        SELECT b.video_id, b.raw_vtt, b.source_language
        FROM transcripts_bronze b
        LEFT JOIN transcripts_silver s USING (video_id)
        WHERE s.video_id IS NULL
        """
    ).fetchall()


def run_transform(
    db_path: str,
    force: bool = False,
    video_ids: list[str] | None = None,
    print_summary: bool = True,
) -> None:
    with duckdb.connect(db_path) as con:
        rows = _get_rows(con, force, video_ids)

        if not rows:
            result = {"action": "run_transform", "result": {"processed": 0, "status": "nothing_to_process"}}
            if print_summary:
                print(json.dumps(result))
            log.info(result)
            return

        log.info({
            "action": "run_transform",
            "input": {"rows": len(rows), "force": force, "video_ids": video_ids},
        })

        for video_id, raw_vtt, source_language in rows:
            log.debug({"action": "transform_row", "video_id": video_id})
            cleaned = parse_vtt(raw_vtt)
            deduped = dedupe_repeated_phrases(cleaned)

            con.execute(
                "DELETE FROM transcripts_silver WHERE video_id = ?",
                [video_id],
            )
            con.execute(
                """
                INSERT INTO transcripts_silver (video_id, full_text, source_language)
                VALUES (?, ?, ?)
                """,
                [video_id, deduped, source_language],
            )

        result = {"action": "run_transform", "result": {"processed": len(rows), "status": "complete"}}
        if print_summary:
            print(json.dumps(result))
        log.info(result)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Transform Bronze VTT rows into Silver cleaned text."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess all Bronze rows, including those already in Silver.",
    )
    args = parser.parse_args()

    db_path = os.getenv("DB_PATH", "youtube.duckdb")

    if not os.path.exists(db_path):
        log.error({"action": "main", "error": "database not found", "db_path": db_path})
        sys.exit(1)

    run_transform(db_path, force=args.force)


if __name__ == "__main__":
    main()
