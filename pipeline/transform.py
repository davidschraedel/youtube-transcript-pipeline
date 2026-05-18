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
import os
import sys

import duckdb
from dotenv import load_dotenv

from pipeline.utils import dedupe_repeated_phrases, parse_vtt

load_dotenv()


def _get_rows(con: duckdb.DuckDBPyConnection, force: bool) -> list[tuple[str, str, str]]:
    """Return (video_id, raw_vtt, source_language) tuples to process."""
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


def run_transform(db_path: str, force: bool = False) -> None:
    with duckdb.connect(db_path) as con:
        rows = _get_rows(con, force)

        if not rows:
            print("Nothing to process.")
            return

        print(f"Processing {len(rows)} row(s).")

        for video_id, raw_vtt, source_language in rows:
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

        print(f"Done. {len(rows)} row(s) written to transcripts_silver.")


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
        print(f"ERROR: database not found at {db_path!r}", file=sys.stderr)
        sys.exit(1)

    run_transform(db_path, force=args.force)


if __name__ == "__main__":
    main()
