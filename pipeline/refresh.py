"""
Incremental playlist sync: new videos, tombstones, caption retries.

Re-runs the flat-playlist snapshot against the DB to detect:
  - New video IDs → ingest path (captions + metadata)
  - Removed IDs → tombstone (removed_at = now())
  - no_subtitles videos still in playlist → re-attempt caption fetch

Newly captioned videos are transformed to Silver automatically.

Usage:
    uv run python pipeline/refresh.py                         # default sync
    uv run python pipeline/refresh.py --refresh-metadata      # also refresh metadata for all snapshot videos
    uv run python pipeline/refresh.py --playlist URL          # override PLAYLIST_URL
    uv run python pipeline/refresh.py --help
"""

import argparse
import json
import os
import sys
import time

import duckdb
from dotenv import load_dotenv

from pipeline import log, progress
from pipeline.ingest import (
    _extract_playlist_id,
    fetch_playlist_ids,
    process_chunk,
    process_metadata_chunk,
    touch_membership,
    upsert_membership,
)
from pipeline.schema import apply_schema
from pipeline.transform import run_transform

load_dotenv()

_CHUNK_SIZE_DEFAULT = 50
_SLEEP_PER_VIDEO_DEFAULT = 2
_SLEEP_BETWEEN_CHUNKS_DEFAULT = 30


def get_active_membership_ids(
    con: duckdb.DuckDBPyConnection,
    playlist_id: str,
) -> set[str]:
    rows = con.execute(
        """
        SELECT video_id FROM playlist_video_membership
        WHERE playlist_id = ? AND removed_at IS NULL
        """,
        [playlist_id],
    ).fetchall()
    return {row[0] for row in rows}


def get_no_subtitles_ids(
    con: duckdb.DuckDBPyConnection,
    snapshot_ids: list[str],
) -> list[str]:
    if not snapshot_ids:
        return []
    placeholders = ", ".join(["?"] * len(snapshot_ids))
    rows = con.execute(
        f"""
        SELECT video_id FROM videos
        WHERE fetch_status = 'no_subtitles'
          AND video_id IN ({placeholders})
        """,
        snapshot_ids,
    ).fetchall()
    return [row[0] for row in rows]


def tombstone_removed(
    con: duckdb.DuckDBPyConnection,
    playlist_id: str,
    video_ids: list[str],
) -> int:
    if not video_ids:
        return 0
    placeholders = ", ".join(["?"] * len(video_ids))
    con.execute(
        f"""
        UPDATE playlist_video_membership
        SET removed_at = now()
        WHERE playlist_id = ?
          AND video_id IN ({placeholders})
          AND removed_at IS NULL
        """,
        [playlist_id, *video_ids],
    )
    rows = con.execute(
        f"""
        SELECT COUNT(*) FROM playlist_video_membership
        WHERE playlist_id = ?
          AND video_id IN ({placeholders})
          AND removed_at IS NOT NULL
        """,
        [playlist_id, *video_ids],
    ).fetchone()
    return rows[0] if rows else 0


def _chunked_process(
    con: duckdb.DuckDBPyConnection,
    video_ids: list[str],
    playlist_id: str,
    chunk_size: int,
    sleep_per_video: float,
    sleep_between_chunks: int,
    label: str,
    show_progress: bool = True,
) -> tuple[dict[str, int], list[str]]:
    empty_totals: dict[str, int] = {
        "ok": 0,
        "no_subtitles": 0,
        "unavailable": 0,
        "rate_limited": 0,
    }
    if not video_ids:
        return empty_totals, []

    chunks = [
        video_ids[i : i + chunk_size] for i in range(0, len(video_ids), chunk_size)
    ]
    totals: dict[str, int] = dict(empty_totals)
    ok_ids: list[str] = []
    progress_offset = 0
    progress_total = len(video_ids)

    for chunk_num, chunk in enumerate(chunks, 1):
        log.info({
            "action": "process_chunk",
            "label": label,
            "chunk": chunk_num,
            "total_chunks": len(chunks),
            "size": len(chunk),
        })
        summary, chunk_ok_ids = process_chunk(
            con,
            chunk,
            playlist_id,
            sleep_per_video,
            show_progress=show_progress,
            progress_offset=progress_offset,
            progress_total=progress_total,
        )
        ok_ids.extend(chunk_ok_ids)
        progress_offset += len(chunk)
        for status, count in summary.items():
            totals[status] = totals.get(status, 0) + count

        if show_progress:
            progress.line(
                f"chunk {chunk_num}/{len(chunks)} done: "
                f"ok={summary['ok']} no_subtitles={summary['no_subtitles']} "
                f"unavailable={summary['unavailable']} rate_limited={summary['rate_limited']}"
            )

        if chunk_num < len(chunks):
            if show_progress:
                progress.line(f"waiting {sleep_between_chunks}s before next chunk...")
            log.info({"action": "sleep_between_chunks", "seconds": sleep_between_chunks})
            time.sleep(sleep_between_chunks)

    return totals, ok_ids


_MAX_TOMBSTONE_WITHOUT_OVERRIDE = 20


def run_refresh(
    playlist_url: str,
    db_path: str,
    chunk_size: int,
    sleep_per_video: float,
    sleep_between_chunks: int,
    refresh_metadata: bool = False,
    force_tombstone: bool = False,
) -> None:
    apply_schema(db_path)

    log.info({"action": "run_refresh", "input": {"playlist_url": playlist_url}})

    progress.section("Playlist snapshot")
    progress.line("Fetching video list (yt-dlp)...")
    snapshot_ids = fetch_playlist_ids(playlist_url)
    progress.line(f"{len(snapshot_ids)} videos in playlist")

    snapshot_set = set(snapshot_ids)
    playlist_id = _extract_playlist_id(playlist_url)

    with duckdb.connect(db_path) as con:
        existing_ids = {
            row[0]
            for row in con.execute("SELECT video_id FROM videos").fetchall()
        }

        new_ids = [vid for vid in snapshot_ids if vid not in existing_ids]
        active_members = get_active_membership_ids(con, playlist_id)
        tombstone_ids = sorted(active_members - snapshot_set)
        retry_ids = get_no_subtitles_ids(con, snapshot_ids)

        # Scheduled for caption fetch this run (new + retry), not necessarily successful.
        fetched_ids = set(new_ids) | set(retry_ids)
        touch_ids = [
            vid for vid in snapshot_ids
            if vid in existing_ids and vid not in fetched_ids
        ]
        metadata_ids = [vid for vid in snapshot_ids if vid not in fetched_ids]

        progress.section("Diff")
        progress.line(
            f"new: {len(new_ids)} | tombstone: {len(tombstone_ids)} | "
            f"retry no_subtitles: {len(retry_ids)} | touch only: {len(touch_ids)}"
        )

        log.info({
            "action": "refresh_diff",
            "result": {
                "snapshot": len(snapshot_ids),
                "new": len(new_ids),
                "tombstone": len(tombstone_ids),
                "retry_no_subtitles": len(retry_ids),
                "touch": len(touch_ids),
            },
        })

        # Circuit breaker: a snapshot that removes more than MAX_TOMBSTONE_WITHOUT_OVERRIDE
        # videos almost certainly reflects a partial/truncated API response, not a real
        # playlist change. Use --force-tombstone to override when a large removal is intentional.
        if len(tombstone_ids) > _MAX_TOMBSTONE_WITHOUT_OVERRIDE and not force_tombstone:
            log.error({
                "action": "run_refresh",
                "error": "circuit breaker tripped",
                "tombstone_count": len(tombstone_ids),
                "max_allowed": _MAX_TOMBSTONE_WITHOUT_OVERRIDE,
                "hint": "pass --force-tombstone to override",
            })
            progress.section("Aborted")
            progress.line(
                f"tombstone count ({len(tombstone_ids)}) exceeds limit ({_MAX_TOMBSTONE_WITHOUT_OVERRIDE}). "
                f"Likely a partial playlist fetch. Re-run with --force-tombstone to override."
            )
            sys.exit(1)

        tombstoned = 0
        if tombstone_ids:
            progress.section("Tombstone")
            tombstoned = tombstone_removed(con, playlist_id, tombstone_ids)
            progress.line(f"{tombstoned} removed from playlist")
        else:
            progress.skip("Tombstone")

        if touch_ids:
            progress.section("Touch membership")
            touch_membership(con, playlist_id, touch_ids)
            progress.line(f"{len(touch_ids)} videos updated")
        else:
            progress.skip("Touch membership")

        added = 0
        captioned_ids: list[str] = []

        if new_ids:
            progress.section(f"New videos ({len(new_ids)})")
            _, new_ok_ids = _chunked_process(
                con,
                new_ids,
                playlist_id,
                chunk_size,
                sleep_per_video,
                sleep_between_chunks,
                label="new",
            )
            added = len(new_ids)
            captioned_ids.extend(new_ok_ids)
        else:
            progress.skip("New videos")

        if retry_ids:
            progress.section(f"Caption retry ({len(retry_ids)})")
            _, retry_ok_ids = _chunked_process(
                con,
                retry_ids,
                playlist_id,
                chunk_size,
                sleep_per_video,
                sleep_between_chunks,
                label="retry",
            )
            captioned_ids.extend(retry_ok_ids)
        else:
            progress.skip("Caption retry")

        newly_captioned = len(captioned_ids)

        if refresh_metadata:
            if metadata_ids:
                progress.section(f"Metadata refresh ({len(metadata_ids)})")
                meta_chunks = [
                    metadata_ids[i : i + chunk_size]
                    for i in range(0, len(metadata_ids), chunk_size)
                ]
                meta_totals = {"updated": 0, "skipped": 0, "failed": 0}
                for chunk_num, chunk in enumerate(meta_chunks, 1):
                    log.info({
                        "action": "process_metadata_chunk",
                        "chunk": chunk_num,
                        "total_chunks": len(meta_chunks),
                        "size": len(chunk),
                    })
                    summary = process_metadata_chunk(con, chunk, sleep_per_video)
                    for key, count in summary.items():
                        meta_totals[key] = meta_totals.get(key, 0) + count
                    progress.line(
                        f"chunk {chunk_num}/{len(meta_chunks)} done: "
                        f"updated={summary['updated']} skipped={summary['skipped']} "
                        f"failed={summary['failed']}"
                    )
                    if chunk_num < len(meta_chunks):
                        progress.line(
                            f"waiting {sleep_between_chunks}s before next chunk..."
                        )
                        time.sleep(sleep_between_chunks)
                log.info({"action": "refresh_metadata", "result": meta_totals})
            else:
                progress.skip("Metadata refresh")

    if captioned_ids:
        progress.section(f"Transform ({len(captioned_ids)})")
        run_transform(db_path, video_ids=captioned_ids, print_summary=False)
        progress.line(f"{len(captioned_ids)} transformed")
    else:
        progress.skip("Transform")

    summary = {
        "action": "run_refresh",
        "result": {
            "added": added,
            "tombstoned": tombstoned,
            "newly_captioned": newly_captioned,
            "status": "complete",
        },
    }

    progress.section("Done")
    progress.line(
        f"added {added}, tombstoned {tombstoned}, newly captioned {newly_captioned}"
    )
    print(json.dumps(summary))
    log.info(summary)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Incremental playlist sync: new videos, tombstones, caption retries."
    )
    parser.add_argument(
        "--playlist",
        help="YouTube playlist URL (overrides PLAYLIST_URL env var).",
    )
    parser.add_argument(
        "--refresh-metadata",
        action="store_true",
        help="Refresh metadata for all videos in the current playlist snapshot.",
    )
    parser.add_argument(
        "--force-tombstone",
        action="store_true",
        help=(
            f"Override the circuit breaker and allow tombstoning more than "
            f"{_MAX_TOMBSTONE_WITHOUT_OVERRIDE} videos. Use only when a large "
            f"playlist removal is intentional."
        ),
    )
    args = parser.parse_args()

    playlist_url = args.playlist or os.getenv("PLAYLIST_URL")
    if not playlist_url:
        log.error({
            "action": "main",
            "error": "PLAYLIST_URL not set",
            "hint": "pass --playlist or add to .env",
        })
        sys.exit(1)

    db_path = os.getenv("DB_PATH", "youtube.duckdb")
    chunk_size = int(os.getenv("CHUNK_SIZE", _CHUNK_SIZE_DEFAULT))
    sleep_per_video = float(os.getenv("SLEEP_PER_VIDEO", _SLEEP_PER_VIDEO_DEFAULT))
    sleep_between_chunks = int(
        os.getenv("SLEEP_BETWEEN_CHUNKS", _SLEEP_BETWEEN_CHUNKS_DEFAULT)
    )

    run_refresh(
        playlist_url,
        db_path,
        chunk_size,
        sleep_per_video,
        sleep_between_chunks,
        refresh_metadata=args.refresh_metadata,
        force_tombstone=args.force_tombstone,
    )


if __name__ == "__main__":
    main()
