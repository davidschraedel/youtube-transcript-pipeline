"""
Ingest a YouTube playlist into Bronze tables.

Fetches all video IDs from the playlist, diffs against the existing DB,
and downloads captions + metadata only for new videos. All writes are
wrapped in per-chunk transactions and the script is safe to re-run.

Usage:
    uv run python pipeline/ingest.py                    # uses PLAYLIST_URL from .env
    uv run python pipeline/ingest.py --playlist URL     # override playlist URL
    uv run python pipeline/ingest.py --help
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
import random
from datetime import datetime

import duckdb
from dotenv import load_dotenv

from pipeline import log, progress
from pipeline.schema import apply_schema
from pipeline.utils import classify_failure

load_dotenv()

_CHUNK_SIZE_DEFAULT = 50
_SLEEP_PER_VIDEO_DEFAULT = 2
_SLEEP_BETWEEN_CHUNKS_DEFAULT = 30


# ---------------------------------------------------------------------------
# Playlist ID discovery
# ---------------------------------------------------------------------------


def fetch_playlist_ids(playlist_url: str) -> list[str]:
    """Return all video IDs in the playlist via yt-dlp --flat-playlist -j.

    Prints one JSON object per line to stdout; we extract the 'id' field.
    """
    result = subprocess.run(
        ["yt-dlp", "--flat-playlist", "-j", "--no-warnings", playlist_url],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    ids: list[str] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            vid_id = data.get("id")
            if vid_id:
                ids.append(vid_id)
        except json.JSONDecodeError:
            continue

    if result.returncode != 0:
        if not ids:
            log.error({
                "action": "fetch_playlist_ids",
                "error": "yt-dlp flat-playlist failed with no output",
                "exit_code": result.returncode,
                "stderr": result.stderr.strip(),
            })
            sys.exit(1)
        else:
            log.error({
                "action": "fetch_playlist_ids",
                "error": "yt-dlp flat-playlist failed with partial output — aborting to prevent false tombstones",
                "exit_code": result.returncode,
                "ids_parsed": len(ids),
                "stderr": result.stderr.strip(),
            })
            sys.exit(1)

    return ids


def _extract_playlist_id(playlist_url: str) -> str:
    """Extract the playlist ID from a YouTube playlist URL."""
    if "list=" in playlist_url:
        return playlist_url.split("list=")[1].split("&")[0]
    return playlist_url


# ---------------------------------------------------------------------------
# Per-video caption + metadata download
# ---------------------------------------------------------------------------


def fetch_video_metadata(video_id: str, tmp_dir: str) -> tuple[int, str]:
    """Download info JSON for a single video (no captions).

    Files are written to tmp_dir using %(id)s as the filename stem.
    Returns (returncode, stderr).
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    output_tmpl = os.path.join(tmp_dir, "%(id)s.%(ext)s")
    result = subprocess.run(
        [
            "yt-dlp",
            "--skip-download",
            "--write-info-json",
            "--no-warnings",
            "--no-progress",
            "-o", output_tmpl,
            url,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.returncode, result.stderr


def fetch_video_captions(video_id: str, tmp_dir: str) -> tuple[int, str]:
    """Download auto-generated captions and info JSON for a single video.

    Files are written to tmp_dir using %(id)s as the filename stem.
    Returns (returncode, stderr).
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    output_tmpl = os.path.join(tmp_dir, "%(id)s.%(ext)s")
    result = subprocess.run(
        [
            "yt-dlp",
            "--skip-download",
            "--write-auto-sub",
            "--sub-lang", "en",
            "--write-info-json",
            "--no-warnings",
            "--no-progress",
            "-o", output_tmpl,
            url,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.returncode, result.stderr


def find_vtt_file(tmp_dir: str, video_id: str) -> tuple[str | None, str | None]:
    """Return (filepath, language_code) for the first VTT file in tmp_dir.

    Matches any file ending in .vtt that contains the video_id. Language code
    is extracted from the filename (e.g. VIDEO_ID.en.vtt → 'en').
    """
    try:
        entries = os.listdir(tmp_dir)
    except OSError:
        return None, None

    for fname in entries:
        if not fname.endswith(".vtt"):
            continue
        if video_id not in fname:
            continue
        path = os.path.join(tmp_dir, fname)
        # Extract language: strip .vtt, then strip VIDEO_ID. prefix
        name_no_ext = fname[:-4]
        if name_no_ext.startswith(video_id + "."):
            lang = name_no_ext[len(video_id) + 1:]
        else:
            lang = "en"
        return path, lang

    return None, None


def find_info_json(tmp_dir: str, video_id: str) -> str | None:
    """Return the path to the info JSON file, or None if not found."""
    candidate = os.path.join(tmp_dir, f"{video_id}.info.json")
    if os.path.exists(candidate):
        return candidate

    # Fallback: scan for any .info.json that contains the video_id
    try:
        for fname in os.listdir(tmp_dir):
            if fname.endswith(".info.json") and video_id in fname:
                return os.path.join(tmp_dir, fname)
    except OSError:
        pass

    return None


def _load_info_json(json_path: str) -> dict:
    with open(json_path, encoding="utf-8") as f:
        return json.load(f)


def _parse_upload_date(date_str: str | None):
    """Convert yt-dlp YYYYMMDD string to a Python date, or return None."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Database write helpers
# ---------------------------------------------------------------------------


def update_video_metadata(
    con: duckdb.DuckDBPyConnection,
    video_id: str,
    info: dict,
) -> None:
    """Update metadata columns on an existing videos row; fetch_status unchanged."""
    con.execute(
        """
        UPDATE videos SET
            title        = ?,
            channel_name = ?,
            channel_id   = ?,
            upload_date  = ?,
            duration     = ?,
            view_count   = ?
        WHERE video_id = ?
        """,
        [
            info.get("title"),
            info.get("channel") or info.get("uploader"),
            info.get("channel_id") or info.get("uploader_id"),
            _parse_upload_date(info.get("upload_date")),
            info.get("duration"),
            info.get("view_count"),
            video_id,
        ],
    )


def upsert_video(
    con: duckdb.DuckDBPyConnection,
    video_id: str,
    info: dict | None,
    fetch_status: str,
) -> None:
    """Insert or update a row in the videos table."""
    if info:
        con.execute(
            """
            INSERT INTO videos
                (video_id, title, channel_name, channel_id, upload_date,
                 duration, view_count, fetch_status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (video_id) DO UPDATE SET
                title        = excluded.title,
                channel_name = excluded.channel_name,
                channel_id   = excluded.channel_id,
                upload_date  = excluded.upload_date,
                duration     = excluded.duration,
                view_count   = excluded.view_count,
                fetch_status = excluded.fetch_status
            """,
            [
                video_id,
                info.get("title"),
                info.get("channel") or info.get("uploader"),
                info.get("channel_id") or info.get("uploader_id"),
                _parse_upload_date(info.get("upload_date")),
                info.get("duration"),
                info.get("view_count"),
                fetch_status,
            ],
        )
    else:
        con.execute(
            """
            INSERT INTO videos (video_id, fetch_status)
            VALUES (?, ?)
            ON CONFLICT (video_id) DO UPDATE SET
                fetch_status = excluded.fetch_status
            """,
            [video_id, fetch_status],
        )


def insert_bronze(
    con: duckdb.DuckDBPyConnection,
    video_id: str,
    raw_vtt: str,
    source_language: str,
) -> None:
    """Append a Bronze transcript row; never overwrites an existing row."""
    con.execute(
        """
        INSERT INTO transcripts_bronze (video_id, raw_vtt, source_language)
        VALUES (?, ?, ?)
        ON CONFLICT (video_id) DO NOTHING
        """,
        [video_id, raw_vtt, source_language],
    )


def upsert_membership(
    con: duckdb.DuckDBPyConnection,
    playlist_id: str,
    video_id: str,
) -> None:
    """Insert or refresh a playlist_video_membership row."""
    con.execute(
        """
        INSERT INTO playlist_video_membership
            (playlist_id, video_id, first_seen_at, last_seen_at)
        VALUES (?, ?, now(), now())
        ON CONFLICT (playlist_id, video_id) DO UPDATE SET
            last_seen_at = now(),
            removed_at   = NULL
        """,
        [playlist_id, video_id],
    )


def touch_membership(
    con: duckdb.DuckDBPyConnection,
    playlist_id: str,
    video_ids: list[str],
) -> None:
    """Upsert playlist_video_membership rows; updates last_seen_at and clears removed_at."""
    if not video_ids:
        return
    con.execute("BEGIN")
    try:
        for video_id in video_ids:
            upsert_membership(con, playlist_id, video_id)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------------------
# Chunk processing
# ---------------------------------------------------------------------------


def process_chunk(
    con: duckdb.DuckDBPyConnection,
    chunk: list[str],
    playlist_id: str,
    sleep_per_video: float,
    show_progress: bool = False,
    progress_offset: int = 0,
    progress_total: int | None = None,
) -> tuple[dict[str, int], list[str]]:
    """Download captions for a chunk of video IDs and write to DB in one transaction.

    Returns (summary, ok_ids) where summary is
    {'ok': N, 'no_subtitles': N, 'unavailable': N, 'rate_limited': N} and ok_ids
    lists video IDs that received a new Bronze transcript this run.
    """
    summary: dict[str, int] = {
        "ok": 0,
        "no_subtitles": 0,
        "unavailable": 0,
        "rate_limited": 0,
    }
    ok_ids: list[str] = []

    with tempfile.TemporaryDirectory() as tmp_dir:
        con.execute("BEGIN")
        try:
            for idx, video_id in enumerate(chunk):
                returncode, stderr = fetch_video_captions(video_id, tmp_dir)

                vtt_path, lang_code = find_vtt_file(tmp_dir, video_id)
                info_path = find_info_json(tmp_dir, video_id)
                info = _load_info_json(info_path) if info_path else None

                if vtt_path:
                    with open(vtt_path, encoding="utf-8") as fh:
                        raw_vtt = fh.read()
                    fetch_status = "ok"
                    insert_bronze(con, video_id, raw_vtt, lang_code or "en")
                    ok_ids.append(video_id)
                    os.remove(vtt_path)
                elif returncode != 0:
                    fetch_status = classify_failure(returncode, stderr)
                else:
                    fetch_status = "no_subtitles"

                if show_progress:
                    total = progress_total if progress_total is not None else progress_offset + len(chunk)
                    n = progress_offset + idx + 1
                    progress.line(f"[{n}/{total}] {video_id}  {fetch_status}")

                if info_path:
                    os.remove(info_path)

                summary[fetch_status] = summary.get(fetch_status, 0) + 1

                if fetch_status == "rate_limited":
                    log.warn({
                        "action": "process_video",
                        "video_id": video_id,
                        "result": "rate_limited",
                        "detail": stderr.strip()[:120],
                    })
                else:
                    upsert_video(con, video_id, info, fetch_status)
                    upsert_membership(con, playlist_id, video_id)
                    if returncode != 0:
                        log.warn({
                            "action": "process_video",
                            "video_id": video_id,
                            "result": fetch_status,
                            "detail": stderr.strip()[:120],
                        })
                    elif not show_progress:
                        log.info({"action": "process_video", "video_id": video_id, "result": fetch_status})

                if idx < len(chunk) - 1:
                    time.sleep(random.uniform(sleep_per_video - 1.0, sleep_per_video + 2.0))

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    return summary, ok_ids


def process_metadata_chunk(
    con: duckdb.DuckDBPyConnection,
    chunk: list[str],
    sleep_per_video: float,
) -> dict[str, int]:
    """Fetch metadata for a chunk of video IDs; update existing rows only.

    Returns a summary dict: {'updated': N, 'skipped': N, 'failed': N}.
    """
    summary: dict[str, int] = {"updated": 0, "skipped": 0, "failed": 0}

    with tempfile.TemporaryDirectory() as tmp_dir:
        con.execute("BEGIN")
        try:
            for idx, video_id in enumerate(chunk):
                exists = con.execute(
                    "SELECT 1 FROM videos WHERE video_id = ?",
                    [video_id],
                ).fetchone()
                if not exists:
                    summary["skipped"] += 1
                    continue

                returncode, stderr = fetch_video_metadata(video_id, tmp_dir)
                info_path = find_info_json(tmp_dir, video_id)
                info = _load_info_json(info_path) if info_path else None

                if info_path:
                    os.remove(info_path)

                if info:
                    update_video_metadata(con, video_id, info)
                    summary["updated"] += 1
                    log.info({"action": "refresh_metadata", "video_id": video_id, "result": "updated"})
                else:
                    summary["failed"] += 1
                    log.warn({
                        "action": "refresh_metadata",
                        "video_id": video_id,
                        "result": "failed",
                        "exit_code": returncode,
                        "detail": stderr.strip()[:120],
                    })

                if idx < len(chunk) - 1:
                    time.sleep(random.uniform(sleep_per_video - 1.0, sleep_per_video + 2.0))

            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    return summary


# ---------------------------------------------------------------------------
# Ingest orchestration
# ---------------------------------------------------------------------------


def get_existing_video_ids(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute("SELECT video_id FROM videos").fetchall()
    return {row[0] for row in rows}


def run_ingest(
    playlist_url: str,
    db_path: str,
    chunk_size: int,
    sleep_per_video: float,
    sleep_between_chunks: int,
) -> None:
    apply_schema(db_path)

    log.info({"action": "run_ingest", "input": {"playlist_url": playlist_url}})
    all_ids = fetch_playlist_ids(playlist_url)

    playlist_id = _extract_playlist_id(playlist_url)

    with duckdb.connect(db_path) as con:
        existing = get_existing_video_ids(con)

    new_ids = [vid for vid in all_ids if vid not in existing]
    overlap_ids = [vid for vid in all_ids if vid in existing]
    already = len(overlap_ids)
    log.info({
        "action": "fetch_playlist_ids",
        "result": {"total": len(all_ids), "new": len(new_ids), "existing": already},
    })

    with duckdb.connect(db_path) as con:
        if overlap_ids:
            touch_membership(con, playlist_id, overlap_ids)

    if not new_ids:
        result = {
            "action": "run_ingest",
            "result": {
                "status": "nothing_to_fetch",
                "linked": len(overlap_ids),
                "new": 0,
            },
        }
        print(json.dumps(result))
        log.info(result)
        return

    chunks = [
        new_ids[i : i + chunk_size] for i in range(0, len(new_ids), chunk_size)
    ]
    totals: dict[str, int] = {
        "ok": 0,
        "no_subtitles": 0,
        "unavailable": 0,
        "rate_limited": 0,
    }

    with duckdb.connect(db_path) as con:
        for chunk_num, chunk in enumerate(chunks, 1):
            log.info({"action": "process_chunk", "chunk": chunk_num, "total_chunks": len(chunks), "size": len(chunk)})
            summary, _ = process_chunk(con, chunk, playlist_id, sleep_per_video)
            for status, count in summary.items():
                totals[status] = totals.get(status, 0) + count

            if chunk_num < len(chunks):
                log.info({"action": "sleep_between_chunks", "seconds": sleep_between_chunks})
                time.sleep(sleep_between_chunks)

    result = {"action": "run_ingest", "result": {**totals, "linked": len(overlap_ids), "status": "complete"}}
    print(json.dumps(result))
    log.info(result)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest a YouTube playlist into Bronze tables."
    )
    parser.add_argument(
        "--playlist",
        help="YouTube playlist URL (overrides PLAYLIST_URL env var).",
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

    run_ingest(playlist_url, db_path, chunk_size, sleep_per_video, sleep_between_chunks)


if __name__ == "__main__":
    main()
