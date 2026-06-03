"""
Unit and integration tests for pipeline.ingest.

Coverage:
  Step 1 — Pure helpers: _extract_playlist_id, _parse_upload_date,
           find_vtt_file, find_info_json
  Step 2 — fetch_playlist_ids (mock subprocess.run)
  Step 3 — DB helpers: insert_bronze, upsert_video, upsert_membership
  Step 4 — process_chunk (mock fetch_video_captions, patch sleep)
  Step 5 — Diff / re-run logic: get_existing_video_ids, run_ingest
  P1     — CLI smoke (subprocess, no live network)
  P2     — Schema: apply_schema idempotency, smoke_test, CHECK constraint
"""

import json
import os
import subprocess
from datetime import date
from subprocess import CompletedProcess
from unittest.mock import patch

import duckdb
import pytest

from pipeline.ingest import (
    _extract_playlist_id,
    _parse_upload_date,
    fetch_playlist_ids,
    find_info_json,
    find_vtt_file,
    get_existing_video_ids,
    insert_bronze,
    process_chunk,
    run_ingest,
    upsert_membership,
    upsert_video,
)
from pipeline.schema import apply_schema, smoke_test

FIXTURES_INGEST = os.path.join(os.path.dirname(__file__), "fixtures", "ingest")

_FLAT_PLAYLIST_2 = CompletedProcess(
    args=[],
    returncode=0,
    stdout='{"id": "abc123", "title": "Test"}\n{"id": "def456", "title": "Test 2"}\n',
    stderr="",
)

_INFO_JSON_MIN = {
    "title": "Rick Astley - Never Gonna Give You Up",
    "channel": "RickAstleyVEVO",
    "channel_id": "UCuAXFkgsw1L7xaCfnd5JJOw",
    "upload_date": "20091025",
    "duration": 213,
    "view_count": 1_234_567_890,
}


@pytest.fixture
def db(tmp_path):
    """Open DuckDB connection with full schema applied; one file per test."""
    path = str(tmp_path / "test.duckdb")
    apply_schema(path)
    con = duckdb.connect(path)
    yield con
    con.close()


@pytest.fixture
def db_path(tmp_path):
    """Return a temp DB path with schema applied; no open connection held."""
    path = str(tmp_path / "test.duckdb")
    apply_schema(path)
    return path


# ---------------------------------------------------------------------------
# Step 1 — Pure helpers (no DB, no subprocess)
# ---------------------------------------------------------------------------


class TestExtractPlaylistId:
    def test_url_with_list_param(self):
        url = "https://www.youtube.com/playlist?list=PLxxx&si=abc123"
        assert _extract_playlist_id(url) == "PLxxx"

    def test_bare_playlist_id_returned_as_is(self):
        assert _extract_playlist_id("PLxxx123") == "PLxxx123"


class TestParseUploadDate:
    def test_valid_yyyymmdd(self):
        assert _parse_upload_date("20200406") == date(2020, 4, 6)

    def test_none_returns_none(self):
        assert _parse_upload_date(None) is None

    def test_bad_string_returns_none(self):
        assert _parse_upload_date("bad") is None


class TestFindVttFile:
    def test_finds_en_vtt(self, tmp_path):
        vtt = tmp_path / "dQw4w9WgXcQ.en.vtt"
        vtt.write_text("WEBVTT")
        path, lang = find_vtt_file(str(tmp_path), "dQw4w9WgXcQ")
        assert path == str(vtt)
        assert lang == "en"

    def test_finds_en_us_vtt(self, tmp_path):
        vtt = tmp_path / "dQw4w9WgXcQ.en-US.vtt"
        vtt.write_text("WEBVTT")
        _, lang = find_vtt_file(str(tmp_path), "dQw4w9WgXcQ")
        assert lang == "en-US"

    def test_empty_dir_returns_none_pair(self, tmp_path):
        path, lang = find_vtt_file(str(tmp_path), "dQw4w9WgXcQ")
        assert path is None
        assert lang is None


class TestFindInfoJson:
    def test_finds_info_json(self, tmp_path):
        info = tmp_path / "dQw4w9WgXcQ.info.json"
        info.write_text("{}")
        assert find_info_json(str(tmp_path), "dQw4w9WgXcQ") == str(info)

    def test_missing_returns_none(self, tmp_path):
        assert find_info_json(str(tmp_path), "dQw4w9WgXcQ") is None


# ---------------------------------------------------------------------------
# Step 2 — fetch_playlist_ids (mock subprocess.run)
# ---------------------------------------------------------------------------


class TestFetchPlaylistIds:
    def test_happy_path_returns_ids(self):
        with patch("pipeline.ingest.subprocess.run", return_value=_FLAT_PLAYLIST_2):
            ids = fetch_playlist_ids("https://youtube.com/playlist?list=PLtest")
        assert ids == ["abc123", "def456"]

    def test_malformed_lines_skipped(self):
        mock = CompletedProcess(
            args=[],
            returncode=0,
            stdout='{"id": "abc123"}\nnot json\n{"id": "def456"}\n',
            stderr="",
        )
        with patch("pipeline.ingest.subprocess.run", return_value=mock):
            ids = fetch_playlist_ids("https://youtube.com/playlist?list=PLtest")
        assert ids == ["abc123", "def456"]

    def test_total_failure_exits(self):
        mock = CompletedProcess(args=[], returncode=1, stdout="", stderr="yt-dlp error")
        with patch("pipeline.ingest.subprocess.run", return_value=mock):
            with pytest.raises(SystemExit):
                fetch_playlist_ids("https://youtube.com/playlist?list=PLtest")

    def test_nonzero_with_partial_stdout_aborts(self):
        mock = CompletedProcess(
            args=[],
            returncode=1,
            stdout='{"id": "abc123"}\n',
            stderr="Partial failure",
        )
        with patch("pipeline.ingest.subprocess.run", return_value=mock):
            with pytest.raises(SystemExit):
                fetch_playlist_ids("https://youtube.com/playlist?list=PLtest")


# ---------------------------------------------------------------------------
# Step 3 — DB helpers (temp-file DuckDB + apply_schema)
# ---------------------------------------------------------------------------


class TestInsertBronze:
    def test_inserts_row(self, db):
        insert_bronze(db, "vid1", "WEBVTT\nHello", "en")
        rows = db.execute("SELECT video_id FROM transcripts_bronze").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "vid1"

    def test_no_op_on_duplicate(self, db):
        insert_bronze(db, "vid1", "WEBVTT\nFirst", "en")
        insert_bronze(db, "vid1", "WEBVTT\nSecond", "en")
        rows = db.execute(
            "SELECT raw_vtt FROM transcripts_bronze WHERE video_id = 'vid1'"
        ).fetchall()
        assert len(rows) == 1
        assert "First" in rows[0][0]


class TestUpsertVideo:
    def test_with_info_json(self, db):
        upsert_video(db, "vid1", _INFO_JSON_MIN, "ok")
        row = db.execute(
            "SELECT title, channel_name, fetch_status FROM videos WHERE video_id = 'vid1'"
        ).fetchone()
        assert row[0] == _INFO_JSON_MIN["title"]
        assert row[1] == _INFO_JSON_MIN["channel"]
        assert row[2] == "ok"

    def test_without_info(self, db):
        upsert_video(db, "vid1", None, "no_subtitles")
        row = db.execute(
            "SELECT video_id, fetch_status FROM videos WHERE video_id = 'vid1'"
        ).fetchone()
        assert row == ("vid1", "no_subtitles")

    def test_updates_fetch_status_on_conflict(self, db):
        upsert_video(db, "vid1", None, "no_subtitles")
        upsert_video(db, "vid1", _INFO_JSON_MIN, "ok")
        row = db.execute(
            "SELECT fetch_status FROM videos WHERE video_id = 'vid1'"
        ).fetchone()
        assert row[0] == "ok"


class TestUpsertMembership:
    def test_inserts_row(self, db):
        upsert_video(db, "vid1", None, "ok")
        upsert_membership(db, "PLtest", "vid1")
        rows = db.execute(
            "SELECT playlist_id, video_id FROM playlist_video_membership"
        ).fetchall()
        assert rows == [("PLtest", "vid1")]

    def test_updates_last_seen_at(self, db):
        upsert_video(db, "vid1", None, "ok")
        upsert_membership(db, "PLtest", "vid1")
        # epoch_ms() returns a BIGINT, avoiding pytz dependency for TIMESTAMPTZ reads
        first = db.execute(
            "SELECT epoch_ms(last_seen_at) FROM playlist_video_membership WHERE video_id = 'vid1'"
        ).fetchone()[0]
        upsert_membership(db, "PLtest", "vid1")
        second = db.execute(
            "SELECT epoch_ms(last_seen_at) FROM playlist_video_membership WHERE video_id = 'vid1'"
        ).fetchone()[0]
        assert second >= first


# ---------------------------------------------------------------------------
# Step 4 — process_chunk (mock fetch_video_captions, patch sleep)
# ---------------------------------------------------------------------------


def _mock_fetch_ok(video_id: str, tmp_dir: str) -> tuple[int, str]:
    vtt = os.path.join(tmp_dir, f"{video_id}.en.vtt")
    with open(vtt, "w") as f:
        f.write("WEBVTT\n\nNever gonna give you up\n")
    info = os.path.join(tmp_dir, f"{video_id}.info.json")
    with open(info, "w") as f:
        json.dump(_INFO_JSON_MIN, f)
    return 0, ""


def _mock_fetch_no_subs(video_id: str, tmp_dir: str) -> tuple[int, str]:
    info = os.path.join(tmp_dir, f"{video_id}.info.json")
    with open(info, "w") as f:
        json.dump({"title": "No Sub Video"}, f)
    return 0, ""


def _mock_fetch_unavailable(_video_id: str, _tmp_dir: str) -> tuple[int, str]:
    return 1, "Video unavailable"


def _mock_fetch_rate_limited(_video_id: str, _tmp_dir: str) -> tuple[int, str]:
    return 1, "HTTP Error 429: Too Many Requests"


_PATCH_SLEEP = patch("pipeline.ingest.time.sleep")
_PATCH_UNIFORM = patch("pipeline.ingest.random.uniform", return_value=0)


class TestProcessChunk:
    def test_ok_writes_all_tables(self, db):
        with patch("pipeline.ingest.fetch_video_captions", side_effect=_mock_fetch_ok):
            with _PATCH_SLEEP, _PATCH_UNIFORM:
                summary, _ = process_chunk(db, ["dQw4w9WgXcQ"], "PLtest", 0)

        assert summary["ok"] == 1
        assert db.execute("SELECT COUNT(*) FROM videos").fetchone()[0] == 1
        assert db.execute("SELECT COUNT(*) FROM transcripts_bronze").fetchone()[0] == 1
        assert db.execute(
            "SELECT COUNT(*) FROM playlist_video_membership"
        ).fetchone()[0] == 1

    def test_no_subtitles_writes_video_only(self, db):
        with patch("pipeline.ingest.fetch_video_captions", side_effect=_mock_fetch_no_subs):
            with _PATCH_SLEEP, _PATCH_UNIFORM:
                summary, _ = process_chunk(db, ["vid1"], "PLtest", 0)

        assert summary["no_subtitles"] == 1
        assert db.execute(
            "SELECT fetch_status FROM videos WHERE video_id = 'vid1'"
        ).fetchone()[0] == "no_subtitles"
        assert db.execute("SELECT COUNT(*) FROM transcripts_bronze").fetchone()[0] == 0

    def test_unavailable_writes_video_only(self, db):
        with patch(
            "pipeline.ingest.fetch_video_captions", side_effect=_mock_fetch_unavailable
        ):
            with _PATCH_SLEEP, _PATCH_UNIFORM:
                summary, _ = process_chunk(db, ["vid1"], "PLtest", 0)

        assert summary["unavailable"] == 1
        assert db.execute(
            "SELECT fetch_status FROM videos WHERE video_id = 'vid1'"
        ).fetchone()[0] == "unavailable"
        assert db.execute("SELECT COUNT(*) FROM transcripts_bronze").fetchone()[0] == 0

    def test_rate_limited_writes_no_rows(self, db):
        with patch(
            "pipeline.ingest.fetch_video_captions", side_effect=_mock_fetch_rate_limited
        ):
            with _PATCH_SLEEP, _PATCH_UNIFORM:
                summary, _ = process_chunk(db, ["vid1"], "PLtest", 0)

        assert summary["rate_limited"] == 1
        assert db.execute("SELECT COUNT(*) FROM videos").fetchone()[0] == 0
        assert db.execute(
            "SELECT COUNT(*) FROM playlist_video_membership"
        ).fetchone()[0] == 0

    def test_chunk_commit_both_videos_visible(self, db):
        with patch("pipeline.ingest.fetch_video_captions", side_effect=_mock_fetch_ok):
            with _PATCH_SLEEP, _PATCH_UNIFORM:
                summary, _ = process_chunk(db, ["vid1", "vid2"], "PLtest", 0)

        assert summary["ok"] == 2
        assert db.execute("SELECT COUNT(*) FROM videos").fetchone()[0] == 2
        assert db.execute("SELECT COUNT(*) FROM transcripts_bronze").fetchone()[0] == 2

    def test_chunk_rollback_on_exception(self, db):
        call_count = 0

        def mock_raise_second(video_id: str, tmp_dir: str) -> tuple[int, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_fetch_ok(video_id, tmp_dir)
            raise RuntimeError("Simulated mid-chunk failure")

        with patch(
            "pipeline.ingest.fetch_video_captions", side_effect=mock_raise_second
        ):
            with _PATCH_SLEEP, _PATCH_UNIFORM:
                with pytest.raises(RuntimeError):
                    process_chunk(db, ["vid1", "vid2"], "PLtest", 0)

        assert db.execute("SELECT COUNT(*) FROM videos").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM transcripts_bronze").fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Step 5 — Diff / re-run logic
# ---------------------------------------------------------------------------


class TestGetExistingVideoIds:
    def test_returns_all_video_ids(self, db):
        for vid in ("vid1", "vid2", "vid3"):
            upsert_video(db, vid, None, "ok")
        assert get_existing_video_ids(db) == {"vid1", "vid2", "vid3"}

    def test_empty_db_returns_empty_set(self, db):
        assert get_existing_video_ids(db) == set()


class TestDiffAndRerun:
    def test_new_only_diff(self, db):
        for vid in ("vid1", "vid2", "vid3"):
            upsert_video(db, vid, None, "ok")
        all_ids = ["vid1", "vid2", "vid3", "vid4", "vid5"]
        existing = get_existing_video_ids(db)
        new_ids = [v for v in all_ids if v not in existing]
        assert set(new_ids) == {"vid4", "vid5"}

    def test_nothing_to_ingest_when_all_present(self, db_path, capsys):
        with duckdb.connect(db_path) as con:
            upsert_video(con, "abc123", None, "ok")
            upsert_video(con, "def456", None, "ok")

        with patch("pipeline.ingest.subprocess.run", return_value=_FLAT_PLAYLIST_2):
            with patch("pipeline.ingest.time.sleep"):
                run_ingest(
                    "https://youtube.com/playlist?list=PLtest",
                    db_path,
                    chunk_size=50,
                    sleep_per_video=0,
                    sleep_between_chunks=0,
                )

        captured = capsys.readouterr()
        out = json.loads(captured.out.strip())
        assert out["result"]["status"] == "nothing_to_ingest"

    def test_rate_limited_id_appears_in_next_diff(self, db):
        upsert_video(db, "vid1", None, "ok")
        # vid2 was rate_limited: no row written, so it stays in diff on re-run
        existing = get_existing_video_ids(db)
        new_ids = [v for v in ["vid1", "vid2"] if v not in existing]
        assert new_ids == ["vid2"]


# ---------------------------------------------------------------------------
# P1 — CLI smoke (subprocess, no live network)
# ---------------------------------------------------------------------------


class TestCLISmoke:
    def test_ingest_help_exits_zero(self):
        result = subprocess.run(
            ["uv", "run", "python", "pipeline/ingest.py", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--playlist" in result.stdout

    def test_ingest_no_playlist_url_exits_nonzero(self):
        env = {**os.environ, "PLAYLIST_URL": ""}
        result = subprocess.run(
            ["uv", "run", "python", "pipeline/ingest.py"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode != 0
        assert result.stderr.strip() != ""
        assert result.stdout.strip() == ""

    def test_transform_help_exits_zero(self):
        result = subprocess.run(
            ["uv", "run", "python", "pipeline/transform.py", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--force" in result.stdout

    def test_transform_missing_db_exits_nonzero(self):
        env = {**os.environ, "DB_PATH": "/tmp/nonexistent_db_xyz_99999.duckdb"}
        result = subprocess.run(
            ["uv", "run", "python", "pipeline/transform.py"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode != 0
        assert result.stderr.strip() != ""
        assert result.stdout.strip() == ""


# ---------------------------------------------------------------------------
# P2 — Schema integration
# ---------------------------------------------------------------------------


class TestApplySchema:
    def test_idempotent(self, tmp_path):
        path = str(tmp_path / "test.duckdb")
        apply_schema(path)
        apply_schema(path)
        with duckdb.connect(path) as con:
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        assert tables == {
            "videos",
            "playlist_video_membership",
            "transcripts_bronze",
            "transcripts_silver",
        }

    def test_smoke_test_passes_on_fresh_db(self, tmp_path):
        path = str(tmp_path / "test.duckdb")
        apply_schema(path)
        smoke_test(path)

    def test_invalid_fetch_status_rejected(self, tmp_path):
        path = str(tmp_path / "test.duckdb")
        apply_schema(path)
        with duckdb.connect(path) as con:
            with pytest.raises(Exception):
                con.execute(
                    "INSERT INTO videos (video_id, fetch_status) "
                    "VALUES ('vid1', 'invalid_status')"
                )
