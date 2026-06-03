"""
Unit and integration tests for pipeline.refresh.

Coverage:
  - Diff helpers: get_active_membership_ids, get_no_subtitles_ids, tombstone_removed
  - touch_membership (last_seen_at + clears removed_at)
  - run_refresh orchestration (mocked fetch + process_chunk)
  - no_subtitles → ok transition flows through to Silver
  - CLI smoke
"""

import json
import os
import subprocess
from unittest.mock import patch

import duckdb
import pytest

from pipeline.ingest import (
    upsert_membership,
    upsert_video,
)
from pipeline.refresh import (
    get_active_membership_ids,
    get_no_subtitles_ids,
    run_refresh,
    tombstone_removed,
    touch_membership,
)
from pipeline.schema import apply_schema
from pipeline.transform import run_transform as real_run_transform

_INFO_JSON_MIN = {
    "title": "Test Video",
    "channel": "TestChannel",
    "channel_id": "UCtest",
    "upload_date": "20200406",
    "duration": 100,
    "view_count": 42,
}

_PATCH_SLEEP = patch("pipeline.refresh.time.sleep")
_PATCH_INGEST_SLEEP = patch("pipeline.ingest.time.sleep")
_PATCH_UNIFORM = patch("pipeline.ingest.random.uniform", return_value=0)


def _parse_refresh_stdout(captured_out: str) -> dict:
    """Return the run_refresh JSON line from stdout (transform may print first)."""
    for line in reversed(captured_out.strip().splitlines()):
        data = json.loads(line)
        if data.get("action") == "run_refresh":
            return data
    raise AssertionError("no run_refresh line in stdout")


@pytest.fixture
def db(tmp_path):
    path = str(tmp_path / "test.duckdb")
    apply_schema(path)
    con = duckdb.connect(path)
    yield con
    con.close()


@pytest.fixture
def db_path(tmp_path):
    path = str(tmp_path / "test.duckdb")
    apply_schema(path)
    return path


def _seed_membership(con, playlist_id: str, video_ids: list[str]) -> None:
    for vid in video_ids:
        upsert_video(con, vid, None, "ok")
        upsert_membership(con, playlist_id, vid)


# ---------------------------------------------------------------------------
# Diff helpers
# ---------------------------------------------------------------------------


class TestGetActiveMembershipIds:
    def test_returns_active_only(self, db):
        _seed_membership(db, "PLtest", ["vid1", "vid2"])
        db.execute(
            """
            UPDATE playlist_video_membership
            SET removed_at = now()
            WHERE video_id = 'vid2'
            """
        )
        assert get_active_membership_ids(db, "PLtest") == {"vid1"}


class TestGetNoSubtitlesIds:
    def test_filters_snapshot_to_no_subtitles(self, db):
        upsert_video(db, "vid1", None, "no_subtitles")
        upsert_video(db, "vid2", None, "ok")
        upsert_video(db, "vid3", None, "no_subtitles")
        result = get_no_subtitles_ids(db, ["vid1", "vid2", "vid3", "vid4"])
        assert set(result) == {"vid1", "vid3"}


class TestTombstoneRemoved:
    def test_sets_removed_at(self, db):
        _seed_membership(db, "PLtest", ["vid1", "vid2"])
        count = tombstone_removed(db, "PLtest", ["vid1"])
        assert count == 1
        assert db.execute(
            """
            SELECT removed_at IS NOT NULL
            FROM playlist_video_membership WHERE video_id = 'vid1'
            """
        ).fetchone()[0]
        assert db.execute(
            """
            SELECT removed_at IS NULL
            FROM playlist_video_membership WHERE video_id = 'vid2'
            """
        ).fetchone()[0]

    def test_empty_list_is_no_op(self, db):
        assert tombstone_removed(db, "PLtest", []) == 0


class TestTouchMembership:
    def test_updates_last_seen_at(self, db):
        _seed_membership(db, "PLtest", ["vid1"])
        first = db.execute(
            "SELECT epoch_ms(last_seen_at) FROM playlist_video_membership WHERE video_id = 'vid1'"
        ).fetchone()[0]
        touch_membership(db, "PLtest", ["vid1"])
        second = db.execute(
            "SELECT epoch_ms(last_seen_at) FROM playlist_video_membership WHERE video_id = 'vid1'"
        ).fetchone()[0]
        assert second >= first

    def test_clears_removed_at(self, db):
        _seed_membership(db, "PLtest", ["vid1"])
        db.execute(
            "UPDATE playlist_video_membership SET removed_at = now() WHERE video_id = 'vid1'"
        )
        touch_membership(db, "PLtest", ["vid1"])
        assert db.execute(
            """
            SELECT removed_at IS NULL
            FROM playlist_video_membership WHERE video_id = 'vid1'
            """
        ).fetchone()[0]


# ---------------------------------------------------------------------------
# run_refresh orchestration
# ---------------------------------------------------------------------------


def _mock_fetch_ok(video_id: str, tmp_dir: str) -> tuple[int, str]:
    vtt = os.path.join(tmp_dir, f"{video_id}.en.vtt")
    with open(vtt, "w") as f:
        f.write("WEBVTT\n\nHello world\n")
    info = os.path.join(tmp_dir, f"{video_id}.info.json")
    with open(info, "w") as f:
        json.dump(_INFO_JSON_MIN, f)
    return 0, ""


def _mock_fetch_no_subs(video_id: str, tmp_dir: str) -> tuple[int, str]:
    info = os.path.join(tmp_dir, f"{video_id}.info.json")
    with open(info, "w") as f:
        json.dump(_INFO_JSON_MIN, f)
    return 0, ""


class TestRunRefreshSynced:
    def test_already_synced_is_no_op(self, db_path, capsys):
        with duckdb.connect(db_path) as con:
            _seed_membership(con, "PLtest", ["abc123", "def456"])

        with patch("pipeline.refresh.fetch_playlist_ids", return_value=["abc123", "def456"]):
            with _PATCH_SLEEP:
                run_refresh(
                    "https://youtube.com/playlist?list=PLtest",
                    db_path,
                    chunk_size=50,
                    sleep_per_video=0,
                    sleep_between_chunks=0,
                )

        captured = capsys.readouterr()
        out = _parse_refresh_stdout(captured.out)
        assert out["result"]["added"] == 0
        assert out["result"]["tombstoned"] == 0
        assert out["result"]["newly_captioned"] == 0


class TestCircuitBreaker:
    def test_large_tombstone_aborts_without_flag(self, db_path):
        # Seed 25 active members; snapshot returns only 1 → 24 would be tombstoned
        members = [f"vid{i}" for i in range(25)]
        with duckdb.connect(db_path) as con:
            _seed_membership(con, "PLtest", members)

        with patch("pipeline.refresh.fetch_playlist_ids", return_value=["vid0"]):
            with _PATCH_SLEEP:
                with pytest.raises(SystemExit):
                    run_refresh(
                        "https://youtube.com/playlist?list=PLtest",
                        db_path,
                        chunk_size=50,
                        sleep_per_video=0,
                        sleep_between_chunks=0,
                    )

        # No tombstones written
        with duckdb.connect(db_path) as con:
            assert con.execute(
                """
                SELECT removed_at IS NULL
                FROM playlist_video_membership WHERE video_id = 'vid1'
                """
            ).fetchone()[0]

    def test_large_tombstone_allowed_with_flag(self, db_path):
        members = [f"vid{i}" for i in range(25)]
        with duckdb.connect(db_path) as con:
            _seed_membership(con, "PLtest", members)

        with patch("pipeline.refresh.fetch_playlist_ids", return_value=["vid0"]):
            with _PATCH_SLEEP:
                run_refresh(
                    "https://youtube.com/playlist?list=PLtest",
                    db_path,
                    chunk_size=50,
                    sleep_per_video=0,
                    sleep_between_chunks=0,
                    force_tombstone=True,
                )

        with duckdb.connect(db_path) as con:
            count = con.execute(
                """
                SELECT COUNT(*) FROM playlist_video_membership
                WHERE removed_at IS NOT NULL
                """
            ).fetchone()[0]
            assert count == 24

    def test_small_tombstone_proceeds_without_flag(self, db_path, capsys):
        members = [f"vid{i}" for i in range(5)]
        with duckdb.connect(db_path) as con:
            _seed_membership(con, "PLtest", members)

        with patch("pipeline.refresh.fetch_playlist_ids", return_value=["vid0"]):
            with _PATCH_SLEEP:
                run_refresh(
                    "https://youtube.com/playlist?list=PLtest",
                    db_path,
                    chunk_size=50,
                    sleep_per_video=0,
                    sleep_between_chunks=0,
                )

        captured = capsys.readouterr()
        out = _parse_refresh_stdout(captured.out)
        assert out["result"]["tombstoned"] == 4


class TestRunRefreshTombstone:
    def test_removed_video_tombstoned(self, db_path, capsys):
        with duckdb.connect(db_path) as con:
            _seed_membership(con, "PLtest", ["abc123", "def456", "removed1"])

        with patch("pipeline.refresh.fetch_playlist_ids", return_value=["abc123", "def456"]):
            with _PATCH_SLEEP:
                run_refresh(
                    "https://youtube.com/playlist?list=PLtest",
                    db_path,
                    chunk_size=50,
                    sleep_per_video=0,
                    sleep_between_chunks=0,
                )

        with duckdb.connect(db_path) as con:
            assert con.execute(
                """
                SELECT removed_at IS NOT NULL
                FROM playlist_video_membership WHERE video_id = 'removed1'
                """
            ).fetchone()[0]

        captured = capsys.readouterr()
        out = _parse_refresh_stdout(captured.out)
        assert out["result"]["tombstoned"] == 1


class TestRunRefreshNewVideo:
    def test_new_id_triggers_ingest(self, db_path, capsys):
        with duckdb.connect(db_path) as con:
            _seed_membership(con, "PLtest", ["abc123"])

        with patch("pipeline.refresh.fetch_playlist_ids", return_value=["abc123", "newvid1"]):
            with patch("pipeline.ingest.fetch_video_captions", side_effect=_mock_fetch_ok):
                with patch("pipeline.refresh.run_transform", wraps=real_run_transform) as mock_transform:
                    with _PATCH_SLEEP, _PATCH_INGEST_SLEEP, _PATCH_UNIFORM:
                        run_refresh(
                            "https://youtube.com/playlist?list=PLtest",
                            db_path,
                            chunk_size=50,
                            sleep_per_video=0,
                            sleep_between_chunks=0,
                        )

        mock_transform.assert_called_once_with(
            db_path, video_ids=["newvid1"], print_summary=False
        )

        with duckdb.connect(db_path) as con:
            assert con.execute(
                "SELECT COUNT(*) FROM videos WHERE video_id = 'newvid1'"
            ).fetchone()[0] == 1
            assert con.execute(
                "SELECT COUNT(*) FROM transcripts_bronze WHERE video_id = 'newvid1'"
            ).fetchone()[0] == 1
            assert con.execute(
                "SELECT COUNT(*) FROM transcripts_silver WHERE video_id = 'newvid1'"
            ).fetchone()[0] == 1

        captured = capsys.readouterr()
        out = _parse_refresh_stdout(captured.out)
        assert out["result"]["added"] == 1
        assert out["result"]["newly_captioned"] == 1


class TestRunRefreshNoSubtitlesRetry:
    def test_no_subtitles_to_ok_creates_silver(self, db_path, capsys):
        with duckdb.connect(db_path) as con:
            upsert_video(con, "retry1", None, "no_subtitles")
            upsert_membership(con, "PLtest", "retry1")

        call_count = 0

        def mock_retry_fetch(video_id: str, tmp_dir: str) -> tuple[int, str]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _mock_fetch_no_subs(video_id, tmp_dir)
            return _mock_fetch_ok(video_id, tmp_dir)

        with patch("pipeline.refresh.fetch_playlist_ids", return_value=["retry1"]):
            with patch("pipeline.ingest.fetch_video_captions", side_effect=_mock_fetch_ok):
                with patch("pipeline.refresh.run_transform", wraps=real_run_transform) as mock_transform:
                    with _PATCH_SLEEP, _PATCH_INGEST_SLEEP, _PATCH_UNIFORM:
                        run_refresh(
                            "https://youtube.com/playlist?list=PLtest",
                            db_path,
                            chunk_size=50,
                            sleep_per_video=0,
                            sleep_between_chunks=0,
                        )

        mock_transform.assert_called_once_with(
            db_path, video_ids=["retry1"], print_summary=False
        )

        with duckdb.connect(db_path) as con:
            assert con.execute(
                "SELECT fetch_status FROM videos WHERE video_id = 'retry1'"
            ).fetchone()[0] == "ok"
            assert con.execute(
                "SELECT COUNT(*) FROM transcripts_bronze WHERE video_id = 'retry1'"
            ).fetchone()[0] == 1
            silver = con.execute(
                "SELECT full_text FROM transcripts_silver WHERE video_id = 'retry1'"
            ).fetchone()
            assert silver is not None
            assert "Hello world" in silver[0]

        captured = capsys.readouterr()
        out = _parse_refresh_stdout(captured.out)
        assert out["result"]["newly_captioned"] == 1


class TestRunRefreshMetadata:
    def test_refresh_metadata_updates_title(self, db_path):
        with duckdb.connect(db_path) as con:
            upsert_video(con, "abc123", {"title": "Old Title"}, "ok")
            upsert_membership(con, "PLtest", "abc123")

        updated_info = {**_INFO_JSON_MIN, "title": "New Title"}

        def mock_metadata(video_id: str, tmp_dir: str) -> tuple[int, str]:
            info = os.path.join(tmp_dir, f"{video_id}.info.json")
            with open(info, "w") as f:
                json.dump(updated_info, f)
            return 0, ""

        with patch("pipeline.refresh.fetch_playlist_ids", return_value=["abc123"]):
            with patch("pipeline.ingest.fetch_video_metadata", side_effect=mock_metadata):
                with _PATCH_SLEEP, _PATCH_INGEST_SLEEP, _PATCH_UNIFORM:
                    run_refresh(
                        "https://youtube.com/playlist?list=PLtest",
                        db_path,
                        chunk_size=50,
                        sleep_per_video=0,
                        sleep_between_chunks=0,
                        refresh_metadata=True,
                    )

        with duckdb.connect(db_path) as con:
            title = con.execute(
                "SELECT title FROM videos WHERE video_id = 'abc123'"
            ).fetchone()[0]
            assert title == "New Title"
            status = con.execute(
                "SELECT fetch_status FROM videos WHERE video_id = 'abc123'"
            ).fetchone()[0]
            assert status == "ok"

    def test_refresh_metadata_excludes_fetched_ids(self, db_path):
        with duckdb.connect(db_path) as con:
            _seed_membership(con, "PLtest", ["abc123"])
            upsert_video(con, "existing1", {"title": "Existing"}, "ok")
            upsert_membership(con, "PLtest", "existing1")

        metadata_chunks: list[list[str]] = []

        def capture_metadata_chunk(con, chunk, sleep_per_video):
            metadata_chunks.append(list(chunk))
            return {"updated": len(chunk), "skipped": 0, "failed": 0}

        with patch(
            "pipeline.refresh.fetch_playlist_ids",
            return_value=["abc123", "existing1", "newvid1"],
        ):
            with patch("pipeline.ingest.fetch_video_captions", side_effect=_mock_fetch_ok):
                with patch(
                    "pipeline.refresh.process_metadata_chunk",
                    side_effect=capture_metadata_chunk,
                ):
                    with patch("pipeline.refresh.run_transform"):
                        with _PATCH_SLEEP, _PATCH_INGEST_SLEEP, _PATCH_UNIFORM:
                            run_refresh(
                                "https://youtube.com/playlist?list=PLtest",
                                db_path,
                                chunk_size=50,
                                sleep_per_video=0,
                                sleep_between_chunks=0,
                                refresh_metadata=True,
                            )

        all_metadata_ids = {vid for chunk in metadata_chunks for vid in chunk}
        assert "newvid1" not in all_metadata_ids
        assert all_metadata_ids == {"abc123", "existing1"}


# ---------------------------------------------------------------------------
# CLI smoke
# ---------------------------------------------------------------------------


class TestCLISmoke:
    def test_refresh_help_exits_zero(self):
        result = subprocess.run(
            ["uv", "run", "python", "pipeline/refresh.py", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--refresh-metadata" in result.stdout

    def test_refresh_no_playlist_url_exits_nonzero(self):
        env = {**os.environ, "PLAYLIST_URL": ""}
        result = subprocess.run(
            ["uv", "run", "python", "pipeline/refresh.py"],
            capture_output=True,
            text=True,
            env=env,
        )
        assert result.returncode != 0
        assert result.stderr.strip() != ""
