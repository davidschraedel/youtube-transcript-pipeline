"""
Unit tests for pipeline.utils: parse_vtt, dedupe_repeated_phrases,
and classify_failure.

Fixture VTT files live in tests/fixtures/.
"""

import os

import duckdb
import pytest

from pipeline.utils import classify_failure, dedupe_repeated_phrases, parse_vtt

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def load_fixture(name: str) -> str:
    with open(os.path.join(FIXTURES, name), encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# parse_vtt
# ---------------------------------------------------------------------------


class TestParseVtt:
    def test_strips_webvtt_header(self):
        result = parse_vtt("WEBVTT\nKind: captions\nLanguage: en\n\nHello world")
        assert "WEBVTT" not in result
        assert "Kind:" not in result
        assert "Language:" not in result
        assert "Hello world" in result

    def test_strips_timestamp_range_lines(self):
        vtt = "WEBVTT\n\n00:00:01.000 --> 00:00:04.000\nsome text"
        result = parse_vtt(vtt)
        assert "-->" not in result
        assert "some text" in result

    def test_strips_inline_timestamp_tags(self):
        result = parse_vtt("<00:00:06.500>hello <00:00:07.200>world")
        assert "<" not in result
        assert "hello world" in result

    def test_strips_caption_tags(self):
        result = parse_vtt("<c>Hello</c> <c>world</c>")
        assert "<c>" not in result
        assert "</c>" not in result
        assert "Hello" in result
        assert "world" in result

    def test_html_entity_unescaping(self):
        result = parse_vtt("AT&amp;T &lt;here&gt;")
        assert "AT&T" in result
        assert "&amp;" not in result

    def test_speaker_change_normalization(self):
        result = parse_vtt("First speaker >> second speaker")
        assert "\n>> " in result

    def test_normal_overlapping_fixture(self):
        raw = load_fixture("normal_overlapping.vtt")
        result = parse_vtt(raw)
        assert "WEBVTT" not in result
        assert "-->" not in result
        assert "<c>" not in result
        assert "<00:" not in result
        assert len(result.strip()) > 0

    def test_empty_vtt_returns_empty_string(self):
        raw = load_fixture("empty.vtt")
        result = parse_vtt(raw)
        assert result == ""

    def test_no_speech_fixture(self):
        raw = load_fixture("no_speech.vtt")
        result = parse_vtt(raw)
        # Should contain bracket labels but no VTT artifacts
        assert "-->" not in result
        assert "[Music]" in result

    def test_non_ascii_fixture(self):
        raw = load_fixture("non_ascii.vtt")
        result = parse_vtt(raw)
        assert "Héllo" in result
        assert "café" in result
        assert "&amp;" not in result
        # HTML entities should be unescaped
        assert "&" in result
        assert "<here>" in result


# ---------------------------------------------------------------------------
# dedupe_repeated_phrases
# ---------------------------------------------------------------------------


class TestDedupeRepeatedPhrases:
    def test_no_repeats_unchanged(self):
        text = "the quick brown fox"
        assert dedupe_repeated_phrases(text) == text

    def test_single_word_repeat_collapsed(self):
        result = dedupe_repeated_phrases("hello hello hello world")
        assert result == "hello world"

    def test_multi_word_phrase_collapsed(self):
        result = dedupe_repeated_phrases("the quick brown fox the quick brown fox")
        assert result == "the quick brown fox"

    def test_three_consecutive_repeats(self):
        result = dedupe_repeated_phrases("go go go now")
        assert result == "go now"

    def test_longest_ngram_matched_first(self):
        # "a b a b" — two-word phrase repeats; should collapse to "a b"
        result = dedupe_repeated_phrases("a b a b")
        assert result == "a b"

    def test_empty_string(self):
        assert dedupe_repeated_phrases("") == ""

    def test_single_word(self):
        assert dedupe_repeated_phrases("hello") == "hello"

    def test_repeated_phrases_fixture(self):
        raw = load_fixture("repeated_phrases.vtt")
        cleaned = parse_vtt(raw)
        result = dedupe_repeated_phrases(cleaned)
        # After dedup the phrase should appear only once
        words = result.split()
        fox_count = sum(1 for i in range(len(words)) if words[i] == "fox")
        assert fox_count == 1

    def test_max_ngram_parameter(self):
        # With max_ngram=2, only phrases up to 2 words are checked
        text = "a b c a b c"
        # 3-word phrase won't be caught by max_ngram=2
        result = dedupe_repeated_phrases(text, max_ngram=2)
        # With max_ngram=12 it should collapse
        result_full = dedupe_repeated_phrases(text, max_ngram=12)
        assert result_full == "a b c"

    def test_non_repeating_adjacent_phrases(self):
        text = "the cat sat on the mat"
        result = dedupe_repeated_phrases(text)
        assert result == "the cat sat on the mat"


# ---------------------------------------------------------------------------
# classify_failure
# ---------------------------------------------------------------------------


class TestClassifyFailure:
    def test_zero_returncode_is_ok(self):
        assert classify_failure(0, "") == "ok"

    def test_rate_limited_detection(self):
        assert classify_failure(1, "HTTP Error 429: Too Many Requests") == "rate_limited"

    def test_rate_limited_lowercase(self):
        assert classify_failure(1, "rate limit exceeded") == "rate_limited"

    def test_no_subtitles_detection(self):
        assert classify_failure(1, "There are no subtitles for this video") == "no_subtitles"

    def test_unavailable_detection(self):
        assert classify_failure(1, "Video unavailable") == "unavailable"

    def test_private_video(self):
        assert classify_failure(1, "This is a private video") == "unavailable"

    def test_unknown_error_defaults_to_unavailable(self):
        assert classify_failure(1, "some completely unknown error") == "unavailable"

    def test_case_insensitive_matching(self):
        assert classify_failure(1, "NO SUBTITLES FOUND") == "no_subtitles"


# ---------------------------------------------------------------------------
# transform integration: run_transform on in-memory DuckDB
# ---------------------------------------------------------------------------


def _make_db() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with the full schema applied."""
    con = duckdb.connect(":memory:")
    con.execute(
        """
        CREATE TABLE videos (
            video_id VARCHAR PRIMARY KEY,
            title VARCHAR,
            channel_name VARCHAR,
            channel_id VARCHAR,
            upload_date DATE,
            duration INTEGER,
            view_count BIGINT,
            fetch_status VARCHAR NOT NULL CHECK (
                fetch_status IN ('ok', 'no_subtitles', 'unavailable', 'rate_limited')
            ),
            ingested_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
        """
    )
    con.execute(
        """
        CREATE TABLE transcripts_bronze (
            video_id VARCHAR PRIMARY KEY,
            raw_vtt VARCHAR NOT NULL,
            source_language VARCHAR NOT NULL,
            fetched_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
        """
    )
    con.execute(
        """
        CREATE TABLE transcripts_silver (
            video_id VARCHAR PRIMARY KEY,
            full_text VARCHAR NOT NULL,
            source_language VARCHAR NOT NULL,
            transformed_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
        """
    )
    return con


class TestRunTransform:
    def test_empty_bronze_is_noop(self, capsys):
        from pipeline.transform import _get_rows

        con = _make_db()
        rows = _get_rows(con, force=False)
        assert rows == []

    def test_transform_writes_silver_row(self):
        from pipeline.transform import _get_rows

        raw = load_fixture("repeated_phrases.vtt")
        con = _make_db()
        con.execute(
            "INSERT INTO transcripts_bronze (video_id, raw_vtt, source_language) VALUES (?, ?, ?)",
            ["vid1", raw, "en"],
        )

        rows = _get_rows(con, force=False)
        assert len(rows) == 1

        video_id, raw_vtt, source_language = rows[0]
        cleaned = parse_vtt(raw_vtt)
        deduped = dedupe_repeated_phrases(cleaned)

        con.execute("DELETE FROM transcripts_silver WHERE video_id = ?", [video_id])
        con.execute(
            "INSERT INTO transcripts_silver (video_id, full_text, source_language) VALUES (?, ?, ?)",
            [video_id, deduped, source_language],
        )

        silver_rows = con.execute("SELECT * FROM transcripts_silver").fetchall()
        assert len(silver_rows) == 1
        assert silver_rows[0][0] == "vid1"
        assert "fox" in silver_rows[0][1]

    def test_already_processed_rows_skipped_without_force(self):
        from pipeline.transform import _get_rows

        raw = load_fixture("normal_overlapping.vtt")
        con = _make_db()
        con.execute(
            "INSERT INTO transcripts_bronze (video_id, raw_vtt, source_language) VALUES (?, ?, ?)",
            ["vid1", raw, "en"],
        )
        con.execute(
            "INSERT INTO transcripts_silver (video_id, full_text, source_language) VALUES (?, ?, ?)",
            ["vid1", "already processed", "en"],
        )

        rows = _get_rows(con, force=False)
        assert rows == []

    def test_force_flag_reprocesses_all(self):
        from pipeline.transform import _get_rows

        raw = load_fixture("normal_overlapping.vtt")
        con = _make_db()
        con.execute(
            "INSERT INTO transcripts_bronze (video_id, raw_vtt, source_language) VALUES (?, ?, ?)",
            ["vid1", raw, "en"],
        )
        con.execute(
            "INSERT INTO transcripts_silver (video_id, full_text, source_language) VALUES (?, ?, ?)",
            ["vid1", "already processed", "en"],
        )

        rows = _get_rows(con, force=True)
        assert len(rows) == 1
