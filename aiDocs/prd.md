# Product Requirements Document — YoutubeTranscriptPipeline

**Version:** 1.0
**Status:** Draft
**Last updated:** 2026-05-12

---

## Problem Statement

YouTube playlists accumulate hours of spoken content that is effectively unsearchable. Transcripts exist as auto-generated captions, but they are locked inside YouTube's interface — not queryable, not exportable in clean form, and not usable for analysis or LLM input. Tools like NotebookLM are limited in the number of sources they can ingest for basic LLM interaction. NotebookLM also has an undesirable GUI. There is no lightweight tool that lets a user pull an entire playlist's worth of transcripts into a local database, clean them, and query them with SQL or browse in a UI. Tools like Firecrawl would incur higher costs than necessary for a personal project, and would need to be weighed against alternatives for a project at scale. This project fills these gaps for personal use and as a portfolio demonstration of a working data engineering pipeline (ingest → store → transform → serve).

---

## Target Users

**Primary:** The developer/owner — a data engineering student building and using this tool personally to explore YouTube content programmatically and demonstrate pipeline skills.

**Secondary:** Technical interviewers or portfolio viewers who evaluate the repository to assess data engineering proficiency (pipeline design, data modeling, SQL, Python).

**Not for:**

- Non-technical end users expecting a polished consumer product
- Teams needing multi-user access controls or concurrent writes
- Use cases involving real-time or streaming ingestion

---

## Goals and Success Metrics

| Goal                                     | Metric                                                                                             |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Ingest a full playlist into local DuckDB | All videos in playlist have a row in `videos`; captioned videos have a row in `transcripts_bronze` |
| Produce clean, queryable transcripts     | `transcripts_silver` rows pass dedupe tests; full text is readable without VTT artifacts           |
| Surface data through a usable UI         | Browse, filter, detail view, and guarded SQL all function without errors in local Streamlit        |
| Handle failures gracefully               | Every video has a populated `fetch_status`; no unhandled exceptions crash a full sync              |
| Support incremental updates              | Re-running `ingest.py` fetches only new videos; `refresh.py` detects newly captioned videos        |
| Repo is portfolio-ready                  | README explains the system end-to-end; no secrets committed; `.env.example` present                |

---

## Key Features

### P0 — Must Have (v1 core, blocking everything else)

- **Playlist ingestion (`ingest.py`):** Fetch all video IDs from a playlist via `yt-dlp --flat-playlist`; diff against DB; fetch VTT subtitles + metadata for new IDs only; land raw data in Bronze; set failure status (`ok`, `no_subtitles`, `unavailable`, `rate_limited`) per video; chunked runs with sleeps to avoid rate limiting
- **Bronze schema:** `videos`, `playlist_video_membership`, `transcripts_bronze` tables in local DuckDB; writes are append/upsert only; `transcripts_bronze` is never overwritten
- **VTT cleaning + deduplication (`transform.py`):** Parse raw VTT text; strip timestamps and formatting tags; deduplicate overlapping caption segments; produce clean prose text; write to `transcripts_silver`; idempotent and re-runnable
- **Silver schema:** `transcripts_silver` with `video_id`, `full_text`, `source_language`; re-derivable from Bronze at any time

### P1 — Should Have (v1 polish, high value)

- **Freshness refresh (`refresh.py`):** Re-run flat-playlist diff; upsert video metadata; detect videos that previously had `no_subtitles` status and now have captions; tombstone removed videos (`removed_at`)
- **Local Streamlit UI (`gui/app.py`):**
  - Browse view: metadata list with filters (e.g. channel, date range), paginated
  - Detail view: full transcript for one selected video, loaded on demand
  - SQL tab: read-only DuckDB connection; query parsing enforces `SELECT`-only and `LIMIT`; statement timeout as second safety net
- **Shared utilities (`utils.py`):** Failure classifier, VTT parser, manifest/logging helpers used by all pipeline scripts

### P2 — Nice to Have (v1 stretch or v2)

- **Gold layer:** LLM-formatted export (transcript + metadata as a single markdown document) per video, derived from Silver
- **Single-video ad hoc fetch:** Fetch and ingest one video by URL outside of a playlist sync
- **MotherDuck backend:** Replace local `.duckdb` with MotherDuck for remote access
- **Streamlit Community Cloud deployment:** Public read-only UI with a separate read-only token

---

## User Stories

1. **As the owner**, I want to run one command against a playlist URL and have all new transcripts land in the database, so I don't have to manually download or manage files.

2. **As the owner**, I want to re-run ingestion at any time and have it only fetch videos not already in the database, so syncs are fast and idempotent.

3. **As the owner**, I want every video to have a failure status even when transcripts are unavailable, so I know exactly why a video has no transcript and can triage later.

4. **As the owner**, I want to run `refresh.py` and have it automatically pick up any videos that previously had no captions but now do, so I don't have to track that manually.

5. **As the owner**, I want to open a UI, type a SQL query, and get results back quickly, so I can explore transcript content without writing scripts for every question.

6. **As the owner**, I want the UI to prevent runaway queries from scanning all transcript text, so the local app stays responsive.

7. **As a portfolio reviewer**, I want to read the README and understand the pipeline architecture, data model, and design decisions without needing to run the code, so I can assess the engineering approach.

8. **As the owner**, I want to click on any video in the browse view and see its full cleaned transcript, so I can read specific content without leaving the UI.

---

## Out of Scope

- **Scheduled / automated sync:** No cron, Airflow, Prefect, or GitHub Actions in v1. Sync is always manually triggered.
- **Caption content update detection:** Changes to existing captions (e.g. YouTube correcting auto-generated text) are not detected. Only presence/absence of captions is tracked.
- **Multi-playlist support:** v1 targets a single playlist. Schema supports it, but ingestion is single-playlist only.
- **Multi-user access or auth:** No login, roles, or concurrent write safety. Single-user local tool only.
- **Docker environment:** Plain `uv` venv is sufficient. Docker is a v3 consideration.
- **dbt transformations:** Transform logic lives in `transform.py`. dbt is a v3 consideration.
- **Non-English transcripts in v1:** English-first; other languages not actively tested or supported.
- **Video or audio download:** Only metadata and VTT captions are fetched. No media files.

---

## Risks and Mitigations

| Risk                                                 | Likelihood | Impact | Mitigation                                                                                          |
| ---------------------------------------------------- | ---------- | ------ | --------------------------------------------------------------------------------------------------- |
| YouTube rate-limiting or blocking `yt-dlp`           | Medium     | High   | Chunked playlist fetches with configurable sleep intervals; `rate_limited` status to allow retry    |
| VTT format changes break the parser                  | Low        | Medium | Fixture-based tests on `transform.py`; parser isolated in `utils.py` for easy patching              |
| DuckDB file corruption on interrupted write          | Low        | High   | Wrap ingest in transactions; Bronze writes are append-only so replaying from scratch is safe        |
| SQL tab allows expensive full-text scans             | Medium     | Medium | Query parser enforces `SELECT`-only and `LIMIT`; statement timeout as fallback                      |
| `yt-dlp` dependency goes unmaintained or changes API | Low        | High   | Pin `yt-dlp` version in `requirements.txt`; isolate fetch logic in `ingest.py` so swap is localized |
| Playlist videos removed by YouTube silently          | Medium     | Low    | `removed_at` tombstone in `playlist_video_membership`; transcripts and metadata are never deleted   |

---

## Timeline and Milestones

| #   | Milestone                 | Exit Criteria                                                                                     |
| --- | ------------------------- | ------------------------------------------------------------------------------------------------- |
| M1  | DuckDB + schema           | Schema applied; `SELECT 1` returns clean; all tables present                                      |
| M2  | `transform.py` + tests    | Fixture VTTs → expected deduped output; edge cases covered                                        |
| M3  | `ingest.py`               | One playlist ingested; Bronze rows populated; failure statuses set for all videos                 |
| M4  | `transform.py` end-to-end | Bronze → Silver for real data; `SELECT * FROM transcripts_silver LIMIT 10` returns readable prose |
| M5  | `refresh.py`              | Diff re-runs cleanly; metadata upserted; newly captioned video detected in a test case            |
| M6  | UI                        | Browse, detail view, and SQL tab all functional locally                                           |
| M7  | Polish                    | README, `.env.example`, known gaps documented; repo is portfolio-presentable                      |
