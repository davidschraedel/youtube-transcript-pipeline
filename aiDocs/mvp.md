# Minimum Viable Product — YoutubeTranscriptPipeline

**Status:** Draft  
**Aligned with:** [prd.md](./prd.md)  
**Last updated:** 2026-05-12

---

## Core problem

Make an entire YouTube playlist’s spoken content **locally searchable and analyzable**: ingest captions and metadata into a **local DuckDB** database so they can be queried with SQL (or exported for LLM use) without relying on YouTube’s UI or heavyweight scraping services.

---

## In scope (MVP)

MVP equals **PRD P0** only:

| Deliverable        | Notes                                                                                                                                                                                                                                |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **`ingest.py`**    | One playlist URL; `yt-dlp --flat-playlist`; diff vs DB; fetch VTT + metadata for new IDs; land **Bronze**; per-video **`fetch_status`** (`ok`, `no_subtitles`, `unavailable`, `rate_limited`); chunked runs / sleeps for rate limits |
| **Bronze**         | Tables: `videos`, `playlist_video_membership`, `transcripts_bronze`; append/upsert; `transcripts_bronze` never blindly overwritten                                                                                                   |
| **`transform.py`** | Parse VTT → strip timestamps/tags → dedupe segments → **Silver** `transcripts_silver`; idempotent, re-runnable                                                                                                                       |
| **Silver**         | `transcripts_silver`: `video_id`, `full_text`, `source_language`; re-derivable from Bronze                                                                                                                                           |

**Consumption:** Any SQL client or script against the local `.duckdb` file (no UI required for MVP).

---

## Explicitly out of MVP (defer)

- **`refresh.py`** — caption/metadata refresh and tombstones (PRD P1)
- **Streamlit UI** — browse, detail, guarded SQL tab (PRD P1)
- **PRD P2** — Gold/LLM export, single-video fetch, MotherDuck, cloud deploy
- **Polish** beyond what’s needed to run ingest/transform and document the pipeline (full README polish can follow MVP or stay minimal)

---

## Technical approach

**Python + pinned `yt-dlp` + local DuckDB** — manual runs, single playlist, no scheduler, Docker, dbt, or multi-user concerns (per PRD out-of-scope).

---

## Success criteria

- Full playlist run: every video has a row in `videos` and a defined **`fetch_status`**; captioned videos have Bronze (and after transform, Silver) transcript data.
- Re-run **ingest** only fetches **new** videos (incremental by diff).
- `transform.py` produces readable prose in `transcripts_silver` (no VTT artifacts); safe to re-run.
- **Primary validation:** you can answer real questions with SQL that YouTube’s UI does not support.
- **Secondary validation:** a reviewer can follow docs and understand **ingest → Bronze → transform → Silver** without needing a GUI.

---

## Reference

Full requirements, risks, and milestones: [prd.md](./prd.md).
