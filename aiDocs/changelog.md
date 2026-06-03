# Changelog

**Purpose:** quick history **without git archaeology**—what matters, **what not to undo**, and enough signal to **stop repeating dead-end fixes**.

**On every commit:** add a line under **today’s date** (newest date at top). **What changed and why**, not how. **1–2 lines** per bullet; be terse.

---

## 2026-06-02

- Phase 4 (M4) complete: ran `transform.py` on 1,048 pending Bronze rows (1,057 total); silver count matches ok videos; artifact scan clean on all rows; added `scripts/validate_transform.py` for repeatable count + VTT artifact checks.

## 2026-05-18

- `ingest.py`: rate-limited videos skip DB writes so re-runs retry them instead of treating them as ingested.
- Phase 3 (M3) complete: `pipeline/ingest.py` — flat-playlist ID fetch (`-j`), DB diff for incremental ingest, chunked per-video caption+metadata download (`--skip-download --write-auto-sub --sub-lang en --write-info-json`), per-video failure classification via `classify_failure`, per-chunk DuckDB transactions, upserts to `videos`/`playlist_video_membership`, append-only Bronze transcript writes, temp-file cleanup; configurable via env vars.
- Phase 2 (M2) complete: `pipeline/utils.py` with `parse_vtt`, `dedupe_repeated_phrases` (v3, max_ngram=12), and `classify_failure`; `pipeline/transform.py` (Bronze→Silver, idempotent, `--force` flag); 5 VTT fixtures; 32 pytest tests all green.
- Added `[build-system]` + `[tool.hatch.build.targets.wheel]` to `pyproject.toml` so `pipeline` is installable via `uv sync` and importable in scripts without path hacks.
- Added `pipeline/schema.py` with idempotent DDL for Bronze (`videos`, `playlist_video_membership`, `transcripts_bronze`) and Silver (`transcripts_silver`); `fetch_status` constrained to the four pipeline outcomes so ingest can classify failures consistently.
- Phase 1 (M1) complete: schema applies via `uv run python pipeline/schema.py`; smoke test checks `SELECT 1` and all four tables exist.
- Initialized uv project (`pyproject.toml`, `uv.lock`, Python 3.13) with pinned runtime deps (`duckdb`, `yt-dlp`, `streamlit`, `python-dotenv`) and dev deps (`pytest`) so installs are reproducible before pipeline code lands.
- Phase 0 scaffold: `.env.example`, `.gitignore` (DB, env, venv, temp VTT), and `pipeline/`, `gui/`, `tests/fixtures/` layout aligned to the implementation plan.

## 2026-05-13

- Expanded `.gitignore` (env files, keys, `secrets/`, common credential paths) so secrets stay out of the repo by default.
- Added `aiDocs/mvp.md` with MVP scope (PRD P0: ingest/transform, Bronze/Silver, local DuckDB) so delivery targets stay explicit.
- Added `aiDocs/architecture/2026-05-13_mvp-architecture.mmd` — Mermaid overview of the MVP path (yt-dlp → Bronze → Silver → SQL consumption).

## 2026-05-07

- Added this file and changelog maintenance rules in `context.md` so intent stays in-repo.
- Changelog = durable memory (constraints, decisions), not a dump of every edit.
- Made repo documentation norms explicit in `context.md` (durable vs scratch, plan→roadmap) so onboarding isn’t ambiguous.
- Shrank `context.md` so repo layout and where to read stay obvious without duplicate link lists.
