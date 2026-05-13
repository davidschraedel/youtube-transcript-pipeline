# Changelog

**Purpose:** quick history **without git archaeology**—what matters, **what not to undo**, and enough signal to **stop repeating dead-end fixes**.

**On every commit:** add a line under **today’s date** (newest date at top). **What changed and why**, not how. **1–2 lines** per bullet; be terse.

---

## 2026-05-13

- Expanded `.gitignore` (env files, keys, `secrets/`, common credential paths) so secrets stay out of the repo by default.
- Added `aiDocs/mvp.md` with MVP scope (PRD P0: ingest/transform, Bronze/Silver, local DuckDB) so delivery targets stay explicit.
- Added `aiDocs/architecture/2026-05-13_mvp-architecture.mmd` — Mermaid overview of the MVP path (yt-dlp → Bronze → Silver → SQL consumption).

## 2026-05-07

- Added this file and changelog maintenance rules in `context.md` so intent stays in-repo.
- Changelog = durable memory (constraints, decisions), not a dump of every edit.
- Made repo documentation norms explicit in `context.md` (durable vs scratch, plan→roadmap) so onboarding isn’t ambiguous.
- Shrank `context.md` so repo layout and where to read stay obvious without duplicate link lists.
