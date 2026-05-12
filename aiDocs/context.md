# Project Context

**YouTube transcript pipeline:** ingest captions (playlist or single video), dedupe segments, expose data for **SQL** and **LLM-friendly exports**. **This file is the index**—linked docs carry the detail.

## Where things live

```text
project-root/
├── aiDocs/          # tracked — shared truth (this file, PRD, MVP, architecture, style, changelog; add ADRs/runbooks as needed)
├── ai/              # gitignored — brainstorming/, guides/, roadmaps/ (scratch & process)
├── scripts/         # CLI scripts for AI automated testing
├── claude.md        # gitignored — personal
└── .cursorrules     # gitignored — personal
```

- **`context.md`:** where to read next—not the only file for every answer.
- **Onboard / operate the repo** → **`aiDocs/`**. **Drafts, research, AI back-and-forth** → **`ai/`** (often `ai/roadmaps/`: **plan** first for depth, then **roadmap** for checklist—promote wins to `aiDocs/`).

## Read these docs

| Topic                    | File                               |
| ------------------------ | ---------------------------------- |
| **PRD**                  | [prd.md](prd.md)                   |
| **MVP**                  | [mvp.md](mvp.md)                   |
| **Architecture**         | [architecture.md](architecture.md) |
| **Style**                | [coding-style.md](coding-style.md) |
| **History / don’t undo** | [changelog.md](changelog.md)       |

## Ops

- **Scripts:** JSON on stdout when tools compose; plain text/Markdown OK for humans.
- **Logs:** structured, **to files** for batch runs.
- **Secrets:** never commit keys or env bundles.
- **Changelog:** every commit → [changelog.md](changelog.md), newest date first; **what & why**, 1–2 lines (memory, not git archaeology).
