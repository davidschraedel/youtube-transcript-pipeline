"""
Shared helpers for the YouTube transcript pipeline.

Functions:
    parse_vtt(raw_vtt)             — strip VTT formatting, return plain text
    dedupe_repeated_phrases(text)  — remove consecutively repeated n-grams
    classify_failure(returncode, stderr) — map yt-dlp failure to a status string
"""

import html
import re


# ---------------------------------------------------------------------------
# VTT parsing
# ---------------------------------------------------------------------------

def parse_vtt(raw_vtt: str) -> str:
    """Strip VTT formatting and return plain prose.

    Removes:
    - Header lines (WEBVTT, Kind:, Language:)
    - Timestamp range lines (00:00:00.000 --> 00:00:00.000 ...)
    - Inline timestamp tags (<01:07:24.079>)
    - Caption tags (<c>, </c>)

    Then:
    - Unescapes HTML entities (&amp; → &, etc.)
    - Normalizes >> speaker-change markers to newline-prefixed form
    - Normalizes whitespace
    """
    cleaned: list[str] = []

    for line in raw_vtt.splitlines():
        # Remove timestamp range lines
        line = re.sub(
            r"\d{2}:\d{2}:\d{2}\.\d{3}\s*-->\s*\d{2}:\d{2}:\d{2}\.\d{3}[^\n]*",
            "",
            line,
        )
        # Remove inline timestamp tags: <01:07:24.079>
        line = re.sub(r"<\d{2}:\d{2}:\d{2}\.\d+>", "", line)
        # Remove caption tags: <c> and </c>
        line = re.sub(r"</?c>", "", line)
        # Normalize whitespace on this line
        line = re.sub(r"\s+", " ", line).strip()
        # Skip VTT headers and blank lines
        if line and not line.startswith(("WEBVTT", "Kind:", "Language:")):
            cleaned.append(line)

    text = " ".join(cleaned)
    text = html.unescape(text)
    text = text.replace(">>", "\n>> ")
    return text


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def dedupe_repeated_phrases(text: str, max_ngram: int = 12) -> str:
    """Remove consecutively repeated n-grams from text (v3 algorithm).

    Auto-generated YouTube captions overlap: the same phrase appears in
    multiple consecutive caption blocks. This function walks the word list
    and, for each position, checks whether the next N words repeat
    consecutively. It tries the longest n-grams first (max_ngram → 1) so
    multi-word runs are collapsed before single words. Repeating phrases are
    emitted once; all consecutive duplicates are skipped.
    """
    words = text.split()
    output: list[str] = []
    i = 0

    while i < len(words):
        added = False
        for n in range(max_ngram, 0, -1):
            if i + n <= len(words):
                phrase = words[i : i + n]
                # Count consecutive repetitions of this phrase
                repetitions = 1
                j = i + n
                while j + n <= len(words) and words[j : j + n] == phrase:
                    repetitions += 1
                    j += n
                if repetitions > 1:
                    output.extend(phrase)
                    i += n * repetitions
                    added = True
                    break
        if not added:
            output.append(words[i])
            i += 1

    return " ".join(output)


# ---------------------------------------------------------------------------
# Failure classification
# ---------------------------------------------------------------------------

_RATE_LIMITED_PATTERNS = [
    "too many requests",
    "429",
    "rate limit",
    "http error 429",
]

_UNAVAILABLE_PATTERNS = [
    "video unavailable",
    "private video",
    "has been removed",
    "not available",
    "members-only",
    "this video is unavailable",
    "410",
    "403",
]

_NO_SUBTITLES_PATTERNS = [
    "no subtitles",
    "no automatic captions",
    "there are no subtitles",
    "subtitles not available",
]


def classify_failure(returncode: int, stderr: str) -> str:
    """Map a yt-dlp exit code + stderr to a fetch_status string.

    Returns one of: 'ok', 'no_subtitles', 'unavailable', 'rate_limited'.

    'ok' is returned only when returncode is 0 (caller should check for VTT
    file existence separately before relying on that status).
    """
    if returncode == 0:
        return "ok"

    lower = stderr.lower()

    for pattern in _RATE_LIMITED_PATTERNS:
        if pattern in lower:
            return "rate_limited"

    for pattern in _NO_SUBTITLES_PATTERNS:
        if pattern in lower:
            return "no_subtitles"

    for pattern in _UNAVAILABLE_PATTERNS:
        if pattern in lower:
            return "unavailable"

    # Default: treat unknown non-zero exit as unavailable
    return "unavailable"
