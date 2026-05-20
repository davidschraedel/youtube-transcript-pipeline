"""Structured JSON logger for pipeline scripts.

All operational output goes to stderr as newline-delimited JSON objects.
Callers emit machine-readable summaries to stdout themselves.

Level control:
    LOG_LEVEL=DEBUG   emit debug + info + warn + error
    LOG_LEVEL=INFO    (default) emit info + warn + error
    LOG_LEVEL=WARN    emit warn + error only
    LOG_LEVEL=ERROR   emit error only
"""

import json
import os
import sys
from datetime import datetime, timezone

_LEVELS: dict[str, int] = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}
_LOG_LEVEL: int = _LEVELS.get(os.getenv("LOG_LEVEL", "INFO").upper(), 20)


def _emit(level: str, data: dict) -> None:
    if _LEVELS.get(level, 20) < _LOG_LEVEL:
        return
    data["level"] = level
    data.setdefault("ts", datetime.now(timezone.utc).isoformat())
    print(json.dumps(data, default=str), file=sys.stderr)


def debug(data: dict) -> None:
    _emit("DEBUG", data)


def info(data: dict) -> None:
    _emit("INFO", data)


def warn(data: dict) -> None:
    _emit("WARN", data)


def error(data: dict) -> None:
    _emit("ERROR", data)
