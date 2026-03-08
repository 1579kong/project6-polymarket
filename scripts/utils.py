from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any

import requests


ET = ZoneInfo("America/New_York")


def safe_request(url: str, params: dict | None = None, timeout: int = 25) -> Any:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def market_phase_from_utc(ts_utc: datetime) -> str:
    et = ts_utc.astimezone(ET)
    t = et.time()
    if t.hour < 9 or (t.hour == 9 and t.minute < 30):
        return "pre"
    if (t.hour == 9 and t.minute >= 30) or t.hour == 10:
        return "open"
    if 11 <= t.hour <= 14:
        return "mid"
    if t.hour == 15 or (t.hour == 16 and t.minute == 0):
        return "close"
    return "after"


def to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default
