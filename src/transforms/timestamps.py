from __future__ import annotations
from datetime import datetime
from utils.commons import parse_to_dt, to_utc

def normalize_timestamp(ts: str | None, default_tz: str = "UTC") -> datetime | None:
    if not ts:
        return None
    try:
        return to_utc(parse_to_dt(ts, default_tz))
    except Exception:
        return None
