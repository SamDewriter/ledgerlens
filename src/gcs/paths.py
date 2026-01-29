from __future__ import annotations

def app_events_prefix(date: str, hour: str | None = None) -> str:
    prefix = f"app_events/date={date}/"
    if hour is not None:
        prefix += f"hour={hour}/"
    return prefix

def provider_exports_prefix(provider: str, date: str) -> str:
    return f"provider_exports/provider={provider}/date={date}/"

def refunds_prefix(provider: str, date: str) -> str:
    return f"provider_exports/provider={provider}/date={date}/refunds.jsonl"