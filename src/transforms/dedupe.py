from __future__ import annotations
from typing import Any, Set, Iterable, List, Dict


def dedupe_by_key(rows: Iterable[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    seen = set()
    out = [] # Duplicates
    for r in rows:
        k = r.get(key)
        if not k:
            out.append(r)
            continue
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out
