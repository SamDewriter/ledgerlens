from typing import Any, Dict
from transforms.amounts import parse_amount
from transforms.timestamps import normalize_timestamp


def normalize_order(row: Dict[str, Any], default_tz: str = "UTC") -> Dict[str, Any]:
    amount, currency_detected = parse_amount(row.get("order_amount"), default_currency=row.get("currency", "NGN"))
    ts = normalize_timestamp(row.get("order_ts"), default_tz=default_tz)

    return {
        "order_id": str(row.get("order_id", "")).strip(),
        "user_id": str(row.get("user_id") or ""),
        "email": str(row.get("email") or ""),
        "phone": str(row.get("phone") or ""),
        "country": str(row.get("country") or "").upper(),
        "product_id": str(row.get("product_id") or ""),
        "product_name": str(row.get("product_name") or ""),
        "order_amount": amount,
        "currency": (row.get("currency") or currency_detected or "NGN").upper(),
        "order_ts_utc": ts,
        "status": str(row.get("status") or "").lower(),
        "raw": row,    
    }