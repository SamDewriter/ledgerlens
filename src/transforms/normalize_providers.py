from __future__ import annotations
from typing import Any, Dict
from transforms.timestamps import normalize_timestamp
from transforms.amounts import parse_amount
from utils.commons import stable_hash


SUCCESS_SET = {"success", "successful", "completed", "paid", "succeeded"}
FAILED_SET = {"failed", "declined", "error", "canceled", "reversed"}

def normalize_status(s: str | None) -> str:
    if not s:
        return "unknown"
    v = str(s).strip().lower()
    if v in SUCCESS_SET:
        return "success"
    if v in FAILED_SET:
        return "failed"
    if "refund" in v: return "refunded"
    if "chargeback" in v: return "chargeback"

def normalize_refund(row: Dict[str, Any], provider: str, default_tz: str = "UTC") -> Dict[str, Any]:
    provider = provider.upper()
    refund_id = str(row.get("refund_id") or "").strip()
    payment_id = str(row.get("tnx_id") or "").strip()
    amount, currency_detected = parse_amount(row.get("refund_amount"), default_currency=row.get("currency", "NGN"))
    ts = normalize_timestamp(row.get("refund_ts"), default_tz=default_tz)

    if not refund_id:
        refund_id = stable_hash(provider, payment_id, str(ts), str(amount))

    return {
        "provider": provider,
        "refund_id": refund_id,
        "amount": amount,
        "currency": (row.get("currency") or currency_detected or "NGN").upper(),
        "refund_ts_utc": ts,
        "reason": str(row.get("reason") or ""),
        "raw": row,
    }


def normalize_provider_payment(row: Dict[str, Any], provider: str, default_tz: str = "UTC") -> Dict[str, Any]:
    provider = provider.upper()
    if provider == "A":
        payment_id = str(row.get("txn_id") or "").strip()
        status_raw = str(row.get("status") or "")
        amount_raw = row.get("amount")
        currency_raw = str(row.get("currency") or "NGN")
        created_at = row.get("created_at") or ""
        order_id = row.get("order_id") or ""
        email = row.get("email") or ""
        phone = row.get("phone") or ""
    else: # provider B
        payment_id = str(row.get("id") or "").strip()
        status_raw = str(row.get("state") or "")
        amount_raw = row.get("value")
        currency_raw = str(row.get("currency") or "NGN")
        created_at = row.get("ts") or ""
        meta = row.get("meta") or {}
        order_id = meta.get("order") or ""
        customer = meta.get("customer") or {}
        email = customer.get("email") or ""
        phone = customer.get("phone") or ""

    status = normalize_status(status_raw)
    amount, currency_detected = parse_amount(amount_raw, default_currency=currency_raw)
    currency = (currency_raw or currency_detected or "NGN").upper()
    ts = normalize_timestamp(created_at, default_tz=default_tz)

    if not payment_id:
        payment_id = stable_hash(provider, str(ts), str(email), str(phone))[:24]
    
    return {
        "provider": provider,
        "payment_id": payment_id,
        "order_id": str(order_id) or "",
        "amount": amount,
        "currency": currency,
        "status": status,
        "payment_ts_utc": ts,
        "email": str(email) or "",
        "phone": str(phone) or "",
        "raw": row,
    }