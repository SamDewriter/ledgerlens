import re
from decimal import Decimal, InvalidOperation
import re

CURRENCY_SYMBOLS = {"₦":"NGN", "$":"USD", "€":"EUR", "£":"GBP"}

def _clean_amount_str(s: str):
    s = s.strip()
    currency = None
    for sym, code in CURRENCY_SYMBOLS.items():
        if sym in s:
            currency = code
            s = s.replace(sym, "")
    s = s.replace(",", "").strip()
    s = re.sub(r"[^0-9.\-]", "", s)
    return s, currency

def parse_amount(value, default_currency: str = "NGN") -> tuple[float | None, str]:
    currency = default_currency
    if value is None:
        return None, currency
    
    if isinstance(value, (int, float)):
        if isinstance(value, int) and value >= 1000:
            return float(Decimal(value) / Decimal(100), currency)
        return float(value), currency
    
    if isinstance(value, str):
        s, detected = _clean_amount_str(value)
        if detected:
            currency = detected
        if not s:
            return None, currency
        try:
            d = Decimal(s)
        except InvalidOperation:
            return None, currency
        if re.fullmatch(r"\d{4,}", s):
            d = d / Decimal(100)
        return float(d), currency
    return None, currency
