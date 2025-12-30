from datetime import datetime, timezone

def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
