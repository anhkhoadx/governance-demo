import json
from pathlib import Path
from ..common.config import load_env_config
from ..common.acl import check_write
from ..common.time import today_utc, now_iso

def run_seed() -> dict:
    cfg = load_env_config()
    check_write("landing")
    dt = today_utc()
    landing = cfg.root/"landing"
    landing.mkdir(parents=True, exist_ok=True)
    path = landing/"events.jsonl"

    # includes one invalid record (missing event_id) to demonstrate quarantine
    rows = [
        {"event_id":"e1","user_id":"u1","event_time":now_iso(),"email":"u1@example.com","ip_address":"1.1.1.1","source":"app"},
        {"event_id":"e2","user_id":"u1","event_time":now_iso(),"email":"u1@example.com","ip_address":"1.1.1.1","source":"app"},
        {"event_id":"e3","user_id":"u2","event_time":now_iso(),"email":"u2@example.com","ip_address":"2.2.2.2","source":"app"},
        {"user_id":"u3","event_time":now_iso(),"email":"u3@example.com","ip_address":"3.3.3.3","source":"app"}, # invalid
    ]
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r)+"\n")
    return {"landing_file": str(path), "rows": len(rows), "dt": dt}
