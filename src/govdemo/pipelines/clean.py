import json
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from ..common.acl import check_read, check_write
from ..common.config import load_env_config
from ..common.audit import start_run, finish_run
from ..common.lineage import emit_edge
from ..common.pii import token
from ..common.time import today_utc

SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("user_id", pa.string()),
    ("event_time", pa.string()),
    ("email_token", pa.string()),
    ("ip_token", pa.string()),
])

def run_clean(dt: str | None = None) -> dict:
    check_read("raw")
    check_write("clean")
    check_write("warehouse")

    cfg = load_env_config()
    dt = dt or today_utc()
    raw_path = cfg.root/"raw"/"events"/f"dt={dt}"/"source=app"/"part-00001.jsonl"
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing raw file: {raw_path}. Run `govdemo ingest` first.")

    run_id = start_run("clean", input_ref=str(raw_path))

    rows = []
    with raw_path.open("r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            rows.append({
                "event_id": str(r.get("event_id")),
                "user_id": str(r.get("user_id")),
                "event_time": str(r.get("event_time")),
                "email_token": token(str(r.get("email",""))),
                "ip_token": token(str(r.get("ip_address",""))),
            })

    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    out_dir = cfg.root/"clean"/"events"/f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir/"part-00001.parquet"
    pq.write_table(table, out_path, use_dictionary=False)

    emit_edge(run_id, "clean", from_ref=str(raw_path), to_ref=str(out_path))
    finish_run(run_id, "SUCCESS", output_ref=str(out_path), details=f"rows={len(rows)}")
    return {"run_id": run_id, "clean_path": str(out_path), "rows": len(rows)}
