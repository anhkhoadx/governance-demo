import json
import pyarrow as pa
import pyarrow.parquet as pq
from ..common.acl import check_read, check_write
from ..common.config import load_env_config
from ..common.audit import start_run, finish_run
from ..common.lineage import emit_edge
from ..common.time import today_utc

IDENTITY_SCHEMA = pa.schema([
    ("dt", pa.string()),
    ("user_id", pa.string()),
    ("email", pa.string()),
])

def run_build_identity(dt: str | None = None) -> dict:
    """Build a restricted identity table from raw (PII zone).

    Governance:
    - identity contains PII and must live in restricted_pii/
    - only activation/ops roles should read it
    """
    check_read("raw")
    check_write("restricted_pii")
    check_write("warehouse")

    cfg = load_env_config()
    dt = dt or today_utc()
    raw_path = cfg.root/"raw"/"events"/f"dt={dt}"/"source=app"/"part-00001.jsonl"
    if not raw_path.exists():
        raise FileNotFoundError(f"Missing raw file: {raw_path}. Run `govdemo ingest` first.")

    run_id = start_run("build_identity", input_ref=str(raw_path))

    latest_email = {}
    with raw_path.open("r", encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            uid = str(r.get("user_id"))
            email = str(r.get("email",""))
            if uid and email:
                latest_email[uid] = email

    rows = [{"dt": dt, "user_id": uid, "email": email} for uid, email in sorted(latest_email.items())]

    out_dir = cfg.root/"restricted_pii"/"identity"/f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir/"identity.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=IDENTITY_SCHEMA), out_path, use_dictionary=False)

    emit_edge(run_id, "build_identity", from_ref=str(raw_path), to_ref=str(out_path))
    finish_run(run_id, "SUCCESS", output_ref=str(out_path), details=f"rows={len(rows)}")
    return {"run_id": run_id, "identity_path": str(out_path), "rows": len(rows)}
