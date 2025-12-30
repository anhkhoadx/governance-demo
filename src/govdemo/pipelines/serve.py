import pyarrow as pa
import pyarrow.parquet as pq
from ..common.acl import check_read, check_write
from ..common.config import load_env_config
from ..common.audit import start_run, finish_run
from ..common.lineage import emit_edge
from ..common.time import today_utc

SERVING_SCHEMA = pa.schema([
    ("dt", pa.string()),
    ("user_id", pa.string()),
    ("events", pa.int64()),
    ("last_seen", pa.string()),
])

def run_serve(dt: str | None = None) -> dict:
    check_read("curated")
    check_write("serving")
    check_write("warehouse")

    cfg = load_env_config()
    dt = dt or today_utc()
    curated_path = cfg.root/"curated"/"facts"/f"dt={dt}"/"fact_user_activity_daily.parquet"
    if not curated_path.exists():
        raise FileNotFoundError(f"Missing curated fact: {curated_path}. Run `govdemo curate` first.")

    run_id = start_run("serve", input_ref=str(curated_path))

    table = pq.ParquetFile(curated_path).read()
    # rename last_event_time -> last_seen
    names = table.column_names
    if "last_event_time" in names:
        table = table.rename_columns([("last_seen" if c == "last_event_time" else c) for c in names])
    table = table.cast(SERVING_SCHEMA, safe=False)

    out_dir = cfg.root/"serving"/"user_metrics"/f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir/"user_metrics.parquet"
    pq.write_table(table, out_path, use_dictionary=False)

    emit_edge(run_id, "serve", from_ref=str(curated_path), to_ref=str(out_path))
    finish_run(run_id, "SUCCESS", output_ref=str(out_path), details=f"rows={table.num_rows}")
    return {"run_id": run_id, "serving_path": str(out_path), "rows": int(table.num_rows)}
