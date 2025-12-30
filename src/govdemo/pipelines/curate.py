import pyarrow as pa
import pyarrow.parquet as pq
from ..common.acl import check_read, check_write
from ..common.config import load_env_config
from ..common.audit import start_run, finish_run
from ..common.lineage import emit_edge
from ..common.time import today_utc
import collections

FACT_SCHEMA = pa.schema([
    ("dt", pa.string()),
    ("user_id", pa.string()),
    ("events", pa.int64()),
    ("last_event_time", pa.string()),
])

def run_curate(dt: str | None = None) -> dict:
    check_read("clean")
    check_write("curated")
    check_write("warehouse")

    cfg = load_env_config()
    dt = dt or today_utc()
    clean_path = cfg.root/"clean"/"events"/f"dt={dt}"/"part-00001.parquet"
    if not clean_path.exists():
        raise FileNotFoundError(f"Missing clean parquet: {clean_path}. Run `govdemo clean` first.")

    run_id = start_run("curate", input_ref=str(clean_path))

    table = pq.ParquetFile(clean_path).read()
    # compute per-user counts and last seen
    counts = collections.Counter()
    last = {}
    user_ids = table.column("user_id").to_pylist()
    times = table.column("event_time").to_pylist()
    for uid, t in zip(user_ids, times):
        if uid is None:
            continue
        counts[uid] += 1
        last[uid] = max(last.get(uid, ""), t)

    rows = [{"dt": dt, "user_id": str(uid), "events": int(cnt), "last_event_time": str(last.get(uid,""))}
            for uid, cnt in sorted(counts.items())]

    out_dir = cfg.root/"curated"/"facts"/f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir/"fact_user_activity_daily.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=FACT_SCHEMA), out_path, use_dictionary=False)

    emit_edge(run_id, "curate", from_ref=str(clean_path), to_ref=str(out_path))
    finish_run(run_id, "SUCCESS", output_ref=str(out_path), details=f"rows={len(rows)}")
    return {"run_id": run_id, "curated_path": str(out_path), "rows": len(rows)}
