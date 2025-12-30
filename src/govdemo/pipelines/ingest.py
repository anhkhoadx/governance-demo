import json
from pathlib import Path
from ..common.acl import check_read, check_write
from ..common.config import load_env_config
from ..common.audit import start_run, finish_run
from ..common.lineage import emit_edge
from ..common.time import today_utc, now_iso

REQUIRED = ["event_id", "user_id", "event_time"]

def run_ingest(source: str = "app", dt: str | None = None) -> dict:
    check_read("landing")
    check_write("raw")
    check_write("quarantine")
    check_write("warehouse")

    cfg = load_env_config()
    dt = dt or today_utc()

    landing_file = cfg.root/"landing"/"events.jsonl"
    if not landing_file.exists():
        raise FileNotFoundError(f"Missing landing file: {landing_file}. Run `govdemo seed` first.")

    run_id = start_run("ingest", input_ref=str(landing_file))

    out_raw_dir = cfg.root/"raw"/"events"/f"dt={dt}"/f"source={source}"
    out_raw_dir.mkdir(parents=True, exist_ok=True)
    out_raw = out_raw_dir/"part-00001.jsonl"

    q_dir = cfg.root/"quarantine"/"events"/f"dt={dt}"/"reason=MISSING_EVENT_ID"
    q_dir.mkdir(parents=True, exist_ok=True)
    out_q = q_dir/"part-00001.jsonl"

    good = bad = 0
    with landing_file.open("r", encoding="utf-8") as fin, out_raw.open("w", encoding="utf-8") as fgood, out_q.open("w", encoding="utf-8") as fbad:
        for line in fin:
            rec = json.loads(line)
            missing = [k for k in REQUIRED if not rec.get(k)]
            if missing:
                bad += 1
                fbad.write(json.dumps(rec)+"\n")
                continue
            rec["_ingested_at"] = now_iso()
            rec["_source"] = source
            rec["_raw_file"] = str(landing_file)
            good += 1
            fgood.write(json.dumps(rec)+"\n")

    emit_edge(run_id, "ingest", from_ref=str(landing_file), to_ref=str(out_raw))
    finish_run(run_id, "SUCCESS", output_ref=str(out_raw), details=f"good={good},bad={bad}")

    return {"run_id": run_id, "raw_path": str(out_raw), "quarantine_path": str(out_q), "good": good, "bad": bad}
