import csv
import json
from pathlib import Path
from uuid import uuid4
import duckdb
import pyarrow.parquet as pq
import pyarrow as pa

from ..common.acl import check_read, check_write, current_role
from ..common.config import load_env_config
from ..common.audit import start_run, finish_run, init_audit
from ..common.lineage import emit_edge
from ..common.time import today_utc, now_iso

def run_export_audience(min_events: int = 1, dt: str | None = None) -> dict:
    """Controlled export that resolves PII for operational needs.

    Reads:
      - curated facts (audience definition)
      - restricted identity (PII resolution)
    Writes:
      - exports/audience/*.csv
      - audit row + evidence json
    """
    check_read("curated")
    check_read("restricted_pii")
    check_write("exports")
    check_write("warehouse")

    cfg = load_env_config()
    dt = dt or today_utc()

    curated_path = cfg.root/"curated"/"facts"/f"dt={dt}"/"fact_user_activity_daily.parquet"
    identity_path = cfg.root/"restricted_pii"/"identity"/f"dt={dt}"/"identity.parquet"
    if not curated_path.exists():
        raise FileNotFoundError(f"Missing curated fact: {curated_path}. Run `govdemo curate` first.")
    if not identity_path.exists():
        raise FileNotFoundError(f"Missing identity table: {identity_path}. Run `govdemo build-identity` first.")

    export_id = str(uuid4())
    run_id = start_run("export_audience", input_ref=f"{curated_path} + {identity_path}")

    facts = pq.ParquetFile(curated_path).read()
    ids = pq.ParquetFile(identity_path).read()

    # Build identity map (small demo scale)
    id_map = {u: e for u, e in zip(ids["user_id"].to_pylist(), ids["email"].to_pylist())}

    # Filter audience by min_events
    rows = []
    for uid, ev in zip(facts["user_id"].to_pylist(), facts["events"].to_pylist()):
        if int(ev) >= int(min_events):
            email = id_map.get(uid, "")
            rows.append({"user_id": uid, "email": email, "min_events": int(min_events), "dt": dt})

    out_dir = cfg.root/"exports"/"audience"/f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir/"audience.csv"

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["dt","min_events","user_id","email"])
        w.writeheader()
        for r in rows:
            w.writerow({"dt": r["dt"], "min_events": r["min_events"], "user_id": r["user_id"], "email": r["email"]})

    # evidence
    cfg.export_evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = cfg.export_evidence_dir/f"{export_id}.json"
    evidence = {
        "export_id": export_id,
        "run_id": run_id,
        "requested_by_role": current_role(),
        "dt": dt,
        "min_events": int(min_events),
        "rows": len(rows),
        "output_path": str(out_path),
        "created_at": now_iso(),
        "notes": "Activation export joins curated audience with restricted identity (PII).",
    }
    evidence_path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")

    # store export in audit db
    init_audit()
    con = duckdb.connect(str(cfg.duckdb_path))
    con.execute(
        "insert into activation_exports values (?, ?, ?, ?, ?, ?, ?)",
        [export_id, current_role(), dt, int(min_events), str(out_path), now_iso(), len(rows)],
    )
    con.close()

    emit_edge(run_id, "export_audience", from_ref=f"{curated_path} + {identity_path}", to_ref=str(out_path))
    finish_run(run_id, "SUCCESS", output_ref=str(out_path), details=f"rows={len(rows)}")

    return {"export_id": export_id, "run_id": run_id, "output_path": str(out_path), "rows": len(rows), "evidence": str(evidence_path)}
