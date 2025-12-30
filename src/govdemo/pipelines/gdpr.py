from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4
from datetime import datetime
import json
import duckdb
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc

from ..common.config import load_env_config
from ..common.acl import check_write
from ..common.audit import init_audit, start_run, finish_run
from ..common.lineage import emit_edge

@dataclass(frozen=True)
class GDPRResult:
    request_id: str
    run_id: str
    user_id: str
    mode: str
    cleaned_files: int
    curated_files: int
    serving_files: int
    identity_files: int
    evidence_path: str

def _rewrite_parquet_excluding_user(path: Path, user_id: str) -> bool:
    if not path.exists():
        return False
    table = pq.ParquetFile(path).read()
    if "user_id" not in table.column_names:
        return False
    before = table.num_rows
    mask = pc.not_equal(table["user_id"], pa.scalar(user_id))
    table2 = table.filter(mask)
    if table2.num_rows == before:
        return False
    pq.write_table(table2, path, use_dictionary=False)
    return True

def request_delete(user_id: str, mode: str = "delete", dt: str | None = None) -> GDPRResult:
    check_write("warehouse")
    check_write("clean")
    check_write("curated")
    check_write("serving")
    check_write("restricted_pii")

    cfg = load_env_config()
    init_audit()

    request_id = str(uuid4())
    con = duckdb.connect(str(cfg.duckdb_path))
    con.execute(
        "insert into gdpr_requests values (?, ?, ?, ?, ?, ?)",
        [request_id, user_id, mode, datetime.utcnow().isoformat()+"Z", "RECEIVED", ""],
    )
    con.close()

    run_id = start_run("gdpr_delete", input_ref=f"user_id={user_id}")

    clean_files = sorted((cfg.root/"clean"/"events").glob("dt=*/part-00001.parquet")) if dt is None else [cfg.root/"clean"/"events"/f"dt={dt}"/"part-00001.parquet"]
    curated_files = sorted((cfg.root/"curated"/"facts").glob("dt=*/fact_user_activity_daily.parquet")) if dt is None else [cfg.root/"curated"/"facts"/f"dt={dt}"/"fact_user_activity_daily.parquet"]
    serving_files = sorted((cfg.root/"serving"/"user_metrics").glob("dt=*/user_metrics.parquet")) if dt is None else [cfg.root/"serving"/"user_metrics"/f"dt={dt}"/"user_metrics.parquet"]
    identity_files = sorted((cfg.root/"restricted_pii"/"identity").glob("dt=*/identity.parquet")) if dt is None else [cfg.root/"restricted_pii"/"identity"/f"dt={dt}"/"identity.parquet"]

    changed_clean = sum(1 for p in clean_files if _rewrite_parquet_excluding_user(p, user_id))
    changed_curated = sum(1 for p in curated_files if _rewrite_parquet_excluding_user(p, user_id))
    changed_serving = sum(1 for p in serving_files if _rewrite_parquet_excluding_user(p, user_id))
    changed_identity = sum(1 for p in identity_files if _rewrite_parquet_excluding_user(p, user_id))

    cfg.gdpr_evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = cfg.gdpr_evidence_dir / f"{request_id}.json"
    evidence = {
        "request_id": request_id,
        "run_id": run_id,
        "user_id": user_id,
        "mode": mode,
        "changed": {
            "clean_files": changed_clean,
            "curated_files": changed_curated,
            "serving_files": changed_serving,
            "identity_files": changed_identity,
        },
        "at": datetime.utcnow().isoformat()+"Z",
        "notes": "Raw is immutable; deletes propagate to clean/curated/serving/restricted_pii.",
    }
    evidence_path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")

    emit_edge(run_id, "gdpr_delete", from_ref=f"user_id={user_id}", to_ref=str(evidence_path))
    finish_run(run_id, "SUCCESS", output_ref=str(evidence_path), details=json.dumps(evidence["changed"]))

    con = duckdb.connect(str(cfg.duckdb_path))
    con.execute(
        "update gdpr_requests set status=?, details=? where request_id=?",
        ["FULFILLED", json.dumps(evidence["changed"]), request_id],
    )
    con.close()

    return GDPRResult(
        request_id=request_id,
        run_id=run_id,
        user_id=user_id,
        mode=mode,
        cleaned_files=changed_clean,
        curated_files=changed_curated,
        serving_files=changed_serving,
        identity_files=changed_identity,
        evidence_path=str(evidence_path),
    )
