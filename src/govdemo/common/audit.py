import duckdb
from .config import load_env_config
from .time import now_iso
from uuid import uuid4

def init_audit() -> None:
    cfg = load_env_config()
    con = duckdb.connect(str(cfg.duckdb_path))
    con.execute("""
      create table if not exists audit_runs (
        run_id varchar,
        pipeline varchar,
        status varchar,
        started_at varchar,
        finished_at varchar,
        input_ref varchar,
        output_ref varchar,
        details varchar
      );
    """)
    con.execute("""
      create table if not exists gdpr_requests (
        request_id varchar,
        user_id varchar,
        mode varchar,
        requested_at varchar,
        status varchar,
        details varchar
      );
    """)
    con.execute("""
      create table if not exists activation_exports (
        export_id varchar,
        requested_by_role varchar,
        dt varchar,
        min_events integer,
        output_path varchar,
        created_at varchar,
        rows integer
      );
    """)
    con.close()

def start_run(pipeline: str, input_ref: str = "") -> str:
    init_audit()
    cfg = load_env_config()
    run_id = str(uuid4())
    con = duckdb.connect(str(cfg.duckdb_path))
    con.execute(
        "insert into audit_runs values (?, ?, ?, ?, ?, ?, ?, ?)",
        [run_id, pipeline, "RUNNING", now_iso(), "", input_ref, "", ""],
    )
    con.close()
    return run_id

def finish_run(run_id: str, status: str, output_ref: str = "", details: str = "") -> None:
    cfg = load_env_config()
    con = duckdb.connect(str(cfg.duckdb_path))
    con.execute(
        "update audit_runs set status=?, finished_at=?, output_ref=?, details=? where run_id=?",
        [status, now_iso(), output_ref, details, run_id],
    )
    con.close()
