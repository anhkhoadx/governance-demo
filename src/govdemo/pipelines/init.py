from ..common.config import load_env_config
from ..common.audit import init_audit
from ..common.acl import check_write

def run_init() -> dict:
    cfg = load_env_config()
    check_write("warehouse")
    # create folders
    for p in [
        cfg.root/"landing",
        cfg.root/"raw",
        cfg.root/"clean",
        cfg.root/"curated",
        cfg.root/"serving",
        cfg.root/"restricted_pii",
        cfg.root/"exports",
        cfg.root/"quarantine",
    ]:
        p.mkdir(parents=True, exist_ok=True)

    cfg.gdpr_evidence_dir.mkdir(parents=True, exist_ok=True)
    cfg.export_evidence_dir.mkdir(parents=True, exist_ok=True)
    init_audit()
    return {"lake_root": str(cfg.root), "duckdb": str(cfg.duckdb_path), "lineage": str(cfg.lineage_path)}
