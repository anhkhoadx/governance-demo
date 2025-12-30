from dataclasses import dataclass
from pathlib import Path
import os

@dataclass(frozen=True)
class EnvConfig:
    root: Path
    duckdb_path: Path
    lineage_path: Path
    gdpr_evidence_dir: Path
    export_evidence_dir: Path
    roles_path: Path
    pii_secret: str

def load_env_config() -> EnvConfig:
    project_root = Path.cwd()
    lake_root = project_root / "data_lake"
    wh_root = project_root / "warehouse"
    wh_root.mkdir(exist_ok=True)

    roles_path = project_root / "configs" / "roles.local.yaml"
    pii_secret = os.environ.get("PII_TOKEN_SECRET", "dev-secret-change-me")

    return EnvConfig(
        root=lake_root,
        duckdb_path=wh_root / "governance.duckdb",
        lineage_path=wh_root / "lineage.jsonl",
        gdpr_evidence_dir=wh_root / "gdpr_evidence",
        export_evidence_dir=wh_root / "export_evidence",
        roles_path=roles_path,
        pii_secret=pii_secret,
    )
