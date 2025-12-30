import json
from .config import load_env_config
from .time import now_iso

def emit_edge(run_id: str, pipeline: str, from_ref: str, to_ref: str) -> None:
    cfg = load_env_config()
    rec = {
        "run_id": run_id,
        "pipeline": pipeline,
        "from_ref": from_ref,
        "to_ref": to_ref,
        "at": now_iso(),
    }
    cfg.lineage_path.parent.mkdir(exist_ok=True)
    with cfg.lineage_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
