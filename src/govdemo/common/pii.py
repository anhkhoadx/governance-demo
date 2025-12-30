import hashlib
from .config import load_env_config

def token(value: str) -> str:
    cfg = load_env_config()
    h = hashlib.sha256()
    h.update((value + cfg.pii_secret).encode("utf-8"))
    return h.hexdigest()
