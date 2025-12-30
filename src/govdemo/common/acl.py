from dataclasses import dataclass
import os
import yaml
from .config import load_env_config

class AccessDenied(RuntimeError):
    pass

@dataclass(frozen=True)
class RolePerms:
    read: set[str]
    write: set[str]

def _load_roles() -> dict[str, RolePerms]:
    cfg = load_env_config()
    data = yaml.safe_load(cfg.roles_path.read_text(encoding="utf-8"))
    roles = data.get("roles", {})
    out: dict[str, RolePerms] = {}
    for name, perms in roles.items():
        out[name] = RolePerms(read=set(perms.get("read", [])), write=set(perms.get("write", [])))
    return out

def current_role() -> str:
    return os.environ.get("GOVDEMO_ROLE", "analyst")

def check_read(layer: str) -> None:
    role = current_role()
    roles = _load_roles()
    if role not in roles or layer not in roles[role].read:
        raise AccessDenied(f"AccessDenied: role '{role}' cannot read layer '{layer}'")

def check_write(layer: str) -> None:
    role = current_role()
    roles = _load_roles()
    if role not in roles or layer not in roles[role].write:
        raise AccessDenied(f"AccessDenied: role '{role}' cannot write layer '{layer}'")
