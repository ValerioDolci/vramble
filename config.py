"""vramble configuration: a single source for paths, endpoints and secrets.

Precedence (strongest first): environment variable → `config.yaml` → default.
The file is looked up in `$VRAMBLE_CONFIG`, then next to this module, then `~/.config/vramble/config.yaml`.

Nothing in this package may hardcode somebody's paths or ports: if you need a new value, add it
here with a sensible default and document it in `config.example.yaml`.
"""
from __future__ import annotations   # so the module imports on older pythons too

import os

try:
    import yaml
except ImportError:                                  # the CLIs run without pyyaml too
    yaml = None

DEFAULT = {
    # --- where vramble lives
    "base": os.path.expanduser("~/vramble"),            # registry, catalog, database
    "port": 8099,                                   # vramble HTTP (lease, queue, proxy)
    "listen": "127.0.0.1",
    # A unix socket beside the port: there the caller is identified by the kernel, not by a header.
    # Empty disables it. The CLIs prefer it when it exists.
    "socket": os.path.expanduser("~/vramble/vramble.sock"),
    "token": "",                                     # when set, POSTs must carry X-Vramble
    # POST /api/jobs runs an arbitrary argv: closed unless you set a token for it. Everything else
    # (the catalog, the lease, the proxy) only runs commands declared in YAML, so it stays open.
    "submit_token": "",
    "state_file": "",                                # where to dump the state (empty = do not write it)
    # --- the services it talks to
    "swap_url": "http://127.0.0.1:8081",             # OpenAI-compatible endpoint of the models
    "comfy_url": "http://127.0.0.1:8188",
    "acestep_url": "http://127.0.0.1:8001",
    # --- limits
    "timeout_llm": 1800,                             # cap on an answer forwarded by the proxy
    "timeout_drain": 120,                            # cap on a drain command from the registry
    "body_max": 4_000_000,                          # bytes accepted in a POST
    "history_days": 30,                            # pruning of finished jobs
    "jobs_kept": 2000,
}

_ENV = {
    "base": "VRAMBLE_BASE", "port": "VRAMBLE_PORT", "listen": "VRAMBLE_LISTEN",
    "token": "VRAMBLE_TOKEN", "submit_token": "VRAMBLE_SUBMIT_TOKEN", "state_file": "VRAMBLE_STATE_FILE",
    "socket": "VRAMBLE_SOCKET",
    "swap_url": "VRAMBLE_SWAP_URL", "comfy_url": "VRAMBLE_COMFY_URL", "acestep_url": "VRAMBLE_ACESTEP_URL",
    "timeout_llm": "VRAMBLE_TIMEOUT_LLM", "timeout_drain": "VRAMBLE_TIMEOUT_DRAIN",
    "body_max": "VRAMBLE_BODY_MAX", "history_days": "VRAMBLE_HISTORY_DAYS",
    "jobs_kept": "VRAMBLE_JOBS_KEPT",
}

def _config_path() -> str | None:
    candidates = [os.environ.get("VRAMBLE_CONFIG"),
                 os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml"),
                 os.path.expanduser("~/.config/vramble/config.yaml")]
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return None


def load() -> dict:
    c = dict(DEFAULT)
    f = _config_path()
    if f and yaml:
        with open(f) as fh:
            c.update({k: v for k, v in (yaml.safe_load(fh) or {}).items() if k in DEFAULT})
    for key, env in _ENV.items():
        value = os.environ.get(env)
        if value:
            c[key] = type(DEFAULT[key])(value) if isinstance(DEFAULT[key], int) else value
    return c


C = load()


def base(*parts) -> str:
    return os.path.join(C["base"], *parts)


def registry() -> str:
    return os.environ.get("VRAMBLE_REGISTRY") or base("activities.yaml")


def catalog() -> str:
    return os.environ.get("VRAMBLE_CATALOG") or base("services.yaml")


def database() -> str:
    return os.environ.get("VRAMBLE_DB") or base("jobs.db")


def url() -> str:
    """vramble URL for the clients (CLI, bot)."""
    return os.environ.get("VRAMBLE_URL") or f"http://{C['listen']}:{C['port']}"
