#!/usr/bin/env python3
"""Service catalog: from "I want a video with this prompt" to the command to run.

The catalog lives in services.yaml (hot-reloaded). Each service declares the vramble activity it
belongs to (for VRAM arbitration) and how params map: `set` for ComfyUI node inputs
(--set node.input=value), `flags` for script options.
"""
import os, random, time

import yaml

import config

CATALOG = config.catalog()


class Catalog:
    def __init__(self):
        self.data = {}
        self.mtime = 0
        self.load()

    def load(self):
        try:
            m = os.path.getmtime(CATALOG)
            if m != self.mtime:
                self.data = yaml.safe_load(open(CATALOG)) or {}
                self.mtime = m
        except Exception:
            if not self.data:
                raise

    def catalog_listing(self):
        self.load()
        out = {}
        for name, s in (self.data.get("services") or {}).items():
            par = list((s.get("set") or {}).keys()) + list((s.get("flags") or {}).keys())
            out[name] = {"description": s.get("description", ""), "time_hint": s.get("time_hint", ""),
                           "activity": s.get("activity", "comfy"),
                           "required": s.get("required", []),
                           "params": par, "note": s.get("note", "")}
        return out

    def build(self, service, params):
        """→ (activity, argv, note). Raises ValueError with a readable message when something is off."""
        self.load()
        s = (self.data.get("services") or {}).get(service)
        if not s:
            raise ValueError(f"unknown service: {service}. Available: "
                             + ", ".join(sorted((self.data.get('services') or {}))))
        macros = dict(self.data.get("macros") or {})
        set_map, flag_map = s.get("set") or {}, s.get("flags") or {}
        positionals = s.get("positional") or []
        known = set(set_map) | set(flag_map) | set(positionals) | {"tag", "seed"}
        unknown = [k for k in params if k not in known]
        if unknown:
            raise ValueError(f"{service}: unknown params {unknown}. It accepts: "
                             + ", ".join(sorted(known)))
        limits = s.get("limits") or {}
        for k, (lo, hi) in limits.items():
            if params.get(k) is not None:
                try:
                    v = float(params[k])
                except (TypeError, ValueError):
                    raise ValueError(f"{service}: {k} must be a number")
                if not (lo <= v <= hi):
                    raise ValueError(f"{service}: {k} must be between {lo} and {hi} (you asked {params[k]})")
        missing = [k for k in s.get("required", []) if params.get(k) in (None, "")]
        if missing:
            raise ValueError(f"{service}: missing {', '.join(missing)}"
                             + (f" ({s['note']})" if s.get("note") else ""))

        p = dict(params)
        if "seed" in set_map and not p.get("seed"):
            p["seed"] = random.randint(1, 10**6)
        tag = str(p.pop("tag", "") or f"{service}-{int(time.time()) % 100000}")

        # Literal replacement, not `str.format`: the registry learned this the hard way — a command
        # containing JSON braces (`-d '{"clear": true}'`) is read as a placeholder by format and the
        # whole command breaks. Same trap, same fix, on this side too.
        argv = []
        for piece in s["command"]:
            piece = str(piece)
            for key, value in macros.items():
                piece = piece.replace("{" + key + "}", str(value))
            argv.append(piece)
        if any("run_prompt.py" in x for x in argv):
            argv += ["--tag", tag]
        pairs = []
        for k, v in p.items():
            if k in set_map:
                targets = set_map[k]
                for b in (targets if isinstance(targets, list) else [targets]):
                    pairs.append(f"{b}={v}")
            elif k in flag_map:
                argv += [flag_map[k], str(v)]
        if pairs:
            argv += ["--set"] + pairs
        for k in positionals:                      # positional args, in declared order
            if p.get(k) is not None:
                argv.append(str(p[k]))
        # the note shown in the queue: the first textual parameter the service declares
        note = str(next((p[k] for k in s.get("required", []) if p.get(k)), None) or tag)[:60]
        activity = s.get("activity", "comfy")
        for par, mapping in (s.get("activity_if") or {}).items():   # e.g. voice with kokoro engine → cpu
            chosen = str(p.get(par) or "")
            if chosen in mapping:
                activity = mapping[chosen]
        return activity, argv, f"{service}: {note}"
