#!/usr/bin/env python3
"""Check that the service catalog points at nodes that really exist in the workflows.

This is the guard against silent failure: when a workflow is re-exported from ComfyUI's UI the node
ids change, a parameter lands in the wrong field, and the job succeeds returning something that
ignores the request. Here it shows up immediately.

Usage: check_catalog.py [services.yaml]   → exit 1 if anything is off.
"""
import json, os, sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import config                                      # noqa: E402

CAT = sys.argv[1] if len(sys.argv) > 1 else config.catalog()


def targets(s):
    for key, v in (s.get("set") or {}).items():
        for b in (v if isinstance(v, list) else [v]):
            yield key, b


KEYS = {"description", "time_hint", "activity", "activity_if", "command", "required",
        "positional", "flags", "limits", "set", "note"}


def known_activities():
    """The registry may be missing (a catalog can be checked on its own): then we skip this check."""
    try:
        reg = yaml.safe_load(open(config.registry())) or {}
    except Exception:
        return None
    return set((reg.get("activities") or {}).keys()) or None


def main():
    data = yaml.safe_load(open(CAT)) or {}
    macros = data.get("macros") or {}
    problems, checked = [], 0
    activities = known_activities()
    for name, s in (data.get("services") or {}).items():
        # A misspelled key does not raise: the service silently falls back to the default activity.
        for k in s:
            if k not in KEYS:
                problems.append(f"{name}: unknown key '{k}' (known: {', '.join(sorted(KEYS))})")
        # An activity that is not in the registry does not raise either: the service silently ends up
        # on the default one, with somebody else's priority, ttl and drain.
        if activities:
            wanted = [s.get("activity", "comfy")]
            for mapping in (s.get("activity_if") or {}).values():
                wanted += list((mapping or {}).values())
            for a in wanted:
                if a not in activities:
                    problems.append(f"{name}: activity '{a}' is not in the registry "
                                    f"({', '.join(sorted(activities))})")
        # Literal replacement, like catalog.py and the registry: `str.format` reads the JSON braces
        # in a command as placeholders and dies — in the guard that exists to catch exactly that.
        cmd = []
        for piece in s.get("command", []):
            piece = str(piece)
            for key, value in macros.items():
                piece = piece.replace("{" + key + "}", str(value))
            cmd.append(piece)
        wf = next((c for c in cmd if c.endswith(".json")), None)
        if not wf:
            continue                      # service with no workflow (music, voice, llm…)
        if not os.path.exists(wf):
            problems.append(f"{name}: workflow missing → {wf}")
            continue
        try:
            graph = json.load(open(wf))
        except Exception as e:
            problems.append(f"{name}: workflow unreadable ({e})")
            continue
        for key, b in targets(s):
            checked += 1
            node, _, inp = str(b).partition(".")
            if node not in graph:
                problems.append(f"{name}: node {node} (param --{key}) does not exist in {os.path.basename(wf)}")
            elif inp and inp.split(".")[0] not in (graph[node].get("inputs") or {}):
                problems.append(f"{name}: node {node} has no input '{inp}' (parameter --{key}) "
                                f"— it has: {', '.join(list((graph[node].get('inputs') or {}))[:8])}")
    for p in problems:
        print("✗", p)
    print(f"{'✗' if problems else '✓'} catalog: {checked} references checked, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
