"""vramble — the arbiter of the GPUs on this machine.

One machine, one resource: with models that take 15-16 GB out of 16.3 per card, two serious
workloads never coexist. So vramble keeps a single lease, starts the environments only when they are
needed, shuts them down once they go idle, and tells whoever arrives second who is holding the
machine and for how long.

HTTP API (127.0.0.1:8099 by default, JSON):
  POST /lease     {activity, note?, ttl?, wait?, preempt?}  -> 200 {token,…} | 409 {holder,…}
  POST /heartbeat {token}                                   -> 200 | 410 (revoked)
  POST /release   {token, drain?}                           -> 200
  POST /preempt   {reason?}                                 -> 200 | 409 (holder cannot yield)
  POST /drain     {hard?}                                   -> 200 (empty the cards)
  POST /api/jobs  {kind, note, who, argv, ttl, prio}        -> queued job (needs X-Vramble-Submit)
  POST /api/requests {service, <params>}                    -> queued job, built from the catalog
  GET  /api/jobs[/<id>] · GET /api/services · GET /status[?text=1]
  POST /v1/…      OpenAI proxy; headers X-Wait, X-Ttl, X-Prio, X-Requester

Paths, ports and endpoints live in config.yaml (see config.example.yaml), never in the code.
"""
import json, os, pwd, re, socket, socketserver, struct, subprocess, sys, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import urllib.error
import urllib.request

try:
    import yaml
except ImportError:      # a crash loop under Restart=always is a bad way to say "pip install pyyaml"
    sys.exit("vramble needs pyyaml: pip install pyyaml")

import jobs as _jobs
import catalog as _catalog

import config

REGISTRY = config.registry()
PORT = config.C["port"]
LISTEN = config.C["listen"]
STATE_FILE = config.C["state_file"]
SOCKET_PATH = config.C["socket"]
SO_PEERCRED = getattr(socket, "SO_PEERCRED", 17)      # 17 on Linux; the call simply fails elsewhere
SAVE_LEASE = None      # set at start(): the arbiter persists the holder through the queue's database


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


PLACEHOLDERS = {"swap_url": config.C["swap_url"], "comfy_url": config.C["comfy_url"],
              "acestep_url": config.C["acestep_url"], "base": config.C["base"]}


def resolve(cmd: str) -> str:
    """Expand {swap_url}, {comfy_url}, {acestep_url}, {base} inside a registry command.

    Literal replacement, NOT `str.format`: commands contain JSON braces
    (`-d '{"unload_models":true}'`) which format would read as placeholders, silently breaking the
    whole command — and a drain that fails silently means two models on the same card.
    """
    if not cmd or "{" not in cmd:
        return cmd
    for key, value in PLACEHOLDERS.items():
        cmd = cmd.replace("{" + key + "}", str(value))
    return cmd


def sh(cmd, timeout=120):
    if not cmd:
        return True, ""
    try:
        p = subprocess.run(["bash", "-lc", cmd], capture_output=True, text=True, timeout=timeout)
        return p.returncode == 0, (p.stdout + p.stderr)[-400:]
    except subprocess.TimeoutExpired:
        return False, "timeout"


_vram_cache = {"t": 0.0, "v": []}
_procs_cache = {"t": 0.0, "v": []}


def vram_by_process():
    """Who is actually holding the VRAM: [(pid, mib, command)]. Without this we know that 9 GB are
    gone but not to whom, and freeing leftovers means draining every activity blindly.
    Cached like vram(): nvidia-smi can hang, and this is read on the status path."""
    if time.time() - _procs_cache["t"] < 2.0:
        return _procs_cache["v"]
    ok, out = sh("nvidia-smi --query-compute-apps=pid,used_gpu_memory --format=csv,noheader,nounits", 15)
    if not ok:
        _procs_cache.update(t=time.time(), v=[])
        return []
    found = []
    for row in out.strip().splitlines():
        bits = [x.strip() for x in row.split(",")]
        if len(bits) < 2 or not bits[0].isdigit():
            continue
        try:
            cmd = open(f"/proc/{bits[0]}/cmdline", "rb").read().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            cmd = ""
        try:
            found.append((int(bits[0]), int(bits[1]), cmd.strip()))
        except ValueError:
            continue
    _procs_cache.update(t=time.time(), v=found)
    return found


_total_cache = {"v": []}


def vram_total():
    """Capacity per GPU. Asked once: it does not change while the daemon runs."""
    if not _total_cache["v"]:
        ok, out = sh("nvidia-smi --query-gpu=memory.total --format=csv,noheader,nounits", 15)
        if ok:
            try:
                _total_cache["v"] = [int(r.strip()) for r in out.strip().splitlines() if r.strip()]
            except ValueError:
                _total_cache["v"] = []
    return _total_cache["v"]


def vram():
    """Per-GPU VRAM, cached for 2 s: nvidia-smi can hang and must not block the leases."""
    if time.time() - _vram_cache["t"] < 2.0:
        return _vram_cache["v"]
    ok, out = sh("nvidia-smi --query-gpu=index,memory.used --format=csv,noheader,nounits", 15)
    v = []
    if ok:
        try:
            v = [int(r.split(",")[1]) for r in out.strip().splitlines()]
        except Exception:
            v = []
    _vram_cache.update(t=time.time(), v=v)
    return v


class Arbiter:
    def __init__(self):
        self.lock = threading.RLock()
        self.handover_lock = threading.Lock()   # serialises drain/start: they are mutually exclusive
        self.reg = {}
        self.reg_mtime = 0
        self.holder = None      # {activity, token, note, da, expires, ttl, refs}
        self.handover = None    # activity we are handing the machine to (drain/start under way)
        self.waiting = {}             # wait_id -> {activity, prio, ts, seen}
        self._cooldown = {}           # activity -> monotonic time before which draining is pointless
        self.preempt_asked = None
        self.load_registry()

    # ---------------------------------------------------------------- registry
    def load_registry(self):
        try:
            m = os.path.getmtime(REGISTRY)
            if m == self.reg_mtime:
                return
            with open(REGISTRY) as f:
                data = yaml.safe_load(f) or {}
            if not data.get("activities"):
                raise ValueError("registry has no activities")
            self.reg = data
            self.reg_mtime = m
        except Exception as e:
            if not self.reg:
                raise
            log(f"registry unreadable: {e}")

    def activity(self, name):
        self.load_registry()
        a = self.reg.get("activities", {}).get(name)
        if a is None:
            raise KeyError(name)
        return a

    def requested_vram(self, a, note=""):
        """VRAM per GPU for this request: the registry can declare it per model (the note)."""
        by_note = a.get("vram_gb_by_note") or {}
        return float(by_note.get((note or "").strip(), a.get("vram_gb", 8)))

    def glob(self, k, d=None):
        return self.reg.get("global", {}).get(k, d)

    # --------------------------------------------------------------- environments
    def start_env(self, name):
        a = self.activity(name)
        hc = resolve(a.get("healthcheck"))
        if hc and sh(hc, 10)[0]:
            return True, "already up"
        if a.get("start"):
            ok, out = sh(resolve(a["start"]), 60)
            if not ok:
                return False, f"start failed: {out}"
        if hc:
            cutoff = time.monotonic() + float(a.get("healthcheck_s", 60))
            while time.monotonic() < cutoff:
                if sh(hc, 10)[0]:
                    return True, "up"
                time.sleep(1)
            return False, "healthcheck never passed"
        return True, "no environment to start"

    def drain(self, name, hard, forced=False):
        """Give back the VRAM held by an activity. hard=True also stops the service.
        Returns True only when the VRAM actually moved: a drain that reports success without
        freeing anything is how two models end up on one card."""
        try:
            a = self.activity(name)
        except KeyError:
            return
        cmd = a.get("drain_hard") if hard else a.get("drain_soft")
        if not cmd and hard:
            cmd = a.get("drain_soft")
        # A service that reloads the moment it is released would otherwise be drained on every
        # 5 s tick forever. An explicit preempt ignores the cooldown: that one is a person asking.
        quiet_until = self._cooldown.get(name, 0.0)
        if cmd and not forced and time.monotonic() < quiet_until:
            return False
        if cmd:
            _t = time.monotonic()
            before = sum(vram())
            ok, out = sh(resolve(cmd), float(config.C["timeout_drain"]))
            # An exit code is not evidence: ComfyUI's /free answers 200 while a job is running and
            # only sets a flag its executor reads between prompts, so the VRAM does not move. Measure.
            _vram_cache["t"] = 0.0
            freed = before - sum(vram())
            outcome = ("ok" if ok else "FAILED: " + (out.strip() or "no output"))
            if ok and freed < 64:
                outcome = "nothing freed (already idle, or still busy)"
                self._cooldown[name] = time.monotonic() + self.COOLDOWN_S
            elif ok:
                outcome = f"ok, freed {freed} MiB"
                self._cooldown.pop(name, None)
            log(f"drain {name} ({'hard' if hard else 'soft'}): {outcome} [{time.monotonic() - _t:.1f}s]")
            return ok and freed >= 64

    def vram_holders(self):
        """{activity: MiB} for the processes we can attribute, plus "?" for the rest. The rule lives
        in the registry (`match`, a regular expression on the command line), not in the code."""
        out, unknown = {}, 0
        rules = [(name, a.get("match")) for name, a in self.reg.get("activities", {}).items() if a.get("match")]
        for _pid, mib, cmd in vram_by_process():
            for name, pattern in rules:
                try:
                    if re.search(pattern, cmd):
                        out[name] = out.get(name, 0) + mib
                        break
                except re.error:
                    continue
            else:
                unknown += mib
        if unknown:
            out["?"] = unknown
        return out

    BLIND_RESTORE_S = 300   # a lease restored on a guess lives 5 minutes unless somebody beats it
    COOLDOWN_S = 60      # after a drain that freed nothing, leave that activity alone for a while

    def free_leftovers(self, apart_from, requested_vram):
        """Before granting: clear out whoever holds VRAM without holding the lease.
        The lease holder is never touched: draining it would cut off work in progress.
        These drains are `forced`: the cooldown exists to stop the idle watchdog from nagging a
        service that reloads itself, not to block the one drain that has to happen before a grant."""
        hard = float(requested_vram) >= float(self.glob("whole_card_threshold_gb", 15.0))
        with self.lock:
            holder_a = self.holder["activity"] if self.holder else None
        for name in self.reg.get("activities", {}):
            if name == apart_from or name == holder_a:
                continue
            self.drain(name, hard, forced=True)

    # -------------------------------------------------------------------- lease
    @staticmethod
    def _since(since):
        """`since` arrives as wall clock (the job's `created`); the line works on monotonic."""
        if since is None:
            return time.monotonic()
        return time.monotonic() - max(0.0, time.time() - float(since))

    def _expired(self):
        o = self.holder
        return o is not None and time.monotonic() > o["expires"]

    def _revoke_if_expired(self):
        if self._expired():
            o = self.holder
            log(f"lease expired: {o['activity']} ({o['note']})")
            self.holder = None
            self.preempt_asked = None

    def vram_readable(self):
        """Whether we can measure the cards at all. A separate method so the tests can say."""
        return bool(vram())

    def _holder_view(self):
        """The holder as the 409 bodies show it. Deliberately NOT state(): that one reads the VRAM,
        and these call sites already hold the lock — shelling out to nvidia-smi under it is the
        freeze this daemon has been bitten by twice."""
        o = self.holder
        if not o:
            return None
        return {"activity": o["activity"], "note": o["note"], "leftover": o.get("leftover"),
                "held_s": int(time.monotonic() - o["since"]),
                "expires_in_s": int(o["expires"] - time.monotonic()),
                "refs": o.get("refs", 1),
                "preemptible": bool(self.activity(o["activity"]).get("preemptible")),
                "preempt_asked": self.preempt_asked}

    def snapshot(self):
        """The holder in wall-clock terms, so it still means something after a restart."""
        o = self.holder
        if not o:
            return None
        return {"activity": o["activity"], "note": o["note"], "token": o["token"],
                "tokens": o.get("tokens", {}), "refs": o.get("refs", 1), "ttl": o["ttl"], "leftover": o.get("leftover"),
                "expires_at": time.time() + max(0.0, o["expires"] - time.monotonic()),
                "seq": o.get("seq", 0)}

    def persist(self):
        if SAVE_LEASE:
            SAVE_LEASE(self.snapshot())

    def restore(self, saved):
        """At startup: a saved lease is only worth restoring if the machine agrees — the activity
        must still be holding VRAM. Otherwise the holder died with the daemon and the card is free."""
        if not saved:
            return
        left = float(saved.get("expires_at", 0)) - time.time()
        if left <= 0:
            log(f"saved lease of {saved.get('activity')} had already expired")
            return
        holders = self.vram_holders()
        held = holders.get(saved.get("activity"), 0)
        if held < 64:
            if not self.vram_readable():
                # No nvidia-smi (another vendor, a laptop): we cannot check, and "cannot check" is
                # not evidence that nobody is there. Trust the saved deadline rather than drop the
                # lease at every restart.
                log(f"lease restored on trust: {saved.get('activity')} ({saved.get('note')}), "
                    f"the VRAM is not readable here")
            else:
                # An activity with no `match` rule cannot be recognised on the card. Rather than
                # drop the lease of a long run (an agent, a training) on every restart, accept
                # unattributed VRAM as proof that somebody is still there — and say it is a guess.
                blind = not (self.reg.get("activities", {}).get(saved.get("activity"), {}) or {}).get("match")
                if blind and holders.get("?", 0) >= 64:
                    # An assumption does not deserve the full remaining TTL: a research lease would
                    # hold the machine for twelve hours on the strength of somebody's browser. Give
                    # it a short leash — whoever is really there will renew it with a heartbeat.
                    left = min(left, self.BLIND_RESTORE_S)
                    log(f"saved lease of {saved.get('activity')} restored on unattributed VRAM "
                        f"({holders['?']} MiB) for {int(left)}s only: it declares no `match`, so "
                        f"this is an assumption — a heartbeat will extend it")
                else:
                    log(f"saved lease of {saved.get('activity')} dropped: it holds no VRAM any more")
                    return
        with self.lock:
            self.holder = {"activity": saved["activity"], "token": saved["token"],
                           "note": saved.get("note", ""), "since": time.monotonic(),
                           "expires": time.monotonic() + left, "ttl": float(saved.get("ttl", left)),
                           "tokens": saved.get("tokens") or {saved["token"]: saved["activity"]},
                           "refs": saved.get("refs", 1), "seq": saved.get("seq", 0),
                           "leftover": saved.get("leftover")}
        log(f"lease restored: {saved['activity']} ({saved.get('note')}) holds {held} MiB, "
            f"{int(left)}s left")

    def state(self):
        holders = self.vram_holders()      # before the lock: it may shell out to nvidia-smi
        with self.lock:
            self._revoke_if_expired()
            o = self.holder
            waiting = sorted(
                [c for c in self.waiting.values() if time.monotonic() - c["seen"] < 20],
                key=lambda c: (-c["prio"], c["ts"]),
            )
            return {
                "holder": None if not o else {
                    "activity": o["activity"], "note": o["note"], "leftover": o.get("leftover"),
                    "held_s": int(time.monotonic() - o["since"]),
                    "expires_in_s": int(o["expires"] - time.monotonic()), "refs": o["refs"],
                    "preemptible": bool(self.activity(o["activity"]).get("preemptible")),
                    "preempt_asked": self.preempt_asked,
                },
                "queue": [{"activity": c["activity"], "waiting_s": int(time.monotonic() - c["ts"])} for c in waiting],
                "vram_mib": vram(),
                "registry": REGISTRY, "vram_by_activity": holders,
            }

    def acquire(self, name, note="", ttl=None, wait=0, preempt=False, wait_id=None, internal=True,
                since=None):
        # `since`: how long this request has really been waiting. A queued job asks again every ~18 s,
        # and its slot in the line expires after 20 s of silence: taking the timestamp of the *attempt*
        # made it lose its seniority to anyone who happened to be polling without interruption. With
        # the job's own `created` the seniority no longer depends on the retry rhythm.
        # `internal` defaults to True because leases taken through the CLI ARE subprocesses of work
        # already running (llama-server inside an agent run, run_prompt inside a job): with False the
        # agent could no longer use the model during its own run. New requests come from the queue,
        # and the worker passes internal=False explicitly.
        """Decide under the lock; drain and start OUTSIDE it. A dying process must be able to call
        /release: holding the lock kept it hanging until its launcher killed it — 30 s wasted."""
        with self.lock:
            self._revoke_if_expired()
            a = self.activity(name)
            preempted = None
            if self.handover and self.handover != name:
                return 409, {"error": "handover in progress", "holder": None,
                             "default_wait": float(a.get("default_wait", 0) or 0)}
            prio = int(a.get("priority", 50))
            ttl = float(ttl or a.get("default_ttl", 1800))
            o = self.holder

            # reentrancy: the same activity still at work. Its OWN token here too: handing out the
            # owner's token means the first `release` would dissolve everybody's lease.
            if o and o["activity"] == name:
                o["seq"] = o.get("seq", 0) + 1
                tok = f"{o['token']}#{o['seq']}"
                o["tokens"][tok] = name
                o["refs"] = len(o["tokens"])
                o["expires"] = max(o["expires"], time.monotonic() + ttl)
                return 200, {"token": tok, "activity": name, "reentrant": True,
                             "expires_in_s": int(o["expires"] - time.monotonic())}

            if o:
                holder_a = self.activity(o["activity"])
                # compatible activities: the holder declares this work as part of its own (e.g. the
                # model calls inside an agent run) ⇒ it goes through without draining anything
                # `internal`: a subprocess of the work in progress (llama-server inside an agent run),
                # not a new request. New requests go through the queue and wait their turn.
                if internal and name in (holder_a.get("compatible_with") or []):
                    o["seq"] = o.get("seq", 0) + 1
                    tok = f"{o['token']}#{o['seq']}"
                    o["tokens"][tok] = name          # when the owner leaves, the lease becomes theirs
                    o["refs"] = len(o["tokens"])
                    return 200, {"token": tok, "activity": o["activity"], "compatible": True,
                                 "expires_in_s": int(o["expires"] - time.monotonic())}
                holder_prio = int(holder_a.get("priority", 50))
                yields = bool(holder_a.get("preemptible")) and prio >= holder_prio
                if preempt:
                    yields = True
                if not yields:
                    if wait and wait_id:
                        c = self.waiting.setdefault(wait_id, {"activity": name, "prio": prio,
                                                              "ts": self._since(since)})
                        c["ts"] = min(c["ts"], self._since(since))
                        c["seen"] = time.monotonic()
                    return 409, {"error": "busy", "holder": self._holder_view(),
                                 "queued": bool(wait and wait_id),
                                 "default_wait": float(a.get("default_wait", 0) or 0),
                                 "hint": f'gpu-lease preempt "{name} needs the machine"'}
                log(f"{name} takes over from {o['activity']}"
                      f"{' (preempted)' if preempt else ''}")
                preempted = o["activity"]
                self.holder = None
                self.handover = name

            # respect the queue: only whoever is ahead goes through
            if wait and wait_id:
                ahead = [c for c in self.waiting.values()
                           if time.monotonic() - c["seen"] < 20 and c is not self.waiting.get(wait_id)
                           and (c["prio"] > prio or (c["prio"] == prio and c["ts"] < self.waiting.get(wait_id, {"ts": time.monotonic()})["ts"]))]
                if ahead:
                    c = self.waiting.setdefault(wait_id, {"activity": name, "prio": prio,
                                                          "ts": self._since(since)})
                    c["ts"] = min(c["ts"], self._since(since))
                    c["seen"] = time.monotonic()
                    return 409, {"error": "queued", "position": len(ahead) + 1,
                                 "holder": self._holder_view(), "queued": True,
                                 "default_wait": float(a.get("default_wait", 0) or 0)}

            self.handover = name

        # --- outside the lock: drain and start are slow and must let release/heartbeat through
        with self.handover_lock:
          try:
            _t = time.monotonic()      # monotonic here and below: mixing the two clocks printed
            if preempted:                  # nonsense like "preemption of llm: -1789376249.6s"
                self.drain(preempted, hard=self.requested_vram(a, note) >= float(self.glob("whole_card_threshold_gb", 15.0)),
                           forced=preempt)
                log(f"  preemption of {preempted}: {time.monotonic() - _t:.1f}s")
            _t1 = time.monotonic()
            wanted = self.requested_vram(a, note)
            self.free_leftovers(apart_from=name, requested_vram=wanted)
            # The drains reported what they freed; now check the card agrees. This runs for every
            # grant, not only for the ones that want the whole card: a 6 GB request landing on 15 GB
            # that were never freed is the same OOM. Unattributed VRAM counts as occupied — being
            # unable to say whose it is does not make it free.
            _procs_cache["t"] = 0.0
            _vram_cache["t"] = 0.0
            capacity, used = sum(vram_total()), sum(vram())
            if capacity:                      # unmeasurable machine: no check to do, and no veto
                mine = self.vram_holders().get(name, 0)
                free_for_us = capacity - used + mine
                wanted_mib = int(float(wanted) * 1024)
                if free_for_us < wanted_mib:
                    with self.lock:
                        if self.handover == name:
                            self.handover = None
                    holders = {k: v for k, v in self.vram_holders().items() if k != name}
                    log(f"{name} not granted: {free_for_us} MiB free, it needs {wanted_mib} MiB "
                        f"· still there: {holders or 'nothing we can attribute'}")
                    return 409, {"error": "could not free enough VRAM", "free_mib": free_for_us,
                                 "needed_mib": wanted_mib, "still_holding": holders,
                                 "hint": "stop it by hand, or check that activity's drain command"}
            _t2 = time.monotonic()
            ok, msg = self.start_env(name)
            log(f"  leftovers: {_t2 - _t1:.1f}s · start {name}: {time.monotonic() - _t2:.1f}s")
          finally:
            with self.lock:
                if self.handover == name:
                    self.handover = None
        with self.lock:
            if not ok:
                return 503, {"error": "environment did not start", "detail": msg}
            if self.holder:      # somebody slipped in while we were starting
                return 409, {"error": "busy", "holder": self._holder_view(),
                             "default_wait": float(a.get("default_wait", 0) or 0)}

            token = f"{name}-{int(time.time()*1000)%10**9}"
            self.holder = {"activity": name, "token": token, "note": note, "since": time.monotonic(),
                              "expires": time.monotonic() + ttl, "ttl": ttl, "refs": 1,
                              "tokens": {token: name}}
            self.preempt_asked = None
            self.waiting.pop(wait_id, None)
            log(f"lease → {name} ({note}) ttl={int(ttl)}s")
            self.persist()
            return 200, {"token": token, "activity": name, "expires_in_s": int(ttl),
                         "heartbeat_s": self.glob("heartbeat_s", 30), "environment": msg}

    def heartbeat(self, token):
        with self.lock:
            o = self.holder
            if not o or token not in o.get("tokens", {}):
                return 410, {"error": "lease revoked"}
            o["expires"] = time.monotonic() + o["ttl"]
            return 200, {"ok": True, "preempt_asked": self.preempt_asked,
                         "expires_in_s": int(o["ttl"])}

    def release_lease(self, token, drain=False):
        with self.lock:
            o = self.holder
            if not o or token not in o.get("tokens", {}):
                return 200, {"ok": True, "note": "no lease to release"}
            main_token = (token == o["token"])
            o["tokens"].pop(token, None)
            o["refs"] = len(o["tokens"])
            if o["refs"] > 0:
                if main_token:
                    # the owner finished but something of theirs is still up (the model loaded
                    # by an agent run): the lease passes to that activity, usually a preemptible one
                    new_owner = next(iter(o["tokens"]))
                    log(f"lease: {o['activity']} finished → passes to {o['tokens'][new_owner]} (leftover)")
                    # A flag, not a rewritten note: the note is data, and wrapping it every time
                    # the lease degrades again produced "leftover of (leftover of (leftover of …))".
                    o.setdefault("leftover", o["activity"])
                    o["activity"] = o["tokens"][new_owner]
                    o["token"] = new_owner
                    o["ttl"] = float(self.activity(o["activity"]).get("default_ttl", 1800))
                    o["expires"] = time.monotonic() + o["ttl"]
                return 200, {"ok": True, "refs": o["refs"]}
            name = o["activity"]
            self.holder = None
            self.preempt_asked = None
            log(f"lease released by {name}")
        self.persist()                      # outside the lock: it is a disk write
        if drain:
            # Outside the lock, like every other drain: holding it here froze every lease,
            # heartbeat and status call for as long as the drain command took (up to timeout_drain).
            with self.handover_lock:
                self.drain(name, hard=False)
        return 200, {"ok": True}

    def preempt(self, reason=""):
        with self.lock:
            o = self.holder
            if not o:
                return 200, {"ok": True, "note": "nobody to preempt"}
            a = self.activity(o["activity"])
            if not (a.get("drain_soft") or a.get("drain_hard")):
                # nothing to run to make it yield: clearing the holder would be a lie
                # (the process still holds the VRAM) and would let a second workload in.
                return 409, {"error": "cannot make it yield",
                             "holder": self._holder_view(),
                             "note": f"{o['activity']} declares no way to free the VRAM: "
                                     "stop the process by hand (systemctl --user stop <unit>, or kill), "
                                     "then the lease frees itself"}
            self.preempt_asked = {"when": time.time(), "reason": reason}
            name, tok = o["activity"], o["token"]
        # outside the lock: draining can be slow
        with self.handover_lock:
            self.drain(name, hard=True, forced=True)
        with self.lock:
            if self.holder and self.holder["token"] == tok:
                self.holder = None
        log(f"forced preemption of {name}: {reason}")
        return 200, {"ok": True, "preempted": name}

    def drain_all(self, hard=True):
        """Empty the cards: used by hand before a long run. Does not touch the lease holder."""
        with self.lock:
            self._revoke_if_expired()
            o = self.holder
            if o:
                # a preemptible holder (a loaded LLM, ComfyUI between two jobs) is not an obstacle
                if not self.activity(o["activity"]).get("preemptible"):
                    return 409, {"error": "busy", "holder": self._holder_view(),
                                 "hint": "gpu-lease preempt"}
                self.holder = None
            names = list(self.reg.get("activities", {}))
            self.handover = "__drain__"
        try:
            with self.handover_lock:
                for n in names:
                    self.drain(n, hard, forced=True)
        finally:
            with self.lock:
                if self.handover == "__drain__":
                    self.handover = None
        return 200, {"ok": True, "vram_mib": vram()}

    # ----------------------------------------------------------------- watchdog
    def loop(self):
        last_empty = time.monotonic()
        drained_set = set()
        while True:
            time.sleep(5)
            try:
                with self.lock:
                    self._revoke_if_expired()
                    busy = self.holder is not None
                    for k, c in list(self.waiting.items()):
                        if time.monotonic() - c["seen"] > 20:
                            self.waiting.pop(k, None)
                if busy:
                    last_empty = time.monotonic()
                    drained_set.clear()
                else:
                    idle_for = time.monotonic() - last_empty
                    for name, a in self.reg.get("activities", {}).items():
                        t = float(a.get("idle_ttl", 0) or 0)
                        if t and idle_for > t and name not in drained_set:
                            log(f"{name}: idle for {int(idle_for)}s → freeing the VRAM")
                            # handover_lock like every other drain: without it the watchdog can
                            # unload a service that acquire() is starting right now for somebody.
                            with self.handover_lock:
                                if self.holder is None:      # re-check: a grant may have landed
                                    self.drain(name, hard=False)
                                    drained_set.add(name)
                self.persist()
                if STATE_FILE:
                    try:
                        with open(STATE_FILE, "w") as f:
                            json.dump(self.state(), f)
                    except Exception:
                        pass
            except Exception as e:
                log(f"watchdog: {e}")


# Created at startup (see `start`), not at import: so the module can be imported in tests
# without touching the registry, the database or the GPU.
ARB = None
QUEUE = None
CATALOG = None


def start():
    global ARB, QUEUE, CATALOG, SAVE_LEASE
    ARB = Arbiter()
    QUEUE = _jobs.Queue(ARB, log)
    CATALOG = _catalog.Catalog()
    SAVE_LEASE = QUEUE.save_lease
    try:
        ARB.restore(QUEUE.load_lease())  # a restart must not make a held machine look free
    except Exception as e:               # never let a bad row keep the daemon from starting
        log(f"saved lease not restored: {e}")
    return ARB, QUEUE, CATALOG
SWAP_URL = config.C["swap_url"]
TOKEN = config.C["token"]          # when set, POSTs must carry X-Vramble
SUBMIT_TOKEN = config.C["submit_token"]   # /api/jobs runs an arbitrary argv: closed unless set


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _reply(self, code, obj):
        body = (obj if isinstance(obj, str) else json.dumps(obj, ensure_ascii=False)).encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8" if isinstance(obj, str) else "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _proxy_llm(self, path, body):
        """An OpenAI request goes through the queue: it gets an id, waits its turn and is then
        forwarded to llama-swap. If the turn does not come in time, the caller gets back its place in the queue."""
        try:
            d = json.loads(body or b"{}")
        except Exception:
            d = {}
        model = d.get("model") or "?"
        who = self.headers.get("X-Requester") or self.client_address[0]
        try:
            raw_wait = self.headers.get("X-Wait")
            wait = None if raw_wait is None else float(raw_wait)
            ttl_job = float(self.headers.get("X-Ttl") or 900)
            if self.headers.get("X-Prio"):
                int(self.headers["X-Prio"])          # validated here: an absurd value is a 400, not a 500
        except ValueError:
            return self._reply(400, {"error": "X-Wait/X-Ttl must be numbers"})
        try:
            prio = int(self.headers["X-Prio"]) if self.headers.get("X-Prio") else None
            j = QUEUE.add("llm", note=model, who=who, attended=True, ttl=ttl_job, prio=prio)
            cutoff = wait if wait is not None else float(ARB.activity("llm").get("default_wait") or 90)
        except (KeyError, ValueError) as e:
            return self._reply(400, {"error": f"activity 'llm' is not configured: {e}"})
        jid = j["id"]
        if not QUEUE.wait_turn(jid, cutoff):
            s = QUEUE.job_state(jid) or {}
            QUEUE.give_up(jid)      # otherwise the job would keep holding its place in the queue
            return self._reply(429, {"waiting": True, "job": jid, "position": s.get("position"),
                                        "ahead": s.get("ahead"), "model": model,
                                        "message": "the machine is working on another request"})
        rc, note, sent = 0, "", False
        # Registered BEFORE the upstream call: between getting the turn and holding the connection
        # there is a window where a cancel would find nothing to stop, decide nothing was running,
        # and let the worker release the lease while this request was on its way to the model.
        upstream = {"conn": None, "cancelled": False}

        def stop_upstream():
            upstream["cancelled"] = True
            c = upstream["conn"]
            if c is not None:
                try:
                    c.close()
                except Exception:
                    pass

        if not QUEUE.attach_canceller(jid, stop_upstream):
            # cancelled between getting the turn and registering: do not send anything upstream
            return self._reply(409, {"error": "cancelled", "job": jid})
        try:
            if upstream["cancelled"]:        # cancelled while we were registering
                raise urllib.error.URLError("cancelled before the request was sent")
            req = urllib.request.Request(SWAP_URL + path, data=body,
                                         headers={"Content-Type": "application/json",
                                                  "Authorization": "Bearer local"}, method="POST")
            with urllib.request.urlopen(req, timeout=min(float(config.C["timeout_llm"]), max(60.0, ttl_job))) as r:
                self.send_response(r.status)
                for k in ("Content-Type", "Cache-Control"):
                    if r.headers.get(k):
                        self.send_header(k, r.headers[k])
                self.send_header("X-Job", jid)
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                sent = True
                upstream["conn"] = r          # from here the canceller can close the stream
                if upstream["cancelled"]:
                    r.close()
                read = getattr(r, "read1", r.read)   # read1: forward as soon as it arrives (SSE), do not buffer
                while True:
                    chunk = read(8192)
                    if not chunk:
                        break
                    self.wfile.write(hex(len(chunk))[2:].encode() + b"\r\n" + chunk + b"\r\n")
                    self.wfile.flush()
                self.wfile.write(b"0\r\n\r\n")
        except urllib.error.HTTPError as e:
            rc, note = e.code, e.read().decode("utf-8", "replace")[:400]
            if not sent:
                self._reply(e.code, note)
            else:
                self.close_connection = True
        except Exception as e:
            rc, note = 1, str(e)
            if sent:
                self.close_connection = True      # headers already sent: close, do not write a 502 into the body
            else:
                try:
                    self._reply(502, {"error": note, "job": jid})
                except Exception:
                    pass
        finally:
            QUEUE.finish_attended(jid, rc if isinstance(rc, int) else 1, note)

    def peer_identity(self):
        """On the unix socket the caller is whoever the kernel says: not a header they wrote.
        Resolved to a user name: a name reads better than "uid:1000" in a job listing."""
        a = self.client_address
        if not (isinstance(a, str) and a.startswith("uid:")):
            return ""
        try:
            uid = int(a.split()[0].split(":")[1])
            return pwd.getpwuid(uid).pw_name
        except Exception:
            return a

    def do_GET(self):
        if self.path.startswith("/api/services"):
            try:
                return self._reply(200, CATALOG.catalog_listing())
            except Exception as e:
                return self._reply(500, {"error": str(e)})
        if self.path.startswith("/api/jobs"):
            rest = self.path[len("/api/jobs"):].strip("/")
            if rest and "?" not in rest:
                j = QUEUE.job_state(rest)
                return self._reply(200 if j else 404, j or {"error": "unknown id"})
            q = {}
            if "?" in self.path:
                for kv in self.path.split("?", 1)[1].split("&"):
                    if "=" in kv:
                        k, v = kv.split("=", 1)
                        q[k] = v
            return self._reply(200, {"current": QUEUE.current,
                                        "job": QUEUE.listing(int(q.get("how_many", 30)), q.get("state"))})
        if self.path.startswith("/v1/") or self.path.startswith("/running") or self.path.startswith("/upstream"):
            try:
                req = urllib.request.Request(SWAP_URL + self.path, headers={"Authorization": "Bearer local"})
                with urllib.request.urlopen(req, timeout=30) as r:
                    body, state = r.read(), r.status
                self.send_response(state)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                return self.wfile.write(body)
            except urllib.error.HTTPError as e:
                return self._reply(e.code, e.read().decode("utf-8", "replace")[:400])
            except Exception as e:
                return self._reply(502, {"error": str(e)})
        if self.path.startswith("/status"):
            s = ARB.state()
            if "text=1" in self.path:
                o = s["holder"]
                line = "free" if not o else (
                    f"held by {o['activity']} ({o['note']}"
                    + (f", left over from {o['leftover']}" if o.get("leftover") else "")
                    + f") for {o['held_s']}s, "
                    f"lease valid {o['expires_in_s']}s more"
                    + (", preemptible" if o["preemptible"] else ", not preemptible"))
                # no nvidia-smi (another vendor, a laptop, a test machine): say so instead of
                # printing an empty field — vramble arbitrates all the same, it just cannot measure.
                vr = ("VRAM " + "/".join(str(v) for v in s["vram_mib"]) + " MiB"
                      if s["vram_mib"] else "VRAM not readable")
                return self._reply(200, f"machine {line} · {vr} · queued {len(s['queue'])}\n")
            s["queue_detail"] = {"current": QUEUE.current,
                         "queued": [{"id": x["id"], "kind": x["kind"], "note": x["note"]}
                                     for x in QUEUE.listing(20, "queued")]}
            return self._reply(200, s)
        self._reply(404, {"error": "not found"})

    def do_POST(self):
        # A web page open in a browser can fetch() 127.0.0.1 without a preflight: it must not be
        # able to enqueue commands. Requests from our own clients carry no Origin/Referer.
        if self.headers.get("Origin") or self.headers.get("Referer"):
            return self._reply(403, {"error": "requests from a browser are not accepted"})
        if TOKEN and self.headers.get("X-Vramble") != TOKEN:
            return self._reply(403, {"error": "missing or wrong token"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
            if n < 0:           # read(-1) would block until the client closes: a free hung thread
                raise ValueError("negative")
        except ValueError:      # a nonsense header must be a 400, not a traceback per request
            return self._reply(400, {"error": "Content-Length is not a valid length"})
        if n > config.C["body_max"]:
            return self._reply(413, {"error": "request too large"})
        if self.path.startswith("/v1/"):
            return self._proxy_llm(self.path, self.rfile.read(n))
        try:
            d = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return self._reply(400, {"error": "invalid json"})
        try:
            if self.path == "/api/requests":
                service = d.pop("service", "")
                declared = d.pop("who", "")      # always popped: it is not a service parameter
                who = (self.peer_identity() or declared or self.headers.get("X-Requester")
                       or str(self.client_address[0]))
                ttl = d.pop("ttl", None)
                prio = d.pop("prio", None)
                try:
                    activity, argv, note = CATALOG.build(service, d)
                except ValueError as e:
                    return self._reply(400, {"error": str(e)})
                j = QUEUE.add(activity, note, who, argv, False, ttl, prio)
                j["service"] = service
                j["command"] = argv
                c, r = 200, j
            elif self.path == "/api/jobs":
                # The only endpoint that runs a command chosen by the caller. Off unless a token is
                # configured: a client that runs LLM-written code must not be able to reach it.
                if not SUBMIT_TOKEN or self.headers.get("X-Vramble-Submit") != SUBMIT_TOKEN:
                    return self._reply(403, {"error": "free-argv submit is closed: set submit_token"})
                j = QUEUE.add(d["kind"], d.get("note", ""),
                                  self.peer_identity() or d.get("who", ""),
                                  d.get("argv"), bool(d.get("attended")), d.get("ttl"), d.get("prio"),
                                  may_raise=True)      # this path already required the token
                c, r = 200, j
            elif self.path.startswith("/api/jobs/") and self.path.endswith("/cancel"):
                ok, msg = QUEUE.cancel(self.path.split("/")[3])
                c, r = (200 if ok else 409), {"ok": ok, "note": msg}
            elif self.path == "/lease":
                c, r = ARB.acquire(d["activity"], d.get("note", ""), d.get("ttl"),
                                      d.get("wait", 0), bool(d.get("preempt")), d.get("wait_id"),
                                      bool(d.get("internal", True)))
            elif self.path == "/heartbeat":
                c, r = ARB.heartbeat(d.get("token", ""))
            elif self.path == "/release":
                c, r = ARB.release_lease(d.get("token", ""), bool(d.get("drain")))
            elif self.path == "/preempt":
                c, r = ARB.preempt(d.get("reason", ""))
            elif self.path == "/drain":
                c, r = ARB.drain_all(bool(d.get("hard", True)))
            else:
                c, r = 404, {"error": "not found"}
        except KeyError as e:
            c, r = 400, {"error": f"unknown activity: {e}"}
        except Exception as e:
            c, r = 500, {"error": str(e)}
        self._reply(c, r)


class UnixServer(socketserver.ThreadingUnixStreamServer):
    """Same HTTP handler, but over a unix socket: there the caller's identity comes from the kernel
    (SO_PEERCRED) instead of a header anybody can write. The TCP port stays for the OpenAI proxy
    and for clients on another machine."""
    daemon_threads = True
    allow_reuse_address = True

    def get_request(self):
        sock, addr = super().get_request()
        try:
            creds = sock.getsockopt(socket.SOL_SOCKET, SO_PEERCRED, struct.calcsize("3i"))
            pid, uid, _gid = struct.unpack("3i", creds)
            addr = f"uid:{uid} pid:{pid}"
        except Exception:
            addr = "peer"
        return sock, addr


def serve_unix(path):
    try:
        if os.path.exists(path):
            os.unlink(path)
        srv = UnixServer(path, H)
        os.chmod(path, 0o600)      # this socket can preempt and submit: keep it to its owner
        log(f"also listening on {path} (caller identity from the kernel)")
        srv.serve_forever()
    except Exception as e:
        log(f"unix socket not available ({e}): only the TCP port then")


if __name__ == "__main__":
    start()
    ok_cat, out_cat = sh(f"{sys.executable} "
                         f"{os.path.join(os.path.dirname(os.path.abspath(__file__)), 'check_catalog.py')} "
                         f"{config.catalog()}", 60)
    log(("catalog ok" if ok_cat else "WARNING, catalog has problems:\n" + out_cat.strip()))
    threading.Thread(target=ARB.loop, daemon=True).start()
    threading.Thread(target=QUEUE.loop, daemon=True).start()
    if SOCKET_PATH:
        threading.Thread(target=serve_unix, args=(SOCKET_PATH,), daemon=True).start()
    srv = ThreadingHTTPServer((LISTEN, PORT), H)
    log(f"listening on {LISTEN}:{PORT} · registry {REGISTRY}")
    srv.serve_forever()
