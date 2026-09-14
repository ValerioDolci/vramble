#!/usr/bin/env python3
"""The request queue: every request gets an id, joins the queue and runs one at a time.

Two kinds of job:
  - **batch**: they carry an `argv` to run (image, video, music, training, scripts); the worker launches them.
  - **attended**: someone holds the connection open and only wants their turn (the /v1 LLM proxy,
    where the answer must be streamed back). The worker marks them running and unblocks the waiter.

The queue is served by ONE worker: the machine is a single resource. Priority comes from the vramble
(activity); at equal priority, whoever asked first wins.
"""
import json, os, signal, sqlite3, subprocess, threading, time

import config

DB = config.database()

SCHEMA = """
CREATE TABLE IF NOT EXISTS job (
  id TEXT PRIMARY KEY, kind TEXT, note TEXT, who TEXT, prio INTEGER, state TEXT,
  attended INTEGER DEFAULT 0, argv TEXT, ttl REAL,
  created REAL, started REAL, finished REAL, rc INTEGER, output TEXT, results TEXT
);
DROP INDEX IF EXISTS i_stato;
DROP INDEX IF EXISTS i_creato;
CREATE INDEX IF NOT EXISTS i_state ON job(state, prio DESC, created);
CREATE INDEX IF NOT EXISTS i_created ON job(created DESC);
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER);
-- The lease, so a restart of the daemon does not make a held machine look free.
CREATE TABLE IF NOT EXISTS lease (only_one INTEGER PRIMARY KEY CHECK (only_one = 1), holder TEXT);
"""


class Queue:
    def __init__(self, arbiter, log):
        self.arb = arbiter
        self.log = log
        self.lock = threading.RLock()
        self.events = {}          # id -> {"turn": Event, "end": Event, "outcome": dict}
        self.current = None      # id of the job being executed
        # Cancelled while the worker already had them in hand: the launcher checks this before
        # starting the process. Closing the row is not enough — the worker holds its own copy.
        self._cancelled: set[str] = set()
        self._procs: dict[str, subprocess.Popen] = {}   # id -> process, any lane: cancel must find it
        self.stopped = False
        self._attempts: dict[str, int] = {}
        self.db = sqlite3.connect(DB, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript(SCHEMA)
        self.db.commit()
        # a daemon restart must not leave ghost jobs stuck as running
        self.db.execute("UPDATE job SET state='interrupted', finished=? WHERE state='running'", (time.time(),))
        self.db.commit()
        d = config.base("jobs")         # nor their log files
        for f in (os.listdir(d) if os.path.isdir(d) else []):
            try:
                os.unlink(os.path.join(d, f))
            except OSError:
                pass

    # ------------------------------------------------------------------ helpers
    _COLUMNS: list[str] = []

    def _row(self, r):
        if not self._COLUMNS:
            Queue._COLUMNS = [d[0] for d in self.db.execute("SELECT * FROM job LIMIT 0").description]
        d = dict(zip(self._COLUMNS, r))
        for k in ("argv", "results"):
            if d.get(k):
                try:
                    d[k] = json.loads(d[k])
                except Exception:
                    pass
        return d

    def _new_id(self):
        """Persistent counter: neither COUNT(*) nor MAX(rowid) survives pruning (ids would
        restart, and 'j0007' would name two different jobs at different times)."""
        cur = self.db.execute("SELECT value FROM meta WHERE key='last_id'").fetchone()
        n = (cur[0] if cur else max(
            (int(r[0][1:]) for r in self.db.execute("SELECT id FROM job") if r[0][1:].isdigit()),
            default=0)) + 1
        self.db.execute("INSERT INTO meta (key, value) VALUES ('last_id', ?) "
                        "ON CONFLICT(key) DO UPDATE SET value=?", (n, n))
        return f"j{n:04d}"

    def position(self, jid):
        r = self.db.execute("SELECT prio, created, state FROM job WHERE id=?", (jid,)).fetchone()
        if not r or r[2] != "queued":
            return 0
        n = self.db.execute(
            "SELECT COUNT(*) FROM job WHERE state='queued' AND (prio > ? OR (prio = ? AND created < ?))",
            (r[0], r[0], r[1])).fetchone()[0]
        return n + 1

    # -------------------------------------------------------------------- API
    def add(self, kind, note="", who="", argv=None, attended=False, ttl=None, prio=None,
            may_raise=False):
        """`prio` from the caller can only LOWER the priority the registry gives the activity —
        yielding your turn is polite, granting yourself precedence is not. A caller that declared
        priority 999 would walk past everybody and make a preemptible holder yield to it.
        `may_raise` is for the administrative path, the one that already needs a token."""
        a = self.arb.activity(kind)   # raises KeyError if the kind is not in the registry
        mine = int(a.get("priority", 50))
        if prio is None:
            prio = mine
        elif not may_raise:
            prio = min(int(prio), mine)
        with self.lock:
            jid = self._new_id()
            self.db.execute(
                "INSERT INTO job (id,kind,note,who,prio,state,attended,argv,ttl,created) "
                "VALUES (?,?,?,?,?,'queued',?,?,?,?)",
                (jid, kind, note, who, int(prio),
                 1 if attended else 0, json.dumps(argv) if argv else None, ttl, time.time()))
            self.db.commit()
            self.events[jid] = {"turn": threading.Event(), "end": threading.Event(), "outcome": {}}
        self.log(f"job {jid} queued: {kind} ({note}) from {who or 'unknown'}")
        return self.job_state(jid)

    def job_state(self, jid):
        r = self.db.execute("SELECT * FROM job WHERE id=?", (jid,)).fetchone()
        if not r:
            return None
        d = self._row(r)
        d["position"] = self.position(jid)
        if d["state"] == "queued":
            d["ahead"] = self.ahead_of(jid)
        return d

    def ahead_of(self, jid):
        """Who is ahead of this job: the running one plus the queued ones that outrank it."""
        out = []
        if self.current:
            c = self.db.execute("SELECT kind,note,started FROM job WHERE id=?", (self.current,)).fetchone()
            if c:
                out.append({"id": self.current, "kind": c[0], "note": c[1],
                              "held_s": int(time.time() - (c[2] or time.time())), "state": "running"})
        r = self.db.execute("SELECT prio, created FROM job WHERE id=?", (jid,)).fetchone()
        if r:
            for q in self.db.execute(
                    "SELECT id,kind,note FROM job WHERE state='queued' AND (prio > ? OR (prio = ? AND created < ?)) "
                    "ORDER BY prio DESC, created", (r[0], r[0], r[1])):
                out.append({"id": q[0], "kind": q[1], "note": q[2], "state": "queued"})
        return out

    def listing(self, how_many=30, state=None):
        q = "SELECT * FROM job"
        p = ()
        if state:
            q += " WHERE state=?"
            p = (state,)
        q += " ORDER BY created DESC LIMIT ?"
        return [self._row(r) for r in self.db.execute(q, p + (how_many,))]

    def cancel(self, jid):
        """Cancel a job. If it is running, stop the process AND the work inside the service:
        killing `run_prompt.py` does not remove the prompt from ComfyUI's own queue, which
        would keep the GPU busy for work nobody is waiting for any more."""
        kind = None
        with self.lock:
            r = self.db.execute("SELECT state, kind FROM job WHERE id=?", (jid,)).fetchone()
            if not r:
                return False, "unknown id"
            state, kind, stop_upstream = r[0], r[1], None
            if state in ("queued", "running"):
                self._cancelled.add(jid)
            if state == "queued":
                self.db.execute("UPDATE job SET state='cancelled', finished=? WHERE id=?", (time.time(), jid))
                self.db.commit()
                ev = self.events.get(jid)
                if ev:
                    ev["outcome"] = {"state": "cancelled"}
                    ev["turn"].set()
                    ev["end"].set()
                return True, "cancelled before starting"
            if state != "running":
                return False, f"state {state}"
            if jid != self.current and jid not in self._procs:
                # ghost row (an attended job never closed): close it without touching other processes
                self.db.execute("UPDATE job SET state='cancelled', finished=? WHERE id=?",
                                (time.time(), jid))
                self.db.commit()
                return True, "row closed (it was not the running job)"
            ev = self.events.get(jid)
            stop_upstream = None
            if ev and not ev["end"].is_set():
                ev["outcome"] = {"state": "cancelled"}
                stop_upstream = ev.get("cancel")
                if stop_upstream is None:
                    ev["end"].set()      # nothing is running elsewhere: the worker can let go
                # else: whoever is attending closes the job. Releasing the lease now, while the
                # model is still generating upstream, is exactly two workloads on one card.
            p = self._procs.get(jid)
            if p and p.poll() is None:
                stopped = self._stop(p, jid)
                outcome = "interrupted while running" if stopped else "it will not stop: check it by hand"
            elif stop_upstream is not None:
                self.db.execute("UPDATE job SET state='cancelled', finished=? WHERE id=?",
                                (time.time(), jid))
                self.db.commit()
                outcome = "stopping the request upstream"
            else:
                # Taken in hand by the worker but not launched yet (it is waiting for the lease):
                # close the row now. Saying "cancelled" and leaving it `running` until the worker
                # happens to pass by again is how a cancelled job looks alive for minutes.
                self.db.execute("UPDATE job SET state='cancelled', finished=? WHERE id=?",
                                (time.time(), jid))
                self.db.commit()
                outcome = "cancelled before starting"   # the marker stays: the worker must see it
        if stop_upstream is not None:      # outside the lock: closing a socket can block
            try:
                stop_upstream()
            except Exception as e:
                self.log(f"job {jid}: could not stop the upstream request ({e})")
        # outside the lock: interrupting talks HTTP to the service and can be slow
        try:
            cmd = self.arb.activity(kind).get("interrupt") if kind else None
            if cmd:
                subprocess.run(["bash", "-c", cmd], capture_output=True, timeout=30)
                self.log(f"job {jid}: stopped the work inside {kind} too")
        except Exception as e:
            self.log(f"job {jid}: interrupting {kind} failed ({e})")
        return True, outcome

    # ------------------------------------------------------------ waiting for a turn
    def wait_turn(self, jid, timeout):
        """Used by attended jobs (the LLM proxy): True only when it is really their turn.
        `turn` is also raised by cancel/give_up: without rechecking the state, a cancelled
        cancelled job would be forwarded upstream without holding the lease."""
        ev = self.events.get(jid)
        if not ev or not ev["turn"].wait(timeout):
            return False
        r = self.db.execute("SELECT state FROM job WHERE id=?", (jid,)).fetchone()
        return bool(r and r[0] == "running")

    def save_lease(self, holder):
        """Called by the arbiter on every change. The holder is kept as JSON in the same database
        as the jobs, so one file is the whole state."""
        try:
            with self.lock:
                if holder is None:
                    self.db.execute("DELETE FROM lease")
                else:
                    self.db.execute("INSERT INTO lease (only_one, holder) VALUES (1, ?) "
                                    "ON CONFLICT(only_one) DO UPDATE SET holder=?",
                                    (json.dumps(holder), json.dumps(holder)))
                self.db.commit()
        except Exception as e:
            self.log(f"lease not saved: {e}")

    def load_lease(self):
        r = self.db.execute("SELECT holder FROM lease").fetchone()
        try:
            return json.loads(r[0]) if r and r[0] else None
        except Exception:
            return None

    def attach_canceller(self, jid, fn):
        """An attended job runs somewhere else (the proxy holds the connection to llama-swap):
        register how to stop it, or `cancel` can only lie about having stopped anything.
        Returns False if the job was already cancelled while the caller was getting here — the
        window between being given the turn and registering, where a cancel finds nothing to stop."""
        with self.lock:
            ev = self.events.get(jid)
            if ev is None:
                return False
            if jid in self._cancelled or (ev["outcome"] or {}).get("state") == "cancelled":
                return False
            ev["cancel"] = fn
            return True

    def finish_attended(self, jid, rc=0, output=""):
        with self.lock:
            # AND state='running': the job may have been cancelled while the answer was streaming,
            # and a cancelled job must not come back as done.
            self.db.execute("UPDATE job SET state=?, finished=?, rc=?, output=? "
                            "WHERE id=? AND state='running'",
                            ("done" if rc == 0 else "error", time.time(), rc, output[-500:], jid))
            self.db.commit()
            self._cancelled.discard(jid)
        ev = self.events.get(jid)
        if ev:
            ev["end"].set()

    def give_up(self, jid, reason="the caller stopped waiting"):
        """The waiter gave up: the job must not keep holding its turn."""
        with self.lock:
            r = self.db.execute("SELECT state FROM job WHERE id=?", (jid,)).fetchone()
            if not r:
                return
            if r[0] == "queued":
                self.db.execute("UPDATE job SET state='abandoned', finished=?, output=? WHERE id=?",
                                (time.time(), reason, jid))
                self.db.commit()
                ev = self.events.get(jid)
                if ev:
                    ev["turn"].set()
                    ev["end"].set()
                self.log(f"job {jid}: {reason} (it was queued)")
                return
        if r[0] == "running":
            self.finish_attended(jid, 1, reason)
            self.log(f"job {jid}: {reason} (it had just got its turn)")

    def prune(self, days=None, keep=None):
        """Drop old finished jobs. Ids stay monotonic (MAX(rowid)), so pruning is safe.
        Defaults come from config.yaml (history_days, jobs_kept)."""
        days = config.C["history_days"] if days is None else days
        keep = config.C["jobs_kept"] if keep is None else keep
        with self.lock:
            cutoff = time.time() - days * 86400
            n = self.db.execute(
                "DELETE FROM job WHERE finished IS NOT NULL AND finished < ? "
                "AND id NOT IN (SELECT id FROM job ORDER BY created DESC LIMIT ?)", (cutoff, keep)).rowcount
            self.db.commit()
        if n:
            self.log(f"queue: {n} old jobs removed")
        return n

    # ------------------------------------------------------------------ worker
    def aging_cap(self):
        """How much priority a waiting job may gain: it has to exceed the widest gap in the
        registry, or the lowest-priority work never overtakes the highest and starves. Read from
        the registry rather than hardcoded, because the gap is whatever the user wrote."""
        try:
            prios = [int(a.get("priority", 50)) for a in self.arb.reg.get("activities", {}).values()]
        except Exception:
            prios = []
        return max(60, (max(prios) - min(prios) + 1) if prios else 0)

    def _next(self):
        # aging: +1 priority per minute waited, capped so that a low-priority job eventually
        # overtakes the highest-priority one instead of waiting forever.
        r = self.db.execute(
            "SELECT id FROM job WHERE state='queued' AND kind != 'cpu' "
            "ORDER BY prio + MIN(?, (? - created)/60) DESC, created LIMIT 1",
            (self.aging_cap(), time.time())).fetchone()
        return r[0] if r else None

    def loop(self):
        """The worker. `stopped` is there so it can be shut down cleanly — a test that leaves its
        worker running goes on touching a database the next test has already thrown away."""
        last_prune = time.time()
        while not self.stopped:
            time.sleep(0.5)
            if time.time() - last_prune > 86400:
                last_prune = time.time()
                try:
                    self.prune()
                except Exception as e:
                    self.log(f"pruning: {e}")
            try:
                with self.lock:
                    # CPU lane: always starts, even while the GPU is held by another job
                    running_cpu = sum(1 for k in list(self._procs)
                                      if (self.db.execute("SELECT kind FROM job WHERE id=?", (k,)).fetchone() or [""])[0] == "cpu")
                    r_cpu = None if running_cpu >= self.CPU_LANE_MAX else self.db.execute(
                        "SELECT id FROM job WHERE state='queued' AND kind='cpu' ORDER BY created LIMIT 1").fetchone()
                    if r_cpu:
                        jc = self._row(self.db.execute("SELECT * FROM job WHERE id=?", (r_cpu[0],)).fetchone())
                        self.db.execute("UPDATE job SET state='running', started=? WHERE id=?",
                                        (time.time(), jc["id"]))
                        self.db.commit()
                        threading.Thread(target=self._launch_guarded, args=(jc, None, time.time()), daemon=True).start()
                        continue
                    if self.current:
                        continue
                    jid = self._next()
                    if not jid:
                        continue
                    j = self._row(self.db.execute("SELECT * FROM job WHERE id=?", (jid,)).fetchone())
                    self.db.execute("UPDATE job SET state='running', started=? WHERE id=?", (time.time(), jid))
                    self.db.commit()
                    if j["kind"] == "cpu":
                        # no GPU involved: it takes nobody's turn
                        threading.Thread(target=self._launch_guarded, args=(j, None, time.time()), daemon=True).start()
                        continue
                    self.current = jid
                # in a thread: the loop must stay free to serve the CPU lane and to answer
                # (GPU exclusivity is guaranteed by `self.current`, not by blocking the loop)
                threading.Thread(target=self._run_guarded, args=(j,), daemon=True).start()
            except Exception as e:
                self.log(f"worker: {e}")
                with self.lock:
                    if self.current:      # the row must not stay running: nobody would pick it up again
                        self.db.execute("UPDATE job SET state='queued', started=NULL WHERE id=? "
                                        "AND state='running'", (self.current,))
                        self.db.commit()
                    self.current = None
                time.sleep(2)

    CPU_LANE_MAX = 4     # the lane runs beside the GPU holder: without a cap, 100 submits = 100 processes
    STOP_GRACE_S = 5      # SIGTERM, then this long, then SIGKILL: nothing survives a cancel

    def _stop(self, proc, jid, grace=None):
        """Stop a job for good: signal the whole process group, escalate to KILL if it does not die.
        Terminating the parent alone leaves the children holding the GPU while the row says cancelled."""
        grace = self.STOP_GRACE_S if grace is None else grace
        for sig, label in ((signal.SIGTERM, "TERM"), (signal.SIGKILL, "KILL")):
            if proc.poll() is not None:
                return True
            try:
                os.killpg(os.getpgid(proc.pid), sig)
            except (ProcessLookupError, PermissionError):
                try:
                    proc.terminate() if sig == signal.SIGTERM else proc.kill()
                except Exception:
                    return proc.poll() is not None
            try:
                proc.wait(timeout=grace if sig == signal.SIGTERM else 3)
                return True
            except subprocess.TimeoutExpired:
                self.log(f"job {jid}: did not stop on {label}")
        return proc.poll() is not None

    def _drop_if_cancelled(self, jid):
        """True if the job was cancelled meanwhile: closes the row and does not run anything."""
        with self.lock:
            if jid not in self._cancelled:
                r = self.db.execute("SELECT state FROM job WHERE id=?", (jid,)).fetchone()
                if r and r[0] == "cancelled":
                    self.log(f"job {jid}: cancelled before starting, not run")
                    return True
                return False
            self._cancelled.discard(jid)
            self.db.execute("UPDATE job SET state='cancelled', finished=? WHERE id=? AND finished IS NULL",
                            (time.time(), jid))
            self.db.commit()
        self.log(f"job {jid}: cancelled before starting, not run")
        return True

    def _launch_guarded(self, j, token, t0):
        """The CPU lane runs outside the lease, so nothing else would notice it dying. Without this,
        an argv that cannot start (empty, or a binary this machine does not have) leaves the row
        `running` for ever: nobody picks it up, nobody closes it, and `follow` polls until you give up."""
        try:
            self._launch(j, token, t0)
        except Exception as e:
            self.log(f"job {j['id']}: could not start ({e})")
            with self.lock:
                self.db.execute("UPDATE job SET state='error', finished=?, rc=-1, output=? "
                                "WHERE id=? AND state='running'", (time.time(), str(e)[:500], j["id"]))
                self.db.commit()
                self._procs.pop(j["id"], None)

    def _launch(self, j, token, t0):
        """Run the job's argv. With a `token` it keeps the lease alive; without, it is CPU-only work."""
        jid, kind = j["id"], j["kind"]
        t0m = time.monotonic()
        argv = j["argv"] or []
        if self._drop_if_cancelled(jid):
            return
        self.log(f"job {jid}: running {' '.join(argv)[:120]}")
        # The TTL is also the runtime cap: past it, the job is terminated. Without it a hung command
        # would hold the queue forever; and without heartbeat the lease would expire under a long job.
        try:
            cap = float(j["ttl"] or self.arb.activity(kind).get("default_ttl", 1800))
        except KeyError:
            cap = float(j["ttl"] or 1800)
        beat = max(5, int(self.arb.glob("heartbeat_s", 30)) // 2)
        # Not /tmp: a predictable name in a world-writable directory is a symlink waiting to happen
        # (anyone can pre-create vramble-j0044.log pointing at a file of yours, and we would open it "w").
        log_dir = config.base("jobs")
        os.makedirs(log_dir, mode=0o700, exist_ok=True)
        os.chmod(log_dir, 0o700)   # also tighten a directory left behind by an older install
        log_file = os.path.join(log_dir, f"{jid}.log")
        expired = False
        with open(log_file, "w") as lf:
            # start_new_session: the job becomes its own process group, so stopping it reaches the
            # children too (run_prompt.py → python → …), instead of leaving orphans on the GPU.
            proc = subprocess.Popen(argv, stdout=lf, stderr=subprocess.STDOUT, text=True,
                                    start_new_session=True)
            with self.lock:
                self._procs[jid] = proc        # CPU lane included: cancel must reach it too
            while proc.poll() is None:
                time.sleep(min(beat, 5))
                if token:
                    self.arb.heartbeat(token)
                if time.monotonic() - t0m > cap:
                    expired = True
                    self.log(f"job {jid}: past the {cap:.0f}s cap → stopping it")
                    self._stop(proc, jid, grace=15)
                    break
            rc = proc.wait()
        with self.lock:
            self._procs.pop(jid, None)
        try:
            out = open(log_file).read()
        except Exception:
            out = ""
        finally:
            try:
                os.unlink(log_file)     # also when reading failed: no leftovers piling up
            except OSError:
                pass
        # cancelled while it was already running: the rc is the one of a terminated process, and
        # calling that "error" would put a red line in the history for something the user asked for.
        cancelled = jid in self._cancelled
        self._cancelled.discard(jid)
        state = "cancelled" if cancelled else ("expired" if expired else ("done" if rc == 0 else "error"))
        with self.lock:
            self.db.execute("UPDATE job SET state=?, finished=?, rc=?, output=? WHERE id=?",
                            (state, time.time(), rc, (out or "")[-2000:], jid))
            self.db.commit()
        self.log(f"job {jid}: {state}{'' if rc == 0 else ' rc=' + str(rc)} in {time.time() - t0:.1f}s")

    def _run_guarded(self, j):
        try:
            self._run(j)
        except Exception as e:
            self.log(f"job {j['id']}: unexpected error ({e})")
            with self.lock:
                self.db.execute("UPDATE job SET state='error', finished=?, rc=-1, output=? WHERE id=? "
                                "AND state='running'", (time.time(), str(e)[:500], j["id"]))
                self.db.commit()
        finally:
            with self.lock:
                if self.current == j["id"]:
                    self.current = None

    def _run(self, j):
        jid, kind = j["id"], j["kind"]
        t0 = time.time()
        # internal=False: a queued job is a new request, not a piece of the work in progress
        code, lease = self.arb.acquire(kind, note=j["note"] or jid, ttl=j["ttl"],
                                            wait=15, wait_id=f"job:{jid}", internal=False,
                                            since=j["created"])
        if code != 200:
            # the machine is held by something outside the queue (a manual run, a training)
            attempts = self._attempts.get(jid, 0) + 1
            self._attempts[jid] = attempts
            if attempts in (1, 10) or attempts % 60 == 0:
                self.log(f"job {jid}: lease denied ({lease.get('error')}), attempt {attempts} → stays queued")
            with self.lock:
                if attempts > 600:        # ~1 h of waiting: better to say so than hang in silence
                    # Same guard as the branch below: a job cancelled while we kept retrying must
                    # stay cancelled, not be relabelled as an error an hour later.
                    self.db.execute("UPDATE job SET state='error', finished=?, rc=-1, output=? "
                                    "WHERE id=? AND state='running'",
                                    (time.time(), "the machine stayed held by something outside the queue", jid))
                    self.log(f"job {jid}: giving up after {attempts} attempts")
                else:
                    # AND state='running': a job cancelled while we were waiting for the lease must
                    # not be put back in the queue — that is how a cancelled job comes back to life.
                    self.db.execute("UPDATE job SET state='queued', started=NULL "
                                    "WHERE id=? AND state='running'", (jid,))
                self.db.commit()
                self.current = None
            time.sleep(3)
            return
        self._attempts.pop(jid, None)
        token = lease.get("token")
        if self._drop_if_cancelled(jid):      # cancelled while it was waiting for its turn
            self.arb.release_lease(token)
            with self.lock:
                self.current = None
            return
        try:
            if j["attended"]:
                ev = self.events.get(jid)
                self.log(f"job {jid}: your turn ({kind} · {j['note']})")
                if ev:
                    ev["turn"].set()
                    # The work happens upstream (the proxy holds the connection), so nothing here
                    # renews the lease: a generation longer than the TTL used to have the card
                    # taken away from under it. Beat until the attendant says it is over.
                    cap = float(j["ttl"] or 900)
                    beat = max(5, int(self.arb.glob("heartbeat_s", 30)) // 2)
                    deadline = time.monotonic() + cap
                    while not ev["end"].wait(min(beat, max(0.1, deadline - time.monotonic()))):
                        if time.monotonic() >= deadline:
                            # Stop the work upstream BEFORE letting the lease go. Walking away from
                            # a live stream is how the next tenant drains a model mid-generation.
                            self.log(f"job {jid}: past the {cap:.0f}s cap → stopping it")
                            stop = ev.get("cancel")
                            if stop:
                                try:
                                    stop()
                                except Exception as e:
                                    self.log(f"job {jid}: could not stop the upstream request ({e})")
                                ev["end"].wait(30)      # give the attendant time to close the job
                            break
                        if token:
                            self.arb.heartbeat(token)
                else:
                    self.finish_attended(jid, 1, "nobody was attending the job")
            else:
                self._launch_guarded(j, token, t0)
        finally:
            if token:
                self.arb.release_lease(token)
            with self.lock:
                self.current = None
