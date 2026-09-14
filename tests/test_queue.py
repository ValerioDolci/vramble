"""The queue: order, priority, aging, cancellation, CPU lane, elapsed cap."""
import sys, os, threading, time, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import VrambleTest


class Queue(VrambleTest):
    def _wait_final(self, jid, limit=25):
        """Wait for a final state instead of a fixed time: under load the timings wobble."""
        for _ in range(int(limit * 10)):
            st = (self.waiting.job_state(jid) or {}).get("state")
            if st in ("done", "error", "cancelled", "expired", "interrupted", "abandoned"):
                return st
            time.sleep(0.1)
        return self.waiting.job_state(jid)["state"]


    def add(self, kind, note="x", argv=None, **kw):
        return self.waiting.add(kind, note, "tests", argv or ["/bin/true"], **kw)

    def test_job_enters_the_queue_with_an_id(self):
        j = self.add("comfy")
        self.assertTrue(j["id"].startswith("j"))
        self.assertEqual(j["state"], "queued")
        self.assertEqual(j["position"], 1)

    def test_priority_decides_the_order(self):
        low = self.add("maintenance")          # prio 10
        high = self.add("comfy")                  # prio 50
        self.assertEqual(self.waiting._next(), high["id"], "the higher priority goes first")

    def test_same_priority_first_come_first_served(self):
        first = self.add("comfy", note="first")
        time.sleep(0.01)
        self.add("comfy", note="second")
        self.assertEqual(self.waiting._next(), first["id"])

    def test_aging_prevents_starvation(self):
        older = self.add("maintenance")        # prio 10, but waiting for 2 hours
        self.waiting.db.execute("UPDATE job SET created=? WHERE id=?",
                             (time.time() - 7200, older["id"]))
        self.waiting.db.commit()
        self.add("comfy")                         # prio 50, just arrived
        self.assertEqual(self.waiting._next(), older["id"],
                         "after two hours of waiting the low-priority job must go through")

    def test_caller_is_told_who_is_ahead(self):
        a = self.add("comfy", note="first")
        b = self.add("comfy", note="second")
        ahead = self.waiting.job_state(b["id"])["ahead"]
        self.assertEqual([d["id"] for d in ahead], [a["id"]])

    def test_cancel_a_queued_job(self):
        j = self.add("comfy")
        ok, _ = self.waiting.cancel(j["id"])
        self.assertTrue(ok)
        self.assertEqual(self.waiting.job_state(j["id"])["state"], "cancelled")
        self.assertIsNone(self.waiting._next(), "a cancelled job must not be served")

    def test_giving_up_removes_the_job_from_the_queue(self):
        j = self.add("llm", attended=True)
        self.waiting.give_up(j["id"])
        self.assertEqual(self.waiting.job_state(j["id"])["state"], "abandoned")
        self.assertIsNone(self.waiting._next(), "a job that gave up must not hold the turn")

    def test_abandoned_job_never_gets_its_turn(self):
        j = self.add("llm", attended=True)
        self.waiting.give_up(j["id"])
        self.assertFalse(self.waiting.wait_turn(j["id"], 0.2),
                         "wait_turn must tell 'your turn' from 'you were cancelled'")

    def test_job_runs_and_output_is_recorded(self):
        j = self.add("comfy", argv=["/bin/echo", "hello world"])
        self.waiting._run(self.waiting._row(
            self.waiting.db.execute("SELECT * FROM job WHERE id=?", (j["id"],)).fetchone()))
        done_job = self.waiting.job_state(j["id"])
        self.assertEqual(done_job["state"], "done")
        self.assertEqual(done_job["rc"], 0)
        self.assertIn("hello world", done_job["output"])

    def test_ttl_is_also_the_runtime_cap(self):
        j = self.add("comfy", argv=["/bin/sleep", "30"], ttl=1)
        t0 = time.time()
        self.waiting._run(self.waiting._row(
            self.waiting.db.execute("SELECT * FROM job WHERE id=?", (j["id"],)).fetchone()))
        elapsed = time.time() - t0
        self.assertLess(elapsed, 20, "a hung command must not hold the queue")
        self.assertEqual(self.waiting.job_state(j["id"])["state"], "expired")

    def test_failed_job_reports_its_code(self):
        j = self.add("comfy", argv=["/bin/sh", "-c", "exit 3"])
        self.waiting._run(self.waiting._row(
            self.waiting.db.execute("SELECT * FROM job WHERE id=?", (j["id"],)).fetchone()))
        r = self.waiting.job_state(j["id"])
        self.assertEqual((r["state"], r["rc"]), ("error", 3))

    def test_cpu_jobs_stay_out_of_the_gpu_lane(self):
        cpu = self.add("cpu")
        self.assertIsNone(self.waiting._next(), "the GPU lane must not pick up cpu jobs")
        self.assertEqual(self.waiting.job_state(cpu["id"])["state"], "queued")

    def test_ids_stay_unique_after_pruning(self):
        for _ in range(3):
            j = self.add("comfy")
            self.waiting.db.execute("UPDATE job SET state='done', finished=? WHERE id=?",
                                 (time.time() - 90 * 86400, j["id"]))
        self.waiting.db.commit()
        self.waiting.prune(days=30, keep=0)
        new = self.add("comfy")
        self.assertNotIn(new["id"], ["j0001", "j0002", "j0003"],
                         "with COUNT(*) the ids would start colliding again")

    def test_pruning_keeps_recent_jobs(self):
        j = self.add("comfy")
        self.waiting.db.execute("UPDATE job SET state='done', finished=? WHERE id=?", (time.time(), j["id"]))
        self.waiting.db.commit()
        self.waiting.prune(days=30)
        self.assertIsNotNone(self.waiting.job_state(j["id"]))




    def test_cancel_before_it_starts_really_stops_it(self):
        """Cancelling a job the worker has already picked (but not launched: it is still waiting for
        the lease) must stop it. Before the fix the row was closed and the command ran anyway."""
        import os, tempfile, time as _t
        witness = os.path.join(tempfile.mkdtemp(), "ran")
        code, lease = self.arb.acquire("research", note="holds the machine", ttl=60)   # not preemptible
        self.assertEqual(code, 200)
        j = self.waiting.add("comfy", "to cancel", "tests", ["/usr/bin/touch", witness])
        threading.Thread(target=self.waiting.loop, daemon=True).start()
        for _ in range(60):                       # wait for the worker to take it in hand
            st = self.waiting.job_state(j["id"]) or {}
            if st.get("state") == "running" or self.waiting.current:
                break
            _t.sleep(0.1)
        # The worker may be anywhere in its retry cycle: holding the row as `running` while it waits
        # for the lease, or having just put it back to `queued` after a refusal. Both are cancellable,
        # and which one we catch is a matter of milliseconds — so try until the row is closed rather
        # than assuming the first attempt lands in the right phase.
        for _ in range(40):
            ok, perche = self.waiting.cancel(j["id"])
            if ok or (self.waiting.job_state(j["id"]) or {}).get("state") == "cancelled":
                break
            _t.sleep(0.1)
        self.assertTrue(ok, f"cancel refused: {perche}")
        self.arb.release_lease(lease["token"])    # the machine frees up: the worker can start
        state = self._wait_final(j["id"])
        self.assertFalse(os.path.exists(witness), "a cancelled job must not run all the same")
        self.assertEqual(state, "cancelled")


    def test_cancel_stops_a_cpu_lane_job_too(self):
        """The CPU lane runs outside the lease (it holds nobody's turn), so its job is never
        `current`: cancel used to close the row and let the process finish its work."""
        import os, tempfile, time as _t
        witness = os.path.join(tempfile.mkdtemp(), "ran")
        j = self.waiting.add("cpu", "cpu to cancel", "tests",
                             ["/bin/sh", "-c", f"sleep 6; touch {witness}"])
        threading.Thread(target=self.waiting.loop, daemon=True).start()
        for _ in range(40):
            if (self.waiting.job_state(j["id"]) or {}).get("state") == "running":
                break
            _t.sleep(0.1)
        ok, _ = self.waiting.cancel(j["id"])
        self.assertTrue(ok)
        state = self._wait_final(j["id"])
        self.assertFalse(os.path.exists(witness), "the process must have been stopped, not just the row")
        self.assertEqual(state, "cancelled")


    def test_a_queued_job_keeps_its_seniority_in_the_line(self):
        """A queued job retries every ~18 s: its seniority in the lease line must be the moment it was
        queued, not the moment of the attempt, or whoever arrives later jumps ahead of it."""
        import time as _t
        old = self.waiting.add("comfy", "queued much earlier", "tests")
        self.waiting.db.execute("UPDATE job SET created=? WHERE id=?",
                                (_t.time() - 600, old["id"]))
        self.waiting.db.commit()
        j = self.waiting.job_state(old["id"])
        self.arb.acquire("research", note="holds the machine", ttl=60)      # machine busy
        # the human queues up now; the job retries now, but has been waiting for 10 minutes
        self.arb.acquire("comfy", wait=30, wait_id="human")
        self.arb.acquire("comfy", wait=30, wait_id=f"job:{j['id']}", since=j["created"])
        line = self.arb.state()["queue"]
        first_job = max(line, key=lambda c: c["waiting_s"])
        self.assertGreater(first_job["waiting_s"], 300, "the job must show as waiting for 10 minutes")


    def test_cancelling_a_streaming_job_keeps_the_lease_until_it_stops(self):
        """An attended job (the LLM proxy) runs upstream: cancelling it used to unblock the worker,
        which gave the lease back while the model was still generating — two workloads, one card."""
        import threading, time as _t
        j = self.waiting.add("llm", "streaming", "tests", attended=True, ttl=30)
        threading.Thread(target=self.waiting.loop, daemon=True).start()
        self.assertTrue(self.waiting.wait_turn(j["id"], 10), "the job must get its turn")
        stopped = threading.Event()
        self.waiting.attach_canceller(j["id"], stopped.set)     # what the proxy registers

        ok, note = self.waiting.cancel(j["id"])
        self.assertTrue(ok)
        self.assertTrue(stopped.wait(5), "cancel must stop the work running upstream")
        _t.sleep(1)
        self.assertEqual(self.arb.state()["holder"]["activity"], "llm",
                         "the lease must stay until whoever is attending says it is over")

        self.waiting.finish_attended(j["id"], 1, "closed by the client")   # the proxy returns
        for _ in range(50):
            if self.arb.state()["holder"] is None:
                break
            _t.sleep(0.1)
        self.assertIsNone(self.arb.state()["holder"], "then the lease goes back")
        self.assertEqual(self.waiting.job_state(j["id"])["state"], "cancelled",
                         "and a cancelled job must not come back as done")


    def test_a_cancelled_job_is_not_put_back_in_the_queue(self):
        """A worker denied the lease puts the job back in the queue: if it was cancelled meanwhile,
        that requeue used to bring it back to life and the command ran all the same."""
        j = self.waiting.add("comfy", "cancelled while waiting", "tests", ["/bin/true"])
        with self.waiting.lock:                      # the worker has taken it in hand
            self.waiting.db.execute("UPDATE job SET state='running' WHERE id=?", (j["id"],))
            self.waiting.db.commit()
            self.waiting.current = j["id"]
        self.waiting.cancel(j["id"])
        self.assertEqual(self.waiting.job_state(j["id"])["state"], "cancelled")
        with self.waiting.lock:                      # ...and then the lease is denied
            self.waiting.db.execute("UPDATE job SET state='queued', started=NULL "
                                    "WHERE id=? AND state='running'", (j["id"],))
            self.waiting.db.commit()
        self.assertEqual(self.waiting.job_state(j["id"])["state"], "cancelled",
                         "a cancelled job must not go back in the queue")
        self.assertIsNone(self.waiting._next(), "and must not be the next one to start")

    def test_a_caller_can_lower_its_priority_but_not_raise_it(self):
        """Yielding your turn is polite; granting yourself precedence is not. A request declaring
        priority 999 would walk past everybody and make a preemptible holder yield to it."""
        registry = int(self.arb.reg["activities"]["comfy"]["priority"])
        polite = self.waiting.add("comfy", "yields", "tests", prio=1)
        self.assertEqual(polite["prio"], 1, "lowering must work: it is how a background job behaves")
        pushy = self.waiting.add("comfy", "pushy", "tests", prio=999)
        self.assertEqual(pushy["prio"], registry, "the registry decides, not the caller")
        admin = self.waiting.add("comfy", "admin", "tests", prio=999, may_raise=True)
        self.assertEqual(admin["prio"], 999, "the path that needs a token may still raise")


    def test_a_job_cancelled_before_the_proxy_registers_never_starts(self):
        """Between being given the turn and registering how to stop it, a cancel used to find
        nothing to stop: the worker let the lease go while the request was on its way to the model."""
        import threading
        j = self.waiting.add("llm", "streaming", "tests", attended=True, ttl=30)
        threading.Thread(target=self.waiting.loop, daemon=True).start()
        self.assertTrue(self.waiting.wait_turn(j["id"], 10))
        self.waiting.cancel(j["id"])                       # arrives in the window
        self.assertFalse(self.waiting.attach_canceller(j["id"], lambda: None),
                         "whoever registers late must be told the job is already cancelled")


    def test_a_job_that_cannot_start_is_closed_not_left_running(self):
        """The CPU lane runs outside the lease: if the command cannot start, nobody would notice.
        The row used to stay `running` for ever and `follow` polled until you gave up."""
        j = self.waiting.add("cpu", "impossible", "tests", [])      # empty argv: Popen raises
        threading.Thread(target=self.waiting.loop, daemon=True).start()
        state = self._wait_final(j["id"], limit=20)
        self.assertEqual(state, "error", "a job that cannot start must be closed with its reason")
        self.assertIn("rc", self.waiting.job_state(j["id"]))


    def test_a_cancelled_job_that_waited_an_hour_stays_cancelled(self):
        """Giving up after ~1 h of denied leases used to relabel the row as an error, even if the
        job had been cancelled meanwhile: the sibling branch had the guard, this one did not."""
        j = self.waiting.add("comfy", "cancelled while waiting", "tests", ["/bin/true"])
        with self.waiting.lock:
            self.waiting.db.execute("UPDATE job SET state='cancelled', finished=? WHERE id=?",
                                    (time.time(), j["id"]))
            self.waiting.db.execute("UPDATE job SET state='error', finished=?, rc=-1, output=? "
                                    "WHERE id=? AND state='running'",
                                    (time.time(), "gave up", j["id"]))
            self.waiting.db.commit()
        self.assertEqual(self.waiting.job_state(j["id"])["state"], "cancelled",
                         "giving up must not overwrite a cancelled job")

    def test_aging_outgrows_the_widest_gap_in_the_registry(self):
        """The cap has to exceed the widest priority gap, or the lowest-priority work never
        overtakes the highest and waits forever. It is read from the registry, not hardcoded."""
        prios = [int(a.get("priority", 50)) for a in self.arb.reg["activities"].values()]
        self.assertGreater(self.waiting.aging_cap(), max(prios) - min(prios),
                           "a job at the bottom must be able to overtake one at the top")



if __name__ == "__main__":
    unittest.main(verbosity=2)
