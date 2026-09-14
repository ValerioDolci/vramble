"""Who holds the machine slot: allocation, preemption, reentrancy, degradation."""
import sys, os, time, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import VrambleTest


class Lease(VrambleTest):

    def test_free_machine_is_granted(self):
        c, r = self.take("comfy", note="image")
        self.assertEqual(c, 200)
        self.assertEqual(self.holder(), "comfy")

    def test_preemptible_holder_gives_way(self):
        self.take("comfy")
        c, r = self.take("llm", note="model")
        self.assertEqual(c, 200, "llm (prio 50) must be able to evict comfy (preemptible, prio 50)")
        self.assertEqual(self.holder(), "llm")

    def test_non_preemptible_holder_keeps_the_machine(self):
        self.take("research", note="training")
        c, r = self.take("comfy")
        self.assertEqual(c, 409)
        self.assertEqual(r["holder"]["activity"], "research")
        self.assertFalse(r["holder"]["preemptible"])
        self.assertIn("default_wait", r, "the 409 must state how long that service is willing to wait")

    def test_preempt_evicts_who_can_yield(self):
        self.take("llm")
        c, r = self.arb.preempt("I need the machine")
        self.assertEqual(c, 200)
        self.assertIsNone(self.holder())

    def test_preempt_does_not_lie_about_who_cannot_yield(self):
        self.take("research", note="training")
        c, r = self.arb.preempt("trying")
        self.assertEqual(c, 409, "research has no drain commands: clearing the holder would be a lie")
        self.assertEqual(self.holder(), "research", "the training must remain the holder")

    def test_reentrant_lease_gets_its_own_token(self):
        _, a = self.take("llm")
        _, b = self.take("llm")
        self.assertNotEqual(a["token"], b["token"], "each acquisition must have its own token")
        self.arb.release_lease(b["token"])
        self.assertEqual(self.holder(), "llm", "releasing one must not dissolve the other's lease")
        self.arb.release_lease(a["token"])
        self.assertIsNone(self.holder())

    def test_compatibility_is_only_for_subprocesses(self):
        self.take("agent", note="run")
        c, _ = self.take("llm", internal=True)
        self.assertEqual(c, 200, "the model used INSIDE the agent's run does not queue")
        c, _ = self.take("llm", internal=False)
        self.assertEqual(c, 409, "a new request instead waits")

    def test_lease_degrades_when_owner_leaves(self):
        _, ag = self.take("agent", note="run")
        _, mod = self.take("llm", internal=True)          # the model loaded inside the run
        self.arb.release_lease(ag["token"])                      # the agent has finished
        self.assertEqual(self.holder(), "llm",
                         "the loaded model remains: the slot must become theirs (and it is preemptible)")
        c, _ = self.take("comfy", internal=False)
        self.assertEqual(c, 200, "now an image can take the slot")

    def test_expired_lease_is_revoked(self):
        _, l = self.take("llm", ttl=0.4)
        time.sleep(0.6)
        self.assertIsNone(self.holder(), "without heartbeat the lease expires")

    def test_heartbeat_keeps_lease_alive(self):
        _, l = self.take("llm", ttl=0.6)
        for _ in range(3):
            time.sleep(0.3)
            self.assertEqual(self.arb.heartbeat(l["token"])[0], 200)
        self.assertEqual(self.holder(), "llm")

    def test_heartbeat_on_dead_token(self):
        _, l = self.take("llm")
        self.arb.release_lease(l["token"])
        self.assertEqual(self.arb.heartbeat(l["token"])[0], 410)

    def test_per_model_vram_decides_drain_depth(self):
        a = self.arb.activity("llm")
        self.assertEqual(self.arb.requested_vram(a, "soft"), 14.0)
        self.assertEqual(self.arb.requested_vram(a, "anything"), 15.7)

    def test_lease_holder_is_never_drained(self):
        self.take("comfy")
        drained = []
        self.arb.drain = lambda name, hard, forced=False: drained.append(name)
        self.arb.free_leftovers(apart_from="research", requested_vram=16)
        self.assertNotIn("comfy", drained, "whoever holds the lease must not be touched")




    def test_the_cooldown_never_blocks_a_drain_needed_to_grant(self):
        """The cooldown exists to stop the idle watchdog from nagging a service that reloads itself.
        If it also silenced the drain that must happen before a grant, two models would land on
        the same card — the failure this whole daemon exists to prevent."""
        self.arb._cooldown["llm"] = time.monotonic() + 999      # as if a drain had freed nothing
        drained = []
        self.arb.drain = lambda name, hard, forced=False: drained.append((name, forced))
        self.arb.free_leftovers(apart_from="comfy", requested_vram=16)
        self.assertIn("llm", [n for n, _ in drained], "the leftover had to be drained anyway")
        self.assertTrue(all(f for _, f in drained), "a drain before a grant is always forced")

    def test_releasing_with_drain_does_not_hold_the_lock(self):
        """release --drain used to run the drain command inside the state lock: a slow drain froze
        every other lease, heartbeat and status call for as long as it took."""
        code, lease = self.arb.acquire("llm", note="model", ttl=60)
        self.assertEqual(code, 200)
        seen = {}

        def slow_drain(name, hard, forced=False):
            seen["locked"] = self.arb.lock.acquire(blocking=False)   # can anybody else get in?
            if seen["locked"]:
                self.arb.lock.release()

        self.arb.drain = slow_drain
        self.arb.release_lease(lease["token"], drain=True)
        self.assertTrue(seen.get("locked"), "the drain must run with the lock free")


    def test_a_lease_that_degrades_twice_does_not_nest_its_note(self):
        """The note used to be rewritten as "leftover of X (note)" at every degradation, so after a
        few hours the status line read "leftover of (leftover of (leftover of …))"."""
        code, owner = self.arb.acquire("agent", note="run", ttl=60)
        self.assertEqual(code, 200)
        code, child = self.arb.acquire("llm", note="model", ttl=60, internal=True)
        self.assertEqual(code, 200)
        self.arb.release_lease(owner["token"])          # the owner leaves, the model stays
        first = self.arb.state()["holder"]
        self.assertEqual(first["note"], "run", "the note is data: it must not be rewritten")
        self.assertEqual(first["leftover"], "agent", "who left is a flag of its own")

        code, again = self.arb.acquire("llm", note="model", ttl=60, internal=True)
        self.assertEqual(code, 200)
        self.arb.release_lease(child["token"])          # degrades a second time
        second = self.arb.state()["holder"]
        self.assertEqual(second["note"], "run")
        self.assertNotIn("leftover of", str(second["note"]), "no nesting, ever")


    def test_a_long_run_without_a_match_rule_survives_a_restart(self):
        """An activity that declares no `match` cannot be recognised on the card. Dropping its lease
        at every restart would hit exactly the longest runs — an agent, a training."""
        code, _ = self.arb.acquire("research", note="training", ttl=600)   # no `match` in the fixture
        self.assertEqual(code, 200)
        saved = self.arb.snapshot()

        fresh = self.vramble.Arbiter()
        fresh.vram_readable = lambda: True
        fresh.vram_holders = lambda: {"?": 15000}   # somebody holds the card, we cannot say who
        fresh.restore(saved)
        self.assertEqual(fresh.state()["holder"]["activity"], "research")

        empty = self.vramble.Arbiter()
        empty.vram_readable = lambda: True
        empty.vram_holders = lambda: {}            # nothing on the card: the run is gone
        empty.restore(saved)
        self.assertIsNone(empty.state()["holder"])


    def test_on_a_machine_without_nvidia_smi_the_lease_is_kept(self):
        """Not being able to measure is not evidence that nobody is there. Dropping the lease at
        every restart on such a machine would make a held card look free."""
        code, _ = self.arb.acquire("llm", note="model", ttl=600)
        self.assertEqual(code, 200)
        saved = self.arb.snapshot()
        fresh = self.vramble.Arbiter()
        fresh.vram_readable = lambda: False        # no nvidia-smi here
        fresh.vram_holders = lambda: {}
        fresh.restore(saved)
        self.assertEqual(fresh.state()["holder"]["activity"], "llm")


    def _fake_cards(self, total, used):
        """Replace the two readers: the code invalidates its own caches before measuring."""
        self.vramble.vram_total = lambda: [total]
        self.vramble.vram = lambda: [used]
        self.addCleanup(setattr, self.vramble, "vram_total", self.vramble.vram_total)

    def test_a_small_request_is_refused_too_if_the_card_was_not_freed(self):
        """The check used to run only for requests wanting the whole card: a 6 GB one landing on
        15 GB that were never freed is the same out-of-memory."""
        real_total, real_vram = self.vramble.vram_total, self.vramble.vram
        try:
            self.vramble.vram_total = lambda: [16000]
            self.vramble.vram = lambda: [15500]          # the card stays full after the drains
            self.arb.vram_holders = lambda: {"comfy": 15500}
            code, r = self.arb.acquire("maintenance", note="batch", ttl=60)   # only 8 GB wanted
            self.assertEqual(code, 409, "granting here means two workloads on one card")
            self.assertIn("comfy", str(r.get("still_holding")))
        finally:
            self.vramble.vram_total, self.vramble.vram = real_total, real_vram

    def test_unattributed_vram_counts_as_occupied(self):
        """Not being able to say whose those 15 GB are does not make them free — and an activity
        without a `match` rule is always unattributed."""
        real_total, real_vram = self.vramble.vram_total, self.vramble.vram
        try:
            self.vramble.vram_total = lambda: [16000]
            self.vramble.vram = lambda: [15500]
            self.arb.vram_holders = lambda: {"?": 15500}
            code, _ = self.arb.acquire("comfy", note="image", ttl=60)
            self.assertEqual(code, 409)
        finally:
            self.vramble.vram_total, self.vramble.vram = real_total, real_vram

    def test_an_unmeasurable_machine_is_not_vetoed(self):
        """Where the VRAM cannot be read at all the check has nothing to say and must not block."""
        real_total = self.vramble.vram_total
        try:
            self.vramble.vram_total = lambda: []
            code, _ = self.arb.acquire("comfy", note="image", ttl=60)
            self.assertEqual(code, 200)
        finally:
            self.vramble.vram_total = real_total

class Placeholders(VrambleTest):
    """Registry commands contain JSON: resolving placeholders must not break the braces."""

    def test_placeholders_resolve_without_breaking_json(self):
        cmd = ("curl -s -X POST {comfy_url}/free -H 'Content-Type: application/json' "
               '-d \'{"unload_models":true,"free_memory":true}\'')
        r = self.vramble.resolve(cmd)
        self.assertIn("http://127.0.0.1:8188/free", r, "the placeholder must be substituted")
        self.assertIn('{"unload_models":true,"free_memory":true}', r,
                      "the command's JSON must remain intact")
        self.assertNotIn("{comfy_url}", r)

    def test_command_without_placeholders_is_unchanged(self):
        cmd = "systemctl --user stop comfy"
        self.assertEqual(self.vramble.resolve(cmd), cmd)

    def test_failed_drain_is_visible_in_the_log(self):
        self.arb.reg["activities"]["comfy"]["drain_soft"] = "false"   # command that fails
        lines = []
        self.vramble.log = lambda m: lines.append(m)
        self.arb.drain("comfy", hard=False)
        self.assertTrue(any("FAILED" in r for r in lines),
                        "a failed drain must not go unnoticed")



    def test_a_restart_does_not_make_a_held_machine_look_free(self):
        """The holder lives in memory: without persistence, restarting the daemon while a model
        holds 15 GB means the arbiter hands the card to somebody else."""
        code, lease = self.arb.acquire("llm", note="model", ttl=600)
        self.assertEqual(code, 200)
        saved = self.arb.snapshot()
        self.assertEqual(saved["activity"], "llm")
        self.assertEqual(saved["token"], lease["token"])

        fresh = self.vramble.Arbiter()                      # as if vramble had just started
        self.assertIsNone(fresh.state()["holder"])
        fresh.vram_readable = lambda: True
        fresh.vram_holders = lambda: {"llm": 15000}      # the card says llm is still there
        fresh.restore(saved)
        self.assertEqual(fresh.state()["holder"]["activity"], "llm")
        self.assertEqual(fresh.state()["holder"]["note"], "model")

        gone = self.vramble.Arbiter()                       # same save, but the VRAM is free now
        gone.vram_readable = lambda: True
        gone.vram_holders = lambda: {}
        gone.restore(saved)
        self.assertIsNone(gone.state()["holder"], "a lease nobody is holding must not come back")

    def test_an_expired_saved_lease_is_not_restored(self):
        code, _ = self.arb.acquire("llm", note="model", ttl=600)
        self.assertEqual(code, 200)
        saved = self.arb.snapshot()
        saved["expires_at"] = 0                          # it expired while the daemon was down
        fresh = self.vramble.Arbiter()
        fresh.vram_readable = lambda: True
        fresh.vram_holders = lambda: {"llm": 15000}
        fresh.restore(saved)
        self.assertIsNone(fresh.state()["holder"])

if __name__ == "__main__":
    unittest.main(verbosity=2)
