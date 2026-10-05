"""A second session of a `reentrant: false` activity waits; what runs inside the lease re-enters.

The bug (05/10): the lease was reentrant for every activity, so a second `gpu-lease run research`
from another agent was let in while the first one held the cards — and went out of memory.
"""
import json, os, subprocess, sys, tempfile, threading, time, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import VrambleTest, ROOT

LEASE = os.path.join(ROOT, "gpu-lease")


class Arbiter(VrambleTest):

    def test_a_second_session_waits_for_the_first(self):
        c, a = self.take("research", note="A")
        self.assertEqual(c, 200)
        c, r = self.take("research", note="B")
        self.assertEqual(c, 409, "a second research session must not be let in beside the first")
        self.assertEqual(r["holder"]["note"], "A")
        self.assertTrue(r.get("not_reentrant"), "the refusal must say why")
        self.assertEqual(r["holder"]["refs"], 1, "nobody joined the lease")
        self.arb.release_lease(a["token"])
        c, b = self.take("research", note="B")
        self.assertEqual(c, 200, "once the first has finished, the second goes in")

    def test_a_waiting_second_session_is_queued_not_granted(self):
        self.take("research", note="A")
        c, r = self.take("research", note="B", wait=600, wait_id="pid@host")
        self.assertEqual(c, 409)
        self.assertTrue(r["queued"], "with a wait it holds a place in the line")

    def test_work_started_inside_the_lease_reenters(self):
        _, a = self.take("research", note="A")
        c, n = self.take("research", note="A step", parent=a["token"])
        self.assertEqual(c, 200, "a command started inside the lease is part of it")
        self.assertTrue(n.get("reentrant"))
        self.assertNotEqual(n["token"], a["token"], "its own token, as for every re-entry")
        c, nn = self.take("research", note="A sub-step", parent=n["token"])
        self.assertEqual(c, 200, "nesting nests: the re-entered token is good as a parent too")
        self.arb.release_lease(nn["token"])
        self.arb.release_lease(n["token"])
        self.assertEqual(self.holder(), "research", "the nested releases leave the owner in place")

    def test_a_token_that_is_not_this_lease_does_not_open_the_door(self):
        _, old = self.take("research", note="old")
        self.arb.release_lease(old["token"])
        self.take("research", note="A")
        for parent in (old["token"], "research-123", "", None, ["list"], 7):
            c, _ = self.take("research", note="B", parent=parent)
            self.assertEqual(c, 409, f"parent={parent!r} must not let a second session in")

    def test_a_shared_server_activity_stays_reentrant(self):
        """llm is one llama-swap: every chat request through the queue re-enters the model's lease."""
        self.take("llm", note="qwen27b")
        c, _ = self.take("llm", note="qwen27b", internal=False)
        self.assertEqual(c, 200, "the default must not change: reentrant unless declared otherwise")

    def test_the_refusal_is_logged_once_per_session_not_per_poll(self):
        lines = []
        self.vramble.log = lines.append
        self.take("research", note="A")
        for _ in range(5):                            # the CLI polls every 3 s while it waits
            self.take("research", note="B", wait=600, wait_id="pid@host")
        said = [l for l in lines if "not reentrant" in l]
        self.assertEqual(len(said), 1, said)
        self.assertIn("research (B) kept out: research (A)", said[0])

    def test_a_queued_job_hands_its_lease_to_its_command(self):
        """The worker does what `gpu-lease run` does: a job's own gpu-lease calls are inside it."""
        j = self.waiting.add("research", "job", "tests", ["/bin/sh", "-c", 'echo "lease=$VRAMBLE_LEASE"'])
        self.waiting._run(self.waiting._row(
            self.waiting.db.execute("SELECT * FROM job WHERE id=?", (j["id"],)).fetchone()))
        done = self.waiting.job_state(j["id"])
        self.assertEqual(done["state"], "done")
        self.assertIn("lease=research-", done["output"])


class Cli(VrambleTest):
    """The real gpu-lease against a real HTTP vramble: exit codes and the VRAMBLE_LEASE hand-off."""

    def setUp(self):
        super().setUp()
        from http.server import ThreadingHTTPServer
        g = self.vramble
        g.ARB, g.QUEUE, g.CATALOG = self.arb, self.waiting, self.catalog
        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), g.H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.env = dict(os.environ, VRAMBLE_URL=f"http://127.0.0.1:{self.srv.server_address[1]}",
                        VRAMBLE_SOCKET=os.path.join(self.dir.name, "absent.sock"))
        self.env.pop("VRAMBLE_LEASE", None)

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()
        super().tearDown()

    def run_cli(self, *argv, env=None):
        return subprocess.Popen([sys.executable, LEASE] + list(argv), env=env or self.env,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def test_two_parallel_sessions_the_second_one_exits_75(self):
        witness = os.path.join(self.dir.name, "second-ran")
        first = self.run_cli("run", "research", "--note", "A", "--ttl", "60", "--", "sleep", "4")
        for _ in range(50):
            if self.holder() == "research":
                break
            time.sleep(0.1)
        self.assertEqual(self.holder(), "research")
        second = self.run_cli("run", "research", "--note", "B", "--wait", "1", "--",
                              "/usr/bin/touch", witness)
        out, err = second.communicate(timeout=30)
        self.assertEqual(second.returncode, 75, err)
        self.assertFalse(os.path.exists(witness), "the second session must not have started")
        self.assertIn("not reentrant", err)
        first.communicate(timeout=30)
        self.assertEqual(first.returncode, 0)

    def test_the_command_inherits_the_token_and_its_own_gpu_lease_reenters(self):
        script = os.path.join(self.dir.name, "inner.sh")
        mark = os.path.join(self.dir.name, "inner-ran")
        with open(script, "w") as f:
            f.write(f'#!/bin/sh\ntest -n "$VRAMBLE_LEASE" || exit 3\n'
                    f'exec {sys.executable} {LEASE} run research --note inner --wait 0 -- '
                    f'/usr/bin/touch {mark}\n')
        p = self.run_cli("run", "research", "--note", "outer", "--ttl", "60", "--", "sh", script)
        out, err = p.communicate(timeout=30)
        self.assertEqual(p.returncode, 0, err)
        self.assertTrue(os.path.exists(mark), "the gpu-lease inside the lease had to re-enter it")
        self.assertIsNone(self.holder(), "and both leases are gone at the end")


if __name__ == "__main__":
    unittest.main(verbosity=2)
