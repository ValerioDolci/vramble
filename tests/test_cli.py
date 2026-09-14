"""The two CLIs, where the tests did not reach — which is exactly where the mismatches hid.
No real vramble: VRAMBLE_URL points at a closed port and we watch what the command does."""
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LEASE = os.path.join(ROOT, "gpu-lease")
DEAD_PORT = "http://127.0.0.1:9"


def run(argv, **env):
    e = dict(os.environ, VRAMBLE_URL=DEAD_PORT, GPU_LEASE_REQUIRE_WAIT="1", **env)
    return subprocess.run([sys.executable, LEASE] + argv, capture_output=True, text=True, env=e, timeout=60)


class Cli(unittest.TestCase):
    def setUp(self):
        self.witness = os.path.join(tempfile.mkdtemp(), "ran")

    def test_without_vramble_a_human_still_runs(self):
        """Fail-open: the arbiter must never be able to block someone working by hand."""
        p = run(["run", "comfy", "--note", "t", "--", "/usr/bin/touch", self.witness])
        self.assertEqual(p.returncode, 0)
        self.assertTrue(os.path.exists(self.witness), "the command had to run all the same")
        self.assertIn("without a lease", p.stderr)

    def test_without_vramble_a_service_refuses_to_start(self):
        """--require-lease: starting unarbitrated is how two models end up on the same card."""
        p = run(["run", "llm", "--note", "model", "--require-lease", "--", "/usr/bin/touch", self.witness])
        self.assertEqual(p.returncode, 76)
        self.assertFalse(os.path.exists(self.witness), "the service must NOT start without a lease")

    def test_options_are_parsed_and_the_command_survives(self):
        """The parser tells options from the command: a regression here takes llama-swap down."""
        p = run(["run", "comfy", "--note", "with spaces", "--ttl", "45", "--",
                 "/bin/sh", "-c", f"echo hello > {self.witness}"])
        self.assertEqual(p.returncode, 0)
        self.assertEqual(open(self.witness).read().strip(), "hello")


if __name__ == "__main__":
    unittest.main(verbosity=2)
