"""Test harness: a fake vramble, with no GPU, no network and no real services.

Registry and catalog are written to temporary files; drain/start commands are
`echo` and `true`, so the tests measure the LOGIC (who holds the slot, who waits, who yields),
not the hardware. No test touches the real machine.
"""
import os
import sys
import tempfile
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

REGISTRY = """
global:
  whole_card_threshold_gb: 15.0
  heartbeat_s: 30

activities:
  research:
    description: "a long run, never yields"
    priority: 90
    preemptible: false
    vram_gb: 16
    default_wait: 0
    default_ttl: 600
    idle_ttl: 0
  agent:
    description: "an agent run"
    priority: 60
    preemptible: false
    vram_gb: 15.7
    default_wait: 0
    default_ttl: 600
    idle_ttl: 0
    compatible_with: [llm]
  llm:
    description: "models"
    priority: 50
    preemptible: true
    vram_gb: 15.7
    vram_gb_by_note:
      soft: 14.0
    default_wait: 5
    default_ttl: 300
    idle_ttl: 0
    drain_soft: "true"
    drain_hard: "true"
    healthcheck: "true"
  comfy:
    description: "images"
    priority: 50
    preemptible: true
    vram_gb: 15.5
    default_wait: 5
    default_ttl: 300
    idle_ttl: 0
    start: "true"
    healthcheck: "true"
    interrupt: "true"
    drain_soft: "true"
    drain_hard: "true"
  cpu:
    description: "work that needs no GPU"
    priority: 0
    preemptible: true
    vram_gb: 0
    default_wait: 0
    default_ttl: 60
    idle_ttl: 0
  maintenance:
    description: "batch work"
    priority: 10
    preemptible: true
    vram_gb: 8
    default_wait: 0
    default_ttl: 300
    idle_ttl: 0
"""

CATALOG = """
macros:
  PY: /usr/bin/python3

services:
  echo:
    description: "test"
    time_hint: "1 s"
    activity: comfy
    command: ["/bin/echo"]
    required: [prompt]
    limits: {elapsed: [1, 20]}
    flags: {prompt: "--prompt", elapsed: "--dur"}
  voice:
    description: "test with a conditional activity"
    time_hint: "1 s"
    activity: llm
    activity_if: {engine: {kokoro: cpu}}
    command: ["/bin/echo"]
    required: [text]
    flags: {text: "--text", engine: "--engine"}
"""


class VrambleTest(unittest.TestCase):
    """Every test starts from a clean vramble, with its own registry and catalog."""

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        d = self.dir.name
        self.reg = os.path.join(d, "activity.yaml")
        self.cat = os.path.join(d, "services.yaml")
        open(self.reg, "w").write(REGISTRY)
        open(self.cat, "w").write(CATALOG)
        # VRAMBLE_BASE too: without it the job logs would land in the real ~/vramble of whoever
        # runs the tests, outside the temporary directory.
        os.environ.update(VRAMBLE_BASE=d, VRAMBLE_REGISTRY=self.reg, VRAMBLE_CATALOG=self.cat,
                          VRAMBLE_DB=os.path.join(d, "jobs.db"), VRAMBLE_STATE_FILE="")
        for m in ("config", "vramble", "jobs", "catalog"):
            sys.modules.pop(m, None)
        import vramble as _vramble
        import jobs as _jobs
        import catalog as _catalog
        self.vramble = _vramble
        self.arb = _vramble.Arbiter()
        self.lines = []
        self.waiting = _jobs.Queue(self.arb, self.lines.append)
        self.catalog = _catalog.Catalog()

    def tearDown(self):
        self.waiting.stopped = True      # no worker survives its own test
        time.sleep(0.6)
        self.dir.cleanup()

    # readable shortcuts
    def take(self, activity, **kw):
        return self.arb.acquire(activity, **kw)

    def holder(self):
        s = self.arb.state()["holder"]
        return s["activity"] if s else None
