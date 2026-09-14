"""The catalog: turning «service + params» into a command, and what it refuses."""
import sys, os, unittest
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import VrambleTest


class Catalog(VrambleTest):

    def test_service_becomes_a_command(self):
        att, argv, note = self.catalog.build("echo", {"prompt": "a lighthouse"})
        self.assertEqual(att, "comfy")
        self.assertIn("--prompt", argv)
        self.assertIn("a lighthouse", argv)
        self.assertIn("a lighthouse", note)

    def test_unknown_param_is_rejected_with_the_list(self):
        with self.assertRaises(ValueError) as e:
            self.catalog.build("echo", {"prompt": "x", "colour": "blue"})
        self.assertIn("colour", str(e.exception))
        self.assertIn("prompt", str(e.exception), "the error must say what it accepts")

    def test_missing_required_param(self):
        with self.assertRaises(ValueError) as e:
            self.catalog.build("echo", {})
        self.assertIn("prompt", str(e.exception))

    def test_unknown_service_lists_the_real_ones(self):
        with self.assertRaises(ValueError) as e:
            self.catalog.build("teleport", {"prompt": "x"})
        self.assertIn("echo", str(e.exception))

    def test_numeric_limits(self):
        self.catalog.build("echo", {"prompt": "x", "elapsed": 5})       # inside
        with self.assertRaises(ValueError) as e:
            self.catalog.build("echo", {"prompt": "x", "elapsed": 600})
        self.assertIn("600", str(e.exception))
        with self.assertRaises(ValueError):
            self.catalog.build("echo", {"prompt": "x", "elapsed": "very"})

    def test_conditional_activity(self):
        att, _, _ = self.catalog.build("voice", {"text": "hello", "engine": "kokoro"})
        self.assertEqual(att, "cpu", "kokoro runs on CPU: it must not take the GPU")
        att, _, _ = self.catalog.build("voice", {"text": "hello"})
        self.assertEqual(att, "llm")

    def test_service_listing(self):
        e = self.catalog.catalog_listing()
        self.assertIn("echo", e)
        self.assertIn("prompt", e["echo"]["params"])
        self.assertEqual(e["echo"]["required"], ["prompt"])

    def test_catalog_hot_reloads(self):
        self.catalog.catalog_listing()
        text = open(self.cat).read().replace("  echo:", "  eco2:\n    description: new\n    activity: comfy\n    command: [\"/bin/echo\"]\n    required: []\n  echo:")
        open(self.cat, "w").write(text)
        os.utime(self.cat, (0, 9999999999))
        self.assertIn("eco2", self.catalog.catalog_listing(), "editing the catalog must not require a restart")



    def test_a_command_with_json_braces_survives(self):
        """`str.format` reads {"clear": true} as a placeholder and destroys the command. The registry
        was fixed for this months ago; the catalog had the same trap."""
        testo = ('macros:\n  PY: /usr/bin/python3\nservices:\n  jsonny:\n    activity: comfy\n'
                 '    command: ["{PY}", "-c", "print(1)", "--data", \'{"clear": true}\']\n'
                 '    required: []\n')
        open(self.cat, "w").write(testo)
        import os as _os
        _os.utime(self.cat, (0, 9999999999))
        _att, argv, _note = self.catalog.build("jsonny", {})
        self.assertIn('{"clear": true}', argv, "the JSON must reach the command untouched")
        self.assertIn("/usr/bin/python3", argv, "and the real placeholder must still be expanded")

class Validator(VrambleTest):
    """check_catalog.py is the guard against silent failures: it deserves tests of its own."""

    def _run(self, text):
        import subprocess, sys, tempfile, os
        f = os.path.join(tempfile.mkdtemp(), "services.yaml")
        open(f, "w").write(text)
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        e = dict(os.environ, VRAMBLE_REGISTRY=self.reg)
        p = subprocess.run([sys.executable, os.path.join(root, "check_catalog.py"), f],
                           capture_output=True, text=True, env=e, timeout=60)
        return p.returncode, p.stdout

    def test_unknown_key_is_refused(self):
        """A misspelled key raises nothing: the service would quietly fall back to the default activity."""
        rc, out = self._run("services:\n  x:\n    activities: comfy\n    command: ['/bin/echo']\n")
        self.assertEqual(rc, 1)
        self.assertIn("unknown key 'activities'", out)

    def test_activity_outside_the_registry_is_refused(self):
        rc, out = self._run("services:\n  x:\n    activity: ghost\n    command: ['/bin/echo']\n")
        self.assertEqual(rc, 1)
        self.assertIn("not in the registry", out)

    def test_a_sound_catalog_passes(self):
        rc, out = self._run("services:\n  x:\n    activity: comfy\n    command: ['/bin/echo']\n")
        self.assertEqual(rc, 0, out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
