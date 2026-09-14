#!/usr/bin/env python3
"""Run every test: `python3 tests/all.py` (add `-v` for detail).

No test touches the GPU, the network or the machine's files: registry, catalog and database are
temporary, and the commands are `true`/`echo`.
"""
import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))

if __name__ == "__main__":
    suite = unittest.TestLoader().discover(HERE, pattern="test_*.py")
    v = 2 if "-v" in sys.argv else 1
    outcome = unittest.TextTestRunner(verbosity=v).run(suite)
    sys.exit(0 if outcome.wasSuccessful() else 1)
