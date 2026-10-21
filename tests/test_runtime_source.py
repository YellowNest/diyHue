"""Ensure the image source preserves every verified production Python file."""

import hashlib
import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class RuntimeSourceTests(unittest.TestCase):
    def test_verified_runtime_source(self):
        expected = json.loads((ROOT / "runtime-source-sha256.json").read_text())
        source = ROOT / "BridgeEmulator"
        for name, digest in expected.items():
            with self.subTest(module=name):
                self.assertEqual(hashlib.sha256((source / name).read_bytes()).hexdigest(), digest)


if __name__ == "__main__":
    unittest.main()
