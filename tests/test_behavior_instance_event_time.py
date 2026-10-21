"""Behavior events must label UTC instants correctly on non-UTC hosts."""

from datetime import datetime, timedelta, timezone
import importlib.util
import logging
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        instant = datetime(2026, 6, 1, 10, 20, 30, tzinfo=timezone.utc)
        if tz is None:
            return instant.astimezone(timezone(timedelta(hours=2))).replace(tzinfo=None)
        return instant.astimezone(tz)


class BehaviorEventTimeTests(unittest.TestCase):
    def setUp(self):
        self.events = []
        hue_objects = types.ModuleType("HueObjects")
        hue_objects.genV2Uuid = lambda: "test-behavior"
        hue_objects.StreamEvent = self.events.append
        log_manager = types.ModuleType("logManager")
        log_manager.logger = types.SimpleNamespace(get_logger=logging.getLogger)
        path = Path(__file__).resolve().parents[1] / "BridgeEmulator/HueObjects/BehaviorInstance.py"
        spec = importlib.util.spec_from_file_location("behavior_event_time", path)
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"HueObjects": hue_objects, "logManager": log_manager}):
            spec.loader.exec_module(self.module)
        self.module.datetime = FixedDatetime
        self.instance = self.module.BehaviorInstance({
            "metadata": {"name": "Test behavior"},
            "configuration": {},
        })

    def test_add_event_uses_utc(self):
        self.assertEqual(self.events[0]["creationtime"], "2026-06-01T10:20:30Z")

    def test_update_event_uses_utc(self):
        self.events.clear()
        self.instance.update_attr({"enabled": True})
        self.assertEqual(self.events[0]["creationtime"], "2026-06-01T10:20:30Z")

    def test_delete_event_uses_utc(self):
        self.events.clear()
        self.instance.__del__()
        self.assertEqual(self.events[0]["creationtime"], "2026-06-01T10:20:30Z")


if __name__ == "__main__":
    unittest.main()
