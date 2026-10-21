"""Local publishing regressions; no broker, discovery or light commands."""

import importlib.util
import logging
from pathlib import Path
import sys
import threading
import types
import unittest
from unittest.mock import Mock, patch


class MqttPublisherTests(unittest.TestCase):
    def setUp(self):
        modules = {name: types.ModuleType(name) for name in (
            "logManager", "paho", "paho.mqtt", "paho.mqtt.publish",
            "paho.mqtt.client", "functions", "functions.colors",
        )}
        modules["logManager"].logger = types.SimpleNamespace(get_logger=logging.getLogger)
        modules["paho"].__path__ = []
        modules["paho.mqtt"].__path__ = []
        modules["paho.mqtt.client"].MQTT_ERR_SUCCESS = 0
        modules["functions"].__path__ = []
        modules["functions.colors"].hsv_to_rgb = Mock()
        modules["functions.colors"].convert_xy = Mock()
        path = Path(__file__).resolve().parents[1] / "BridgeEmulator/lights/protocols/mqtt.py"
        spec = importlib.util.spec_from_file_location("local_mqtt_publisher_test", path)
        self.module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules), patch("atexit.register"):
            spec.loader.exec_module(self.module)
        self.cfg = {"mqttServer": "test.invalid", "mqttPort": 1883,
                    "mqttUser": "", "mqttPassword": ""}
        self.publisher = self.module._MqttPublisher(self.cfg)
        self.client = Mock()
        self.client.is_connected.return_value = True
        self.publisher.client = self.client

    def test_failed_publish_is_not_retried_or_queued(self):
        self.client.publish.return_value = types.SimpleNamespace(rc=4)
        with self.assertRaises(ConnectionError):
            self.publisher.publish([{"topic": "test/set", "payload": "OFF"}])
        self.assertEqual(self.client.publish.call_count, 1)
        self.assertIsNone(self.publisher.client)
        self.client.disconnect.assert_called_once()
        self.client.loop_stop.assert_called_once()

    def test_incomplete_publish_discards_connection(self):
        self.client.publish.return_value = types.SimpleNamespace(
            rc=0, wait_for_publish=Mock(), is_published=lambda: False)
        with self.assertRaises(TimeoutError):
            self.publisher.publish([{"topic": "test/set", "payload": "OFF"}])
        self.assertIsNone(self.publisher.client)

    def test_cancelled_animation_does_not_publish(self):
        event = threading.Event()
        event.set()
        self.assertFalse(self.publisher.publish([{"topic": "test/set"}], event))
        self.client.publish.assert_not_called()

    def test_command_batch_keeps_order_and_every_command(self):
        self.client.publish.return_value = types.SimpleNamespace(
            rc=0, wait_for_publish=Mock(), is_published=lambda: True)
        messages = [{"topic": "test/set", "payload": str(i)} for i in range(32)]
        self.assertTrue(self.publisher.publish(messages))
        self.assertEqual([c.kwargs for c in self.client.publish.call_args_list], messages)

    def test_distinct_credentials_use_distinct_publishers(self):
        first = self.module._get_mqtt_publisher(self.cfg)
        second = self.module._get_mqtt_publisher(dict(self.cfg, mqttUser="another-user"))
        self.assertIsNot(first, second)
        self.assertIs(first, self.module._get_mqtt_publisher(dict(self.cfg)))

    def test_closed_publisher_rejects_new_commands(self):
        self.publisher.close()
        with self.assertRaises(RuntimeError):
            self.publisher.publish([{"topic": "test/set", "payload": "ON"}])
        self.client.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
