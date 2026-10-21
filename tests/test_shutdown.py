"""The process must drain producers before saving state and exiting."""

import ast
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch


class ShutdownTests(unittest.TestCase):
    def test_producers_stop_once_before_persistence_and_exit(self):
        source = Path(__file__).resolve().parents[1] / 'BridgeEmulator/HueEmulator3.py'
        tree = ast.parse(source.read_text())
        shutdown = next(node for node in ast.walk(tree)
                        if isinstance(node, ast.FunctionDef) and node.name == 'shutdown')
        phases = []
        modules = {name: types.ModuleType(name) for name in
                   ('lights', 'lights.protocols', 'lights.protocols.mqtt')}
        modules['lights'].__path__ = []
        modules['lights.protocols'].__path__ = []
        modules['lights.protocols.mqtt'].shutdown_mqtt_publishers = lambda: phases.append('drain')

        def save_config(backup, blocking):
            self.assertTrue(blocking)
            phases.append('backup' if backup else 'save')

        def exit_process(code):
            self.assertEqual(code, 0)
            phases.append('exit')
            raise SystemExit(code)

        namespace = {
            'bridgeConfig': {'groups': {}},
            'logging': Mock(),
            'configManager': types.SimpleNamespace(bridgeConfig=types.SimpleNamespace(save_config=save_config)),
            'os': types.SimpleNamespace(_exit=exit_process),
        }
        exec(compile(ast.Module(body=[shutdown], type_ignores=[]), str(source), 'exec'), namespace)
        with patch.dict(sys.modules, modules), self.assertRaises(SystemExit):
            namespace['shutdown'](15, None)
        self.assertEqual(phases, ['drain', 'save', 'backup', 'exit'])


if __name__ == '__main__':
    unittest.main()
