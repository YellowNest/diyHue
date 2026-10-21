"""The synthetic all-lights group's public identity must survive reloads."""

import ast
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
import uuid
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1] / 'BridgeEmulator'


class GroupZeroIdentityTests(unittest.TestCase):
    def setUp(self):
        logger = types.SimpleNamespace(logger=types.SimpleNamespace(get_logger=lambda _: Mock()))
        with patch.dict(sys.modules, {'logManager': logger}):
            spec = importlib.util.spec_from_file_location(
                'HueObjects', ROOT / 'HueObjects/__init__.py',
                submodule_search_locations=[str(ROOT / 'HueObjects')])
            hue = importlib.util.module_from_spec(spec)
            sys.modules['HueObjects'] = hue
            spec.loader.exec_module(hue)
            spec = importlib.util.spec_from_file_location('identity_test_group', ROOT / 'HueObjects/Group.py')
            self.groups = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(self.groups)
        path = ROOT / 'configManager/configHandler.py'
        tree = ast.parse(path.read_text())
        nodes = [node for node in ast.walk(tree) if isinstance(node, ast.Assign)
                 and ast.unparse(node.targets[0]) == "self.yaml_config['groups']['0']"]
        self.assertEqual(len(nodes), 1)
        self.create = compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec')

    def load_zero(self, config):
        loader = types.SimpleNamespace(yaml_config={'config': config, 'groups': {}})
        exec(self.create, {'self': loader, 'Group': self.groups, 'uuid': uuid})
        return loader.yaml_config['groups']['0']

    def test_identity_survives_saved_config_reload(self):
        config = {}
        first = self.load_zero(config)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'config.json'
            path.write_text(json.dumps(config))
            second = self.load_zero(json.loads(path.read_text()))
        self.assertEqual(first.id_v2, second.id_v2)
        self.assertEqual(config['group_zero_id_v2'], first.id_v2)

    def test_existing_identity_is_reused(self):
        expected = '11111111-2222-4333-8444-555555555555'
        group = self.load_zero({'group_zero_id_v2': expected})
        self.assertEqual(group.id_v2, expected)


if __name__ == '__main__':
    unittest.main()
