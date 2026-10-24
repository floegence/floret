import copy
import importlib.util
import json
import pathlib
import sys
import unittest

sys.dont_write_bytecode = True

ROOT = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('catalog_generator', ROOT / 'generate.py')
generator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generator)


class ScopedRefreshTest(unittest.TestCase):
    def test_refresh_preserves_other_providers_and_provenance(self):
        previous = {'source': 'old', 'sha256': 'old-hash', 'providers': {
            'moonshot': {'old': {}}, 'deepseek': {'existing': {'id': 'existing'}},
        }, 'provider_sources': {'deepseek': {'source': 'prior', 'sha256': 'prior-hash'}}}
        incoming = {'source': 'new', 'sha256': 'new-hash', 'providers': {
            'moonshot': {'new': {'id': 'new'}}, 'deepseek': {'unreviewed': {}},
        }}
        frozen = copy.deepcopy(previous)
        result = generator.refresh_providers(previous, incoming, ['moonshot'])
        self.assertEqual(previous, frozen)
        self.assertEqual(result['providers']['deepseek'], previous['providers']['deepseek'])
        self.assertEqual(result['provider_sources']['deepseek'], previous['provider_sources']['deepseek'])
        self.assertEqual(result['sha256'], 'old-hash')
        self.assertEqual(result['provider_sources']['moonshot'], {'source': 'new', 'sha256': 'new-hash'})
        result['providers']['moonshot']['new']['id'] = 'changed'
        self.assertEqual(incoming['providers']['moonshot']['new']['id'], 'new')

    def test_review_date_is_provider_scoped(self):
        rules = json.loads((ROOT / 'overrides.json').read_text())
        data = json.loads((ROOT / 'upstream.json').read_text())
        result = generator.generate(data, rules)
        self.assertEqual(result['provider_sources'], data['provider_sources'])
        for pid, models in result['providers'].items():
            for model in models:
                cap = model['reasoning']
                if not cap:
                    continue
                override = rules.get('models', {}).get(pid + '/' + model['id'], {}).get('reasoning', {})
                expected = override.get('source_checked_at', rules.get('provider_checked_at', {}).get(pid, rules['checked_at']))
                self.assertEqual(cap['source_checked_at'], expected, pid + '/' + model['id'])


if __name__ == '__main__':
    unittest.main()
