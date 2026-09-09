#!/usr/bin/env python3
"""Generate the offline engine catalog. Network access requires --update."""
import argparse
import hashlib
import json
import pathlib
import re
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
DATA = ROOT / 'scripts/model-catalog'
OUTPUT = ROOT / 'internal/provider/catalog/models.generated.json'
SOURCES = {'openai': 'openai', 'anthropic': 'anthropic', 'google': 'google',
           'moonshot': 'moonshotai-cn', 'chatglm': 'zai', 'deepseek': 'deepseek',
           'qwen': 'alibaba', 'xai': 'xai', 'groq': 'groq'}
FIELDS = ('id', 'name', 'status', 'release_date', 'tool_call', 'modalities',
          'limit', 'cost', 'reasoning', 'reasoning_options')


def snapshot(raw):
    upstream = json.loads(raw)
    return {'source': 'https://models.dev/api.json',
            'sha256': hashlib.sha256(raw).hexdigest(),
            'providers': {local: {mid: {key: value for key, value in model.items() if key in FIELDS}
                                  for mid, model in upstream[source]['models'].items()}
                          for local, source in SOURCES.items()}}


def reasoning(pid, model, rules):
    if not model.get('reasoning'):
        return {}
    options = model.get('reasoning_options', [])
    types = {o['type'] for o in options}
    if types - {'effort', 'toggle', 'budget_tokens'}:
        raise ValueError(f"{pid}/{model['id']}: review new reasoning options {types}")
    effort = next((o.get('values', []) for o in options if o['type'] == 'effort'), [])
    budget = next((o for o in options if o['type'] == 'budget_tokens'), None)
    levels = ['off' if v == 'none' else v for v in effort if v != 'default']
    cap = {'kind': 'always_on', 'source_checked_at': rules['checked_at'],
           'source_urls': rules['sources'][pid]}
    if effort:
        cap.update(kind='effort', supported_levels=levels)
    if 'toggle' in types or 'off' in levels:
        cap['disable_supported'] = True
    if budget is not None:
        cap['kind'] = 'effort_budget' if effort else ('toggle_budget' if 'toggle' in types else 'budget')
        cap['budget'] = {k + '_tokens': budget[k] for k in ['min', 'max'] if k in budget}
    elif 'toggle' in types and not effort:
        cap['kind'] = 'toggle'
    if pid in ['openai', 'google', 'groq', 'xai']:
        cap['wire_shape'] = 'openai_chat_reasoning_effort'
        if pid == 'google' and budget is not None:
            cap.update(wire_shape='gemini_openai_thinking_budget', budget_shape='gemini_thinking_budget')
    elif pid == 'anthropic':
        cap['wire_shape'] = 'anthropic_output_config_effort' if effort else 'anthropic_thinking_budget_tokens'
        if budget is not None:
            cap.update(budget_shape='anthropic_thinking_budget_tokens', disable_supported=True)
    elif pid == 'moonshot':
        cap['wire_shape'] = 'kimi_reasoning_effort' if effort else 'kimi_thinking_type'
        cap['history_replay_requirements'] = ['reasoning_content']
    elif pid == 'chatglm':
        cap['wire_shape'] = 'glm_reasoning_effort' if effort else 'glm_thinking_type'
    elif pid == 'deepseek':
        cap.update(wire_shape='deepseek_responses_reasoning_effort', history_replay_requirements=['reasoning.content'])
    elif pid == 'qwen':
        cap['wire_shape'] = 'qwen_reasoning_effort' if effort else 'qwen_enable_thinking'
        if budget is not None:
            cap['budget_shape'] = 'qwen_thinking_budget'
    cap['fixture'] = cap['wire_shape']
    return cap


def generate(data, rules):
    result = {'source': data['source'], 'sha256': data['sha256'], 'checked_at': rules['checked_at'], 'providers': {}}
    for pid, models in data['providers'].items():
        output = []
        for mid, original in models.items():
            model = dict(original)
            model.update(rules.get('models', {}).get(pid + '/' + mid, {}).get('metadata', {}))
            if mid in rules.get('exclude', {}).get(pid, []):
                continue
            if re.search(rules['exclude_pattern'], mid) or model.get('status') == 'deprecated':
                continue
            if not model.get('tool_call') or model.get('modalities', {}).get('output') != ['text']:
                continue
            if pid == 'qwen' and not mid.startswith(('qwen', 'qwq', 'qvq')):
                continue
            if pid == 'google' and not mid.startswith('gemini-'):
                continue
            limits = model['limit']
            if limits.get('context', 0) <= 0 or limits.get('output', 0) <= 0:
                raise ValueError(f'{pid}/{mid}: missing token limits')
            cap = reasoning(pid, model, rules)
            cap.update(rules.get('models', {}).get(pid + '/' + mid, {}).get('reasoning', {}))
            status = model.get('status', '')
            if not status and re.search(r'preview|experimental|(?:^|-)exp(?:-|$)', mid):
                status = 'beta'
            cost = model.get('cost', {})
            output.append({'id': mid, 'name': model['name'], 'status': status,
                           'context_window': limits['context'], 'max_tokens': limits['output'],
                           'input': [x for x in model['modalities']['input'] if x in ['text', 'image']],
                           'reasoning': cap,
                           'cost': {dst: cost[src] for src, dst in [('input','input_per_mtok'),('output','output_per_mtok'),('cache_read','cache_read_per_mtok'),('cache_write','cache_write_per_mtok')] if src in cost},
                           'release_date': model.get('release_date', '')})
        output.sort(key=lambda m: (m['release_date'], m['id']), reverse=True)
        if not output:
            raise ValueError(f'{pid}: empty catalog')
        result['providers'][pid] = output
    return result


def encoded(value):
    return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--update', action='store_true')
    group.add_argument('--source', type=pathlib.Path, help='Explicit previously downloaded models.dev API response')
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    rules = json.loads((DATA / 'overrides.json').read_text())
    if args.update:
        request = urllib.request.Request('https://models.dev/api.json', headers={'User-Agent': 'Floret model catalog updater'})
        with urllib.request.urlopen(request, timeout=60) as response:
            data = snapshot(response.read())
    elif args.source:
        data = snapshot(args.source.read_bytes())
    else:
        data = json.loads((DATA / 'upstream.json').read_text())
    output = encoded(generate(data, rules))
    if args.check:
        if OUTPUT.read_text() != output:
            raise SystemExit('Catalog is stale. Run python3 scripts/model-catalog/generate.py')
    else:
        # Validate and render completely before replacing either file.
        (DATA / 'upstream.json').write_text(encoded(data))
        OUTPUT.write_text(output)
    print(f"Catalog: {sum(len(ms) for ms in data['providers'].values())} upstream records; SHA256 {data['sha256']}")


if __name__ == '__main__':
    main()
