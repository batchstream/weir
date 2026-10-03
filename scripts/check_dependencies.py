#!/usr/bin/env python3
"""Keep the Weir, SDK and public protocol module dependencies one-way."""
import json
import os
from pathlib import Path
import re
import subprocess


SERVER = 'github.com/batchstream/weir'
SDK = 'github.com/batchstream/weir-go'
PROTOCOL = 'github.com/batchstream/weir-protocol'
STABLE_VERSION = re.compile(r'v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)')


def objects(raw):
    decoder = json.JSONDecoder()
    result = []
    while raw.strip():
        item, end = decoder.raw_decode(raw.lstrip())
        result.append(item)
        raw = raw.lstrip()[end:]
    return result


def check_graph(raw):
    levels = {SERVER: 2, SDK: 1, PROTOCOL: 0}
    edges = {}
    for line in raw.splitlines():
        source, target = (item.split('@', 1)[0] for item in line.split())
        edges.setdefault(source, set()).add(target)
    for origin, level in levels.items():
        pending = list(edges.get(origin, ()))
        seen = set()
        while pending:
            target = pending.pop()
            if target in seen:
                continue
            seen.add(target)
            if target in levels and levels[target] >= level:
                raise ValueError('project module dependency points backwards: ' + origin + ' reaches ' + target)
            pending.extend(edges.get(target, ()))


def check_modules(items):
    for module in items:
        if module.get('Replace'):
            raise ValueError('module replacement hides the published dependency graph: ' + module['Path'])
        if module['Path'] in (SDK, PROTOCOL) and not STABLE_VERSION.fullmatch(module.get('Version', '')):
            raise ValueError('project dependency must use a stable release: ' + module['Path'])


def check_production(items):
    for package in items:
        module = package.get('Module', {}).get('Path')
        if module == SDK:
            raise ValueError('server binary imports the SDK: ' + package['ImportPath'])


def main():
    root = Path(__file__).resolve().parent.parent
    env = dict(os.environ, GOWORK='off')
    graph = subprocess.check_output(['go', 'mod', 'graph'], cwd=root, env=env, text=True, timeout=120)
    check_graph(graph)
    modules = subprocess.check_output(['go', 'list', '-m', '-json', 'all'], cwd=root, env=env, text=True, timeout=120)
    check_modules(objects(modules))
    packages = subprocess.check_output(['go', 'list', '-deps', '-json', './cmd/weir'], cwd=root, env=env, text=True, timeout=120)
    check_production(objects(packages))
    print('One-way project module graph and SDK-free server binary verified')


if __name__ == '__main__':
    main()
