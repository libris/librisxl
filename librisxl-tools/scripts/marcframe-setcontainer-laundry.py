#!/usr/bin/env python3
import re
from pathlib import Path

repodir = Path(__file__).parent.parent.parent

singleprops = set()
multiprops = set()
props = dict()

Counter = lambda: dict(single=0, multi=0)

with open(repodir / 'whelk-core/src/main/resources/ext/marcframe.json') as f:
    for line in f:
        for key, prop in re.findall(r'"(addLink|addProperty|link|property)":\s*"(\w+)"', line):
            counter = props.setdefault(prop, Counter())
            if key.startswith('add'):
                multiprops.add(prop)
                counter['multi'] += 1
            else:
                singleprops.add(prop)
                counter['single'] += 1

for prop in multiprops & singleprops:
    print(prop, props[prop])

for prop in sorted(multiprops):
    print('    "%s": {"@container": "@set"},' % prop)
