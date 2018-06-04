#!/usr/bin/env python3
import os
import json
from collections import defaultdict
import sys

args = sys.argv[1:]

pgargs = args[0] if args else 'whelk'

fields = defaultdict(lambda: defaultdict(list))

with os.popen(f'''
psql {pgargs} -tc "
select data->'@graph'->1->>'@type', data->'@graph'->0->'_marcUncompleted' from lddb where (data->'@graph'->0->>'_marcUncompleted') IS NOT NULL;
"
''') as pf:
    for line in pf:
        if not line.strip():
            continue
        rtype, uncompleted = line.split('|', 1)
        rtype = rtype.strip()
        for field in json.loads(uncompleted):
            unhandled = field.pop('_unhandled', [])
            items = list(field.items())
            assert len(items) == 1
            for k, v in items:
                fields[rtype][k] = sorted(set(fields[rtype][k] + unhandled))

print(json.dumps(fields, indent=2, sort_keys=True))
