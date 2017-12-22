#!/usr/bin/env python3
import json

import sys

mf_path = sys.argv[1]

print(mf_path)

with open(mf_path) as f:
    mf = json.load(f)

for name, pattern in mf['patterns'].items():
    includes = pattern.get('include', [])
    if includes:
        print(name, includes)
    for match in pattern.get('match', []):
        matchincludes = match.get('include', [])
        if matchincludes:
            print(f'{name}[{match["when"]}]', matchincludes)
        for submatch in match.get('match', []):
            assert 'match' not in submatch, "Too nested match"
            submatchincludes = submatch.get('include', [])
            if submatchincludes:
                print(f'{name}[{match["when"]}][{submatch["when"]}]', submatchincludes)
