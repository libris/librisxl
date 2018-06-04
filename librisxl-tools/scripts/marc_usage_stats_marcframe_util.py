import json
from sys import argv
from pathlib import Path

mf_path = argv[1] if len(argv) > 1 else str(Path(__file__).parent /
        '../../whelk-core/src/main/resources/ext/marcframe.json')

with open(mf_path) as f:
    mf = json.load(f)

def cleanrange(s):
    assert s[0] == '[' and s[-1] == ']'
    s = s[1:-1]
    if ':' in s:
        start, stop = map(int, s.split(':'))
        if stop == start + 1:
            s = str(start)
    return f'[{s}]'.replace(']', '],')

f008 = mf['bib']['008']
for k, v in f008.items():
    if k.startswith('['):
        print(cleanrange(k))
    if k[0].isupper() and isinstance(v, dict):
        print(k)
        for subk, subv in v.items():
            if subk.startswith('['):
                print(' ' * 11, cleanrange(subk),
                        '#', subv.get('addLink') or subv.get('link'))
        print()
