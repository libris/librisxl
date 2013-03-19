# -*- coding: utf-8 -*-
import re
from collections import OrderedDict
import json


def camelize(data, source, dest=None):
    dest = dest or source
    for o in data.values():
        if source not in o: continue
        v = o[source]
        o[dest] = v[0].lower() + re.sub(r"\W", "", v.title()[1:])
    return data

def sort_keys(o):
    out = OrderedDict()
    for key in sorted(o.keys()):
        out[key] = o[key]
    return out

def to_list(o, keykey='key'):
    l = []
    for k, v in o.items():
        if isinstance(v, dict) and keykey not in v:
            v2 = OrderedDict()
            v2[keykey] = k
            v2.update(v)
            l.append(v2)
        else:
            l.append(v)
    return l

def to_dict(data, keykey, keep=None):
    return OrderedDict(
            (v[keykey] if keep == 'keep' else v.pop(keykey), v)
            for v in data)

def objectify(data, key):
    for k, v in data.items():
        data[k] = {key: v}
    return data

if __name__ == '__main__':
    import sys

    tr = vars()[sys.argv[1]]

    data = json.load(sys.stdin, object_pairs_hook=OrderedDict)
    data = tr(data, *sys.argv[2:])

    print json.dumps(data,
            indent=2,
            ensure_ascii=False,
            separators=(',', ': ')
            ).encode('utf-8')
