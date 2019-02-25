from __future__ import print_function, unicode_literals
import json
import sys


MAX_STATS = 20

def compute_shape(node, index):
    if len(node) == 1 and '@id' in node:
        count_value('@id', node['@id'], index)
        return

    rtype = node.get('@type')
    shape = index.setdefault(rtype, {})

    for k, vs in node.items():
        if not isinstance(vs, list):
            vs = [vs] # Ignoring dict/list difference for now

        for v in vs:
            if isinstance(v, dict):
                subindex = shape.setdefault(k, {})
                compute_shape(v, subindex)
            else:
                count_value(k, v, shape)

def count_value(k, v, shape):
    stats = shape.setdefault(k, {})
    if isinstance(stats, dict):
        if len(stats) < MAX_STATS:
            stats[v] = stats.setdefault(v, 0) + 1
        else:
            shape[k] = sum(stats.values()) + 1
    else:
        shape[k] = stats + 1


if __name__ == '__main__':
    index = {}

    for i, l in enumerate(sys.stdin):
        if not l.rstrip():
            continue
        l = l.replace(b'\\\\"', b'\\"')
        try:
            data = json.loads(l)
            thing = data['@graph'][1]
            thing['meta'] = data['@graph'][0]
            compute_shape(thing, index)
        except ValueError as e:
            print("ERROR at", i, "in data:", file=sys.stderr)
            print(l, file=sys.stderr)
            print(e, file=sys.stderr)

    print(json.dumps(index, indent=2)) 
