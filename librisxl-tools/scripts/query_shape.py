#!/usr/bin/env python3
import json


def find_paths(data, key, trail=[]):
    if not isinstance(data, dict):
        return

    for k, v in data.items():
        subtrail = trail + [k]
        if k == key:
            yield subtrail, v
        else:
            yield from find_paths(v, key, subtrail)


def find_keys(data, key=None):
    yield key, count(data)

    if not isinstance(data, dict):
        return

    for k, v in data.items():
        if k == '@id' or isinstance(v, dict) and any(m.startswith('@value ') for m in v):
            continue
        yield from find_keys(v, k)


def count(data):
    if isinstance(data, int):
        return data
    else:
        return sum(count(v) for v in data.values())


if __name__ == '__main__':
    import sys

    args = sys.argv[1:]
    if not args:
        exit(1)

    keys = []
    json_repr = False

    for arg in args:
        if arg in ('-j', '--json'):
            json_repr = True
        else:
            keys.append(arg)

    filepath = keys.pop(0)

    with open(filepath) as f:
        shapes = json.load(f)

    def by_count(items):
        return sorted(items, key=lambda it: count(it[1]), reverse=True)

    for key in keys:
        for path, v in by_count(find_paths(shapes, key)):
            if json_repr:
                v_repr = json.dumps(v, indent=4, ensure_ascii=False)
                v_repr.replace('\n', '\n    ')
            else:
                v_repr = f'{count(v):,}'
            print('.'.join(path), v_repr)

    if not keys:
        counter = {}
        for key, v in find_keys(shapes):
            counter[key] = counter.setdefault(key, 0) + v
        for key, v in by_count(counter.items()):
            print(key, f'{v:,}')
