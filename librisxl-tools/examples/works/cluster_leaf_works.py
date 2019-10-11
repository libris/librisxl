import json
from hashlib import md5


def _repr(o):
    return json.dumps(o, sort_keys=True, ensure_ascii=False).replace('"', '')


def find_leaf_works(item, path):
    if isinstance(item, list):
        for it in item:
            yield from find_leaf_works(it, path)
    elif isinstance(item, dict):
        if item.get('@type') == 'Work':
            yield path, item
        for key, nested in item.items():
            yield from find_leaf_works(nested, path + [key])


def process_record(data):
    for thing in data['@graph'][1:]:
        for path, leaf in find_leaf_works(thing, []):
            yield thing, path, leaf


def digest_leaf(thing, path, leaf):
    if leaf['@type'] != 'Work':
        return None

    instance = leaf.pop('hasInstance', None)
    if isinstance(instance, list):
        instance = instance[0]
    if not instance:
        return None

    leaf_repr = _repr(leaf)

    at_key, ctrlnr = None, None
    try:
        title = _repr(leaf.get('hasTitle') or instance['hasTitle'])
        agent = _repr(leaf['contribution'][0]['agent'])
        at_key = f'{title} + {agent}'
    except (IndexError, KeyError):
        record = instance.get('describedBy')
        if isinstance(record, list):
            record = record[0]
        if record:
            ctrlnr = record.get('controlNumber')
            if isinstance(ctrlnr, list):
                ctrlnr = ', '.join(ctrlnr)

    if not at_key or ctrlnr:
        return None

    leaf_checksum = md5(leaf_repr.encode('utf-8')).hexdigest()
    type_path =  f"{thing['@type']} {'.'.join(path)}"

    leaf_repr = None # TODO: until needed, don't pass back
    return type_path, at_key, ctrlnr, leaf_checksum, leaf_repr


def digest_leafs(line):
    return list(filter(None, (digest_leaf(*args)
        for args in process_record(json.loads(line)))))


if __name__ == '__main__':
    from collections import defaultdict, Counter
    from multiprocessing import Pool
    import sys
    from time import time

    clusters = defaultdict(lambda: defaultdict(dict))

    def _count(d, k):
        d.setdefault(k, 0)
        d[k] += 1

    pool = Pool()
    results = pool.imap_unordered(digest_leafs, sys.stdin, chunksize=8192)
    try:
        t_last = 0
        for i, digests in enumerate(results):
            for type_path, at_key, ctrlnr, leaf_checksum, leaf_repr in digests:
                cluster = clusters[at_key or ctrlnr]
                shapeinfo = cluster[leaf_checksum]
                if leaf_repr:
                    shapeinfo.setdefault('objs').append(leaf_repr)
                else:
                    _count(shapeinfo, 'objs')
                _count(shapeinfo, type_path)
                if ctrlnr:
                    _count(shapeinfo.setdefault(ctrlnr, {}), type_path)

            t_now = time()
            if t_now - t_last > 2:
                t_last = t_now
                print(f'\033cRecords: {i + 1}', file=sys.stderr)
                print('Example:', digests, file=sys.stderr)

    finally:
        for key in list(clusters.keys()):
            size = len(clusters[key])
            if size < 2:
                del clusters[key]
            else:
                clusters[key]['size'] = size

        json.dump(clusters, sys.stdout, ensure_ascii=False, indent=2)
