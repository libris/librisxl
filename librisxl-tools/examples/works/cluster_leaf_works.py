import json
from hashlib import md5


def digest_leafs(line):
    rec_uri, work_ta_key, leafs = process_record(json.loads(line))
    return rec_uri, work_ta_key, list(filter(None,
        (digest_leaf(*args) for args in leafs)))


def process_record(data):
    graph = data['@graph']
    entities = graph[1:]
    return graph[0]['@id'], _make_ta_key(*entities), (
            (thing, path, leaf) for thing in entities
            for path, leaf in find_leaf_works(thing, []))


def find_leaf_works(item, path):
    if isinstance(item, list):
        for it in item:
            yield from find_leaf_works(it, path)
    elif isinstance(item, dict):
        if item.get('@type') == 'Work':
            yield path, item
        for key, nested in item.items():
            yield from find_leaf_works(nested, path + [key])


def digest_leaf(thing, path, leaf):
    if leaf['@type'] != 'Work':
        return None

    instance = leaf.pop('hasInstance', None)
    if isinstance(instance, list):
        instance = instance[0]
    if not instance:
        return None

    leaf_repr = _repr(leaf)

    ta_key = _make_ta_key(instance, leaf)

    ctrlnr = None
    record = instance.get('describedBy')
    if isinstance(record, list):
        record = record[0]
    if record:
        ctrlnr = record.get('controlNumber')
        if isinstance(ctrlnr, list):
            ctrlnr = ', '.join(ctrlnr)

    if not (ta_key or ctrlnr):
        return None

    leaf_checksum = md5(leaf_repr.encode('utf-8')).hexdigest()
    type_path =  f"{thing['@type']} {'.'.join(path)}"

    leaf_repr = None # TODO: until needed, don't pass back
    return type_path, ta_key, ctrlnr, leaf_checksum, leaf_repr


def _make_ta_key(instance, work=None, *args):
    if not work:
        return None
    try:
        title = work.get('hasTitle') or instance['hasTitle']
        if isinstance(title, list):
            title = title[0]
        agent = work['contribution'][0]['agent']
        return f'{_repr(title)} + {_repr(agent)}'
    except (IndexError, KeyError):
        return None


def _repr(o):
    return json.dumps(o, sort_keys=True, ensure_ascii=False).replace('"', '')


if __name__ == '__main__':
    from collections import defaultdict
    from multiprocessing import Pool
    import sys
    from time import time

    #work_key_counts = defaultdict(int)
    clusters = {
        'skipped': None,
        #'work_key_counts': None
    }

    def _count(d, k):
        d.setdefault(k, 0)
        d[k] += 1

    pool = Pool()
    try:
        results = pool.imap_unordered(digest_leafs, sys.stdin, chunksize=8192)
        t_last = 0
        for i, result in enumerate(results):
            #work_key_counts[work_ta_key] += 1
            rec_uri, work_ta_key, digests = result
            for type_path, ta_key, ctrlnr, leaf_checksum, leaf_repr in digests:
                cluster = clusters.setdefault(ta_key or ctrlnr, {})
                shapeinfo = cluster.setdefault(leaf_checksum, {})
                if leaf_repr:
                    shapeinfo.setdefault('objs').append(leaf_repr)
                else:
                    _count(shapeinfo, 'objs')
                shapeinfo.setdefault(f'{type_path} of', []).append(rec_uri)
                if ctrlnr:
                    _count(shapeinfo.setdefault(ctrlnr, {}), type_path)

            t_now = time()
            if t_now - t_last > 2:
                t_last = t_now
                print(f'\033cRecords: {i + 1}', file=sys.stderr)
                print('Example:', result, file=sys.stderr)

    finally:
        skipped = 0
        for key in list(clusters.keys()):
            cluster = clusters[key]
            if cluster is None:
                continue
            size = len(cluster)
            if size < 2:
                skipped += 1
                del clusters[key]
            else:
                #matching_works = work_key_counts.get(key)
                #if matching_works:
                #    cluster['matching_works'] = matching_works
                cluster['size'] = size

        clusters['skipped'] = skipped
        #clusters['work_key_counts'] = len(work_key_counts)
        json.dump(clusters, sys.stdout, ensure_ascii=False, indent=2)
