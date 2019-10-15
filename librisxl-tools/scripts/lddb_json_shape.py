import json
import sys


MAX_STATS = 100
STATS_FOR_ALL = {
        # from auth 008
        "marc:subdivision",
        "marc:romanization",
        "marc:languageOfCatalog",
        "marc:kindOfRecord",
        "descriptionConventions",
        "marc:subjectHeading",
        "marc:typeOfSeries",
        "marc:numberedSeries",
        "marc:headingSeries",
        "marc:subjectSubdivision",
        "marc:govtAgency",
        "marc:reference",
        "marc:recordUpdate",
        "marc:personalName",
        "marc:level",
        "marc:modifiedRecord",
        "marc:catalogingSource",
        "marc:headingMain",
        "marc:headingSubject",
        # "shouldn't" be too many...
        "marc:displayText",
}


def compute_shape(node, index):
    if len(node) == 1 and '@id' in node:
        count_value('@id', node['@id'], index)
        return

    rtype = node.get('@type')
    if isinstance(rtype, list):
        rtype = '+'.join(rtype)
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
        if k in STATS_FOR_ALL or len(stats) < MAX_STATS:
            stats[v] = stats.setdefault(v, 0) + 1
        else:
            shape[k] = sum(stats.values()) + 1
    else:
        shape[k] = stats + 1


if __name__ == '__main__':
    from time import time

    index = {}

    t_last = 0
    cr = '\r'
    for i, l in enumerate(sys.stdin):
        if not l.rstrip():
            continue
        if isinstance(l, bytes):
            l = l.decode('utf-8')

        t_now = time()
        if t_now - t_last > 2:
            t_last = t_now
            print(f'{cr}At: {i}', end='', file=sys.stderr)

        try:
            data = json.loads(l)
            graph =  data['@graph']
            thing =graph[1]
            thing['meta'] =graph[0]

            if len(graph) > 2 and 'instanceOf' in thing:
                work = graph[2]
                assert thing['instanceOf']['@id'] == work['@id']
                thing['instanceOf'] = work

            compute_shape(thing, index)
            compute_shape(work, index)

        except (ValueError, AttributeError) as e:
            print(f'ERROR at: {i} in data:', file=sys.stderr)
            print(l, file=sys.stderr)
            print(e, file=sys.stderr)

    print(json.dumps(index, indent=2, ensure_ascii=True))
