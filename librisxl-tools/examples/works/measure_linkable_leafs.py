#!/usr/bin/env python3
import json
import re
from unicodedata import normalize


keys = ['controlNumber']
#annotate_on = ['marc:displayText', 'part']
annotate_on = {}
by_type = True


def map_record(line):
    graph = json.loads(line)['@graph']
    rec_id = graph[0]['@id'].rsplit('/', 1)[-1]
    return rec_id, tuple(('.'.join(path), value)
            for i, thing in enumerate(graph[1:])
            for path, value in
            find_paths(graph, thing, keys, trail=_start_trail(i, thing)))


def _start_trail(i, thing):
    key = _annotated_key('instanceOf' if i else '', thing)
    return [key] if key else []


def _annotated_key(k, o, path=[]):
    is_dict = isinstance(o, dict)
    if by_type and is_dict and '@type' in o:
        k = f"{k}[{o['@type']}]"
    if annotate_on and is_dict:
        for k2 in annotate_on:
            if k2 in o:
                if isinstance(annotate_on, dict):
                    k2 = annotate_on[k2](o[k2], o, path)
                if k2:
                    k += f'[{k2}]'
    return k


def find_paths(graph, data, keys, trail=[]):
    if isinstance(data, list):
        for item in data:
            yield from find_paths(graph, item, keys, trail)
        return

    if not isinstance(data, dict):
        return

    for k, vs in data.items():
        if not vs:
            continue
        if not isinstance(vs, list):
            vs = [vs]
        for v in vs:
            tk = _annotated_key(k, v, trail)
            subtrail = trail + [tk]
            if k in keys:
                yield subtrail, v
            else:
                yield from find_paths(graph, v, keys, subtrail)


MATCH_REVIEW_OF = re.compile(
        r'(?i)(\s|\W)*r.?e.?[czv]\s?e.*i.?(on|rt)\w*\W+(av|of)')

MATCH_DROP = re.compile(r'(?i)^projekt$|^värdpub.*|^channel ?record')

MATCH_COMMENT = re.compile(r'(?i)^komment.+(till|kring|på)')

MATCH_CONTINUED_BY = re.compile(r'(?i)^forts.*')

MATCH_SUPPLEMENT_TO = re.compile(r'(?i)^bilaga.+(till|kring|på)')

MATCH_PARALLELL = re.compile(r'(?i)^parall?ell?|^repr(int|oduct).*')

def digest_displaytext(v, owner, path):
    if isinstance(v, list):
        v = ' | '.join(v)

    dsign = {True: '+', False: '-', None: ''}[owner.get('marc:toDisplayNote')]
    #    BOOST_PRESERVE_STATUS? It has been true by default though, and false
    #    "ought" to be used with a bib 580 instead... *May* be of value if
    #    false *is* used on the majority of the long tail, or to indicate
    #    conversion of marc:displayText to note on some major group...

    if MATCH_DROP.match(v):
        return None

    if MATCH_REVIEW_OF.match(v):
        return '>reviewOf'

    # TODO: in path doesn't work, it contains annotations; check in pathstr...
    path = '.'.join(path)

    if MATCH_CONTINUED_BY.match(v):
        return None if 'continuedBy' in path else '>continuedBy'

    if MATCH_COMMENT.match(v):
        return 'KOMMENTAR_TILL'

    if MATCH_SUPPLEMENT_TO.match(v):
        return None if 'supplementTo' in path else '>supplementTo'

    if MATCH_PARALLELL.match(v):
        return None if 'otherEdition' in path else '>otherEdition'

    v = v.lower()
    v = normalize('NFD', v)
    v = re.sub(r'[^a-z0-9 ]', '', v)
    v = v.strip()

    if 'otherPhysicalFormat' in path:
        if v == 'annat format':
            return None
        if v == "inlast ur": # TODO: missing?
            return '>derivativeOf'

    return f'{dsign}displayText={v}'

annotate_on['marc:displayText'] = digest_displaytext


def repr_path(path, value, strip_detail=False):
    pathrepr = re.sub(r'instanceOf\[(\w+)\]\.', 'W@', path) \
            .replace('.hasInstance', '@I') \
            .replace(".describedBy[Record].controlNumber", "")

    if strip_detail:
        pathrepr = re.sub(r'\[[A-Z]\w+\]', '', pathrepr)
        pathrepr = re.sub(r'=[^]]+', '', pathrepr)
        pathrepr = re.sub(r'\[[+-]', '[', pathrepr)
    else:
        pathrepr = pathrepr.replace('[Instance]', '')

    if pathrepr.startswith('.'):
        pathrepr = pathrepr[1:]

    valuerepr = "LINK" if value in linkable_ids else "BLANK"

    return f'{pathrepr}:{valuerepr}'



if __name__ == '__main__':
    from collections import defaultdict, Counter
    #from multiprocessing import Pool
    from pathlib import Path
    import sys
    from time import time

    CHUNKSIZE = 8192

    args = sys.argv[1:]

    # Get all record identifiers (TODO make it better and quicker using pypy):
    # $ jq zcat ~/data/kb/libris/dumps/stg-lddb-bib.json.lines.gz | jq '.["@graph"][0]|([ .["@id"], .sameAs[0]["@id"] ] + [ .identifiedBy[]?|select(.["@type"] == "LibrisIIINumber")|.value ])' | awk '/"http/ { print gensub(/.+\/([^\/"]+)",?/, "\\1", "g") } !/\/\// && /"/ { print gensub(/[ ",]/, "", "g") }'
    if args:
        id_dump_fpath = args.pop(0)
    else:
        id_dump_fpath = '~/data/kb/libris/tmp/xlids-bibs-libris3s'

    with open(Path(id_dump_fpath).expanduser()) as f:
        linkable_ids = {s.strip() for s in f}

    keep_examples = 10

    def add_example(examples, rec_id):
        if len(examples) < keep_examples:
            examples.add(rec_id)
        elif len(examples) == keep_examples:
            examples.add('...')

    measurements = defaultdict(lambda: (set(), 0, defaultdict(lambda: (set(), 0))))

    def repr_count(n):
        return '1' if n == 1 else '2+' if n < 10 else '10+'

    #pool = Pool()
    #results = pool.imap_unordered(map_record, sys.stdin, chunksize=CHUNKSIZE)
    results = map(map_record, sys.stdin)
    try:
        t_last = 0
        for i, (rec_id, paths) in enumerate(results):

            pattern = "|".join(f'{s}#{repr_count(n)}' for s, n in
                Counter(repr_path(path, value, strip_detail=True)
                    for path, value in paths).most_common()) or 'NOREF'

            examples, count, detailed = measurements[pattern]

            #add_example(examples, rec_id)

            for path, value in Counter(repr_path(path, value)
                    for path, value in paths).most_common():
                dexamples, dcount = detailed[path]
                add_example(dexamples, rec_id)
                detailed[path] = (dexamples, dcount + 1)

            measurements[pattern] = (examples, count + 1, detailed)

            t_now = time()
            if (t_now - t_last) > 2:
                t_last = t_now
                measured = len(measurements)
                print(f"\033cRecords: {i + 1:,}, Measurements: {measured:,}",
                        file=sys.stderr)

    finally:
        for k, (examples, count, detailed) in sorted(measurements.items(),
                key=lambda kv: kv[1][1], reverse=True):
            print(k, f'{count:,}', sep='\t') #, ','.join(examples)
            for detail, (dexamples, dcount) in sorted(detailed.items(),
                    key=lambda kv: kv[1][1], reverse=True):
                print('', detail, f'{dcount:,}', ','.join(dexamples), sep='\t')
