#!/usr/bin/env python3
import sys
from subprocess import run, PIPE
from urllib.parse import urljoin

args = sys.argv[1:]

hostenv = args.pop(0) if args else 'dev'
user = args.pop(0) if args else 'whelk'

host = ('pgsql01-{}.libris.kb.se'.format(hostenv)
        if '.' not in hostenv else hostenv)

new_ids, old_ids = [], []

for l in sys.stdin:
    s = l.strip()
    if len(s) > 12:
        new_ids.append(s)
    else:
        old_ids.append(s)

def to_sql_collection(seq):
    return '({})'.format(', '.join(
        "'{}'".format(s) for s in seq))

select = 'SELECT id FROM lddb__identifiers WHERE iri in {};'.format(
        to_sql_collection(
            urljoin('http://libris.kb.se/resource/bib/', s)
            for s in old_ids))

cmd = ['psql', '-h', host, '-U', user, '-t']
result = run(cmd, input=select, check=True, encoding='ascii',
             stdout=PIPE)

ids = result.stdout.split() + new_ids
print(to_sql_collection(ids))
