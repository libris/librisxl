#!/usr/bin/env python3
import os
from urllib.parse import unquote

def find_duplicates():
    # TODO: join instead of id1/id2 and ld1/ld2
    return filter(None, map(str.strip, os.popen("""
    psql -qtA whelk <<-END
    select ld1.data->'@graph'->0->'controlNumber', ld2.data->'@graph'->0->'controlNumber',
        id1.id, id2.id,
        id1.identifier
        from lddb__identifiers id1, lddb__identifiers id2, lddb ld1, lddb ld2
        where id1.identifier = id2.identifier and id1.id != id2.id
            and ld1.id = id1.id and ld2.id = id2.id;
    END
    """)))

uris = {}

for line in find_duplicates():
    no1, no2, id1, id2, uri = (word[1:-1] if word.startswith('"') else word for word in line.split('|'))
    item = uris.setdefault(uri, set())
    item.add((no1, id1))
    item.add((no2, id2))

row = lambda *args: print(*args, sep='\t', end='\r\n')
row("label", "auth-ids", "uri")
for uri, pairs in uris.items():
    leaf = uri.rsplit('/', 1)[-1]
    row(unquote(leaf), ", ".join(no for no, id in pairs), "<%s>" % uri)
