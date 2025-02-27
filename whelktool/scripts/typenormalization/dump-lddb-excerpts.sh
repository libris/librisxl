#!/bin/bash
set -euxo pipefail

# TODO: Either copy commands and run manually; split into multiple test scripts; or factor out tool (if we do this a lot)...

# Access an "LDDB" (a Postgres DB with data in tables defined by XL):
PGSQL_HOST=$1
PGSQL_USER=$2
BASEDIR=$3

## Option A) Pick Specific Examples By IDs
XLIDS="'p3n5x5g0m8jq33c4', 'dwpmt3dq02c3ldm', 'cwp2z0np1lc7ql3', 'k2vqr29w3zgcqrw', 'vd6mrs8650wpjz8', 'w77j9j1nts7lbpb0', 'wf7hs3v745v37c7', 'wd6339675g3dqdr', 'jsr018hfgnw8vwnj', '2ldlr5qd26w1zpd', 'gzrc3q9s59ftgdk'"
BASENAME=stg-lddb-selection1

# Dump work data:
psql -h $PGSQL_HOST -U $PGSQL_USER whelk -tc "copy (select lddb.data from lddb, lddb__dependencies instanceof where lddb.id = instanceof.dependsonid and instanceof.relation = 'instanceOf' and instanceof.id in ($XLIDS)) to stdout;" | sed 's/\\\\/\\/g' | gzip > $BASEDIR/$BASENAME-works.jsonl.gz
# Dump instance data:
psql -h $PGSQL_HOST -U $PGSQL_USER whelk -tc "copy (select data from lddb where id in ($XLIDS)) to stdout;" | sed 's/\\\\/\\/g' | gzip > $BASEDIR/$BASENAME-instances.jsonl.gz

## Option B) Get By Library
SIGEL_URIS="'https://libris.kb.se/library/Ssb', 'https://libris.kb.se/library/SsbE'"
BASENAME=stg-lddb-heldbyssb

# Dump work data:
psql -h $PGSQL_HOST -U $PGSQL_USER whelk -tc "copy (select lddb.data from lddb, lddb__dependencies instanceof, lddb__dependencies itemof, lddb__dependencies heldby where lddb.id = instanceof.dependsonid and instanceof.relation = 'instanceOf' and instanceof.id = itemof.dependsonid and itemof.relation = 'itemOf' and heldby.relation = 'heldBy' and itemof.id = heldby.id and heldby.dependsonid in (select id from lddb__identifiers where iri in ($SIGEL_URIS))) to stdout;" | sed 's/\\\\/\\/g' | gzip > $BASEDIR/$BASENAME-works.jsonl.gz
# Dump instance data:
psql -h $PGSQL_HOST -U $PGSQL_USER whelk -tc "copy (select lddb.data from lddb, lddb__dependencies itemof, lddb__dependencies heldby where lddb.id = itemof.dependsonid and itemof.relation = 'itemOf' and heldby.relation = 'heldBy' and itemof.id = heldby.id and heldby.dependsonid in (select id from lddb__identifiers where iri in ($SIGEL_URIS))) to stdout;" | sed 's/\\\\/\\/g' | gzip > $BASEDIR/$BASENAME-instances.jsonl.gz
