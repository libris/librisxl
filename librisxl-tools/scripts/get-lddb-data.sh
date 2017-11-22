#!/bin/bash
DB=$1
ID=$2
if [ -z "$DB" ] || [ -z "$ID" ]; then
    echo "usage: $0 DB ID"
    exit 1
fi
psql $DB -tc "select data from lddb where id in (select id from lddb__identifiers where iri = '$ID');" | python -mjson.tool
