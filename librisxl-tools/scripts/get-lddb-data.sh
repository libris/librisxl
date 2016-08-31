#!/bin/bash
usage() {
    echo "usage $0 DB ID"
}

DB=$1
ID=$2

if [ -z "$DB" ]; then
    usage
    exit 1
fi
if [ -z "$ID" ]; then
    usage
    exit 1
fi
psql $DB -tc "select data from lddb where id in (select id from lddb__identifiers where identifier = '$ID');"
