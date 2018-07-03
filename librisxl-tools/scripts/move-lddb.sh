#!/bin/bash
#sourcehost=$1
dumpfile=$1
env=$2
#tohost=localhost

#pg_dump -U whelk -h $sourcehost whelk

replaceid="s!\(\"@id\": \"https://libris\)\(-\w\+\)\?\(.kb.se/[bcdfghjklmnpqrstvwxz0-9]\+\)\>!\1-$env\3!g"

cat $dumpfile | sed "$replaceid"

#| psql -U whelk -h $tohost whelk
