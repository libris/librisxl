#!/bin/bash
sourcehost=$1
destenv=$2
desthost=$3

if [ -z $sourcehost ] || [ -z $destenv ]; then
    echo "USAGE: $0 SOURCEHOST DESTENV DESTHOST"
    exit 1
fi

if [ -z $desthost ]; then
    handle_sql_dump() {
        cat -
    }
else
    handle_sql_dump() {
        psql -h $desthost -U whelk -W whelk
    }
fi

replaceid="s!\(\(\"@id\": \"\|\t\)https://libris\)\(-\w\+\)\?\(.kb.se/[bcdfghjklmnpqrstvwxz0-9]\+\)\>!\1-$destenv\4!g"

#-T lddb__versions
pg_dump -h $sourcehost -U whelk whelk -W |
    ( read; cat - |
    sed "$replaceid" |
    handle_sql_dump )
