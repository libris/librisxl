#!/bin/bash
set -eu

TOOLDIR=$(dirname $(dirname $0))

usage () {
    cat <<EOF

Usage: $0 --dumppath <DUMPPATH> --destenv <DESTENV>
           --desthost <DESTHOST> --destuser <DESTUSER>
           --destpassword <DESTPASSWORD> --destdb <DESTDB>
    or (write only to stdout)
       $0 --dumppath <DUMPPATH> --destenv <DESTENV>

    --dumppath
        Path to PostgreSQL DB dump, e.g., /foo/dump_prod_whelk_something.gz

    --destenv
        Destination environment ({dev|qa|stg}).

    --desthost
        Destination PostgreSQL host to write to.

    --destuser
        Destination database username.

    --destpassword
        Destination database password.

    --destdb
        Destination database name.

EOF
}

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        --dumppath)
            DUMPPATH="$2"
            shift
            ;;
        --desthost)
            DESTHOST="$2"
            shift
            ;;
        --destenv)
            DESTENV="$2"
            shift
            ;;
        --destuser)
            DESTUSER="$2"
            shift
            ;;
        --destpassword)
            DESTPASSWORD="$2"
            shift
            ;;
        --destdb)
            DESTDB="$2"
            shift
            ;;
        *)
            usage
            exit 1
            ;;
    esac
    shift
done

if ! [[ -v DUMPPATH ]] || ! [[ -v DESTENV ]]; then
    usage
    exit 1
fi

if [[ -v DESTHOST ]]; then
    if ! [[ -v DESTUSER ]] || ! [[ -v DESTPASSWORD ]] || ! [[ -v DESTDB ]]; then
        usage
        exit 1
    fi

    cat $TOOLDIR/postgresql/drop-all-lddb-tables.plsql | PGPASSWORD=$DESTPASSWORD psql -h $DESTHOST -U $DESTUSER $DESTDB

    handle_sql_dump() {
        PGPASSWORD=$DESTPASSWORD psql -h $DESTHOST -U $DESTUSER $DESTDB
    }
else
    handle_sql_dump() {
        cat -
    }
fi

REPLACEID="s/((@id\": \"|\t)https:\/\/libris)(?:-\w+)?(.kb.se\/[bcdfghjklmnpqrstvwxz0-9]+)\b/\1-$DESTENV\3/g"

pg_restore -f - "${DUMPPATH}" |
    ( read; cat - |
    perl -pe "$REPLACEID" |
    handle_sql_dump )
