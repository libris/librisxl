#!/bin/bash
set -e

TOOLDIR=$(dirname $(dirname $0))

usage () {
    cat <<EOF

Usage: $0 -s <SOURCEHOST> -u <SOURCEUSER> -e <DESTENV> [-p <SOURCEPASSWORD>]
           [-d <DESTHOST>] [-n <DESTUSER>] [-w <DESTPASSWORD>]
           [-S <DESTDB>] [-D <DESTDB>]

    -s, --sourcehost
        PostgreSQL host to read from.

    -u, --sourceuser
        Source database username.

    -p, --sourcepassword
        Source database password.

    -e, --destenv
        Destination environement ({dev|qa|stg}).

    -d, --desthost
        PostgreSQL host to write to. Defaults to localhost.

    -n, --destuser
        Destination database username. Defaults to source database username.

    -w, --destpassword
        Destination database password. Defaults to source database password.

    -S, --sourcedb
        Source database name. Defaults to 'whelk'.

    -D, --destdb
        Destination database name. Default to 'whelk'.

EOF
}

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        -s|--sourcehost)
            SOURCEHOST="$2"
            shift
            ;;
        -d|--desthost)
            DESTHOST="$2"
            shift
            ;;
        -e|--destenv)
            DESTENV="$2"
            shift
            ;;
        -u|--sourceuser)
            SOURCEUSER="$2"
            shift
            ;;
        -p|--sourcepassword)
            SOURCEPASSWORD="$2"
            shift
            ;;
        -n|--destuser)
            DESTUSER="$2"
            shift
            ;;
        -w|--destpassword)
            DESTPASSWORD="$2"
            shift
            ;;
        -S|--sourcedb)
            SOURCEDB="$2"
            shift
            ;;
        -D|--destdb)
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


if [ -z $SOURCEHOST ] || [ -z $SOURCEUSER ] || [ -z $SOURCEPASSWORD ] || [ -z $DESTENV ]; then
    usage
    exit 1
fi

if [ -z "$SOURCEDB" ]; then
    SOURCEDB="whelk"
fi

if [ -z "$DESTDB" ]; then
    DESTDB="$SOURCEDB"
fi

if [ -z "$DESTUSER" ]; then
    DESTUSER="$SOURCEUSER"
fi

if [ -z "$DESTPASSWORD" ]; then
    DESTPASSWORD="$SOURCEPASSWORD"
fi

if [ -z $DESTHOST ]; then
    handle_sql_dump() {
        cat -
    }
else
    cat $TOOLDIR/postgresql/drop-all-lddb-tables.plsql | PGPASSWORD=$DESTPASSWORD psql -h $DESTHOST -U $DESTUSER $DESTDB

    handle_sql_dump() {
        PGPASSWORD=$DESTPASSWORD psql -h $DESTHOST -U $DESTUSER $DESTDB
    }
fi

REPLACEID="s!\(\(\"@id\": \"\|\t\)https://libris\)\(-\w\+\)\?\(.kb.se/[bcdfghjklmnpqrstvwxz0-9]\+\)\>!\1-$DESTENV\4!g"

# Using `--schema=public` excludes e.g. extensions.
#-T lddb__versions
PGPASSWORD=$SOURCEPASSWORD pg_dump --schema=public -h $SOURCEHOST -U $SOURCEUSER $SOURCEDB |
    ( read; cat - |
    sed "$REPLACEID" |
    handle_sql_dump )
