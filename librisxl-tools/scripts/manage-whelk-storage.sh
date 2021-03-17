#!/bin/bash
set -e
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )
TOOLDIR=$(dirname $SCRIPT_DIR)

RECREATE_DB=false
NUKE_DEFINITIONS=false
RUN_MIGRATIONS=false

usage () {
    cat <<EOF

Usage: ./manage-whelk-storage.sh -n <NAME> [-h <POSTGRES HOST>]
           [-e <ELASTICSEARCH HOST>] [-D <POSTGRES USER>]
           [-C <CREATEDB USER>] [-R] [-N]

    -n, --whelk-name
        Instance name. Used as database name and ElasticSearch index.

    -h, --db-host
        PostgreSQL host to connect to.

        Defaults to localhost.

    -e, --es-host
        ElasticSearch host to connect to.

        Defaults to localhost.

    -D, --db-user
        PostgreSQL database user.

        If unset, no username will be explicitly supplied to
        PostgreSQL.

    -C, --createdb-user
        User to run 'createdb' as ('sudo -u <CREATEDB USER> createdb
        ...').

        If unset, runs 'createdb' without 'sudo'.

    -R, --recreate-db
        If set, will drop and recreate PostgreSQL and ElasticSearch
        databases indicated by -n/--whelk-name, and reload schema.

        Unset by default.

    -M, --run-migrations
        If set, will run all migration scripts on PostgreSQL database
        indicated by -n/--whelk-name

        Unset by default.

    -N, --nuke-definitions
        If set, will delete all definitions data from PostgreSQL and
        ElasticSearch.

        Unset by default.
EOF
}

delete_definitions() {
    psql -h $DBHOST $DBUSER_ARG $WHELKNAME -c \
         "DELETE FROM lddb__identifiers WHERE id in (SELECT id from lddb where ( data#>'{@graph,0,inDataset}' @> '[{\"@id\":\"https://id.kb.se/dataset/definitions\"}]' OR data#>>'{@graph,1,@id}' in ('https://id.kb.se/vocab/', 'https://id.kb.se/vocab/context', 'https://id.kb.se/vocab/display')) and collection = 'definitions');"
    psql -h $DBHOST $DBUSER_ARG $WHELKNAME -c \
         "DELETE FROM lddb where ( data#>'{@graph,0,inDataset}' @> '[{\"@id\":\"https://id.kb.se/dataset/definitions\"}]' OR data#>>'{@graph,1,@id}' in ('https://id.kb.se/vocab/', 'https://id.kb.se/vocab/context', 'https://id.kb.se/vocab/display')) and collection = 'definitions';"
    psql -h $DBHOST $DBUSER_ARG $WHELKNAME -c \
         "DELETE FROM lddb__versions where ( data#>'{@graph,0,inDataset}' @> '[{\"@id\":\"https://id.kb.se/dataset/definitions\"}]' OR data#>>'{@graph,1,@id}' in ('https://id.kb.se/vocab/', 'https://id.kb.se/vocab/context', 'https://id.kb.se/vocab/display')) and collection = 'definitions' ;"

    curl -XPOST http://$ESHOST:9200/$ESINDEX/_delete_by_query \
          -H 'Content-Type: application/json' \
          -d '{ "query": { "match": { "meta.inDataset.@id": "https://id.kb.se/dataset/definitions" } } }'
}

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        -n|--whelk-name)
            WHELKNAME="$2"
            shift
            ;;
        -h|--db-host)
            DBHOST="$2"
            shift
            ;;
        -e|--es-host)
            ESHOST="$2"
            shift
            ;;
        -E|--es-index)
            ESINDEX="$2"
            shift
            ;;
        -D|--db-user)
            DBUSER="$2"
            shift
            ;;
        -C|--createdb-user)
            CREATEDB_USER="$2"
            shift
            ;;
        -R|--recreate-db)
            RECREATE_DB=true
            ;;
        -N|--nuke-definitions)
            NUKE_DEFINITIONS=true
            ;;
        -M|--run-migrations)
            RUN_MIGRATIONS=true
            ;;
        *)
            usage
            exit 1
            ;;
    esac
    shift
done


if [ -z "$DBHOST" ]; then
    DBHOST="localhost"
fi

if [ -z "$ESHOST" ]; then
    ESHOST="localhost"
fi

if [ -z "$ESINDEX" ]; then
    ESINDEX="${WHELKNAME}"
fi

if [ "$DBUSER" ]; then
    DBUSER_ARG="-U ${DBUSER}"
fi


if [ "$RECREATE_DB" = true ]; then
    echo "(Re)creating PostgreSQL database..."
    echo ""

    psql -h $DBHOST $DBUSER_ARG $WHELKNAME -qAt -c 'select $$drop table if exists "$$ || tablename || $$" cascade;$$ from pg_tables where schemaname = $$public$$;' > /tmp/droptables.sql
    cat /tmp/droptables.sql | psql -h $DBHOST $DBUSER_ARG $WHELKNAME

    psql -h $DBHOST $DBUSER_ARG $WHELKNAME < $TOOLDIR/postgresql/tables.sql
    psql -h $DBHOST $DBUSER_ARG $WHELKNAME < $TOOLDIR/postgresql/indexes.sql
    MIGRATIONS=${TOOLDIR}/postgresql/migrations/*.plsql
    for MIGRATIONFILE in $MIGRATIONS
    do
      psql -h $DBHOST $DBUSER_ARG $WHELKNAME < $MIGRATIONFILE
    done

    echo ""
    echo "(Re)creating ElasticSearch database..."
    echo ""

    curl -XDELETE http://$ESHOST:9200/$ESINDEX

    echo ""
    echo ""
    curl -XPUT http://$ESHOST:9200/$ESINDEX \
         -d@$TOOLDIR/elasticsearch/libris_config.json \
         --header "Content-Type: application/json"
    echo ""
    echo ""

fi

if [ "$RUN_MIGRATIONS" = true ]; then
    echo "Running PostgreSQL database migrations..."
    CURRENT_VERSION=$(psql -qtA -h $DBHOST $DBUSER_ARG $WHELKNAME -c "SELECT version FROM lddb__schema" || echo 0)
    echo "Version before: $CURRENT_VERSION"
    echo ""

    MIGRATIONS=$(ls -1 ${TOOLDIR}/postgresql/migrations/*.plsql | awk "NR > $CURRENT_VERSION")
    for MIGRATIONFILE in $MIGRATIONS
    do
      psql -h $DBHOST $DBUSER_ARG $WHELKNAME < $MIGRATIONFILE
    done

    echo ""
    echo "Version after: $(psql -qtA -h $DBHOST $DBUSER_ARG $WHELKNAME -c "SELECT version FROM lddb__schema")"
fi

if [ "$NUKE_DEFINITIONS" = true ]; then
    echo ""
    echo "Nuking definitions data..."
    echo ""

    delete_definitions

    echo ""
    echo ""
fi
