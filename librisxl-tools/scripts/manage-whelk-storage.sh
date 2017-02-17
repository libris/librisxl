#!/bin/bash
set -e

TOOLDIR=$(dirname $(dirname $0))
RECREATE_DB=false
NUKE_DEFINITIONS=false

usage () {
    cat <<EOF

Usage: ./rebuild-whelk-storage.sh -n <NAME> [-h <POSTGRES HOST>]
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

    -N, --nuke-definitions
        If set, will delete all definitions data from PostgreSQL and
        ElasticSearch.

        Unset by default.
EOF
}

delete_es_collection() {
    collection=$1

    psql -h $DBHOST $DBUSER_ARG $WHELKNAME -c \
         "DELETE FROM lddb__identifiers WHERE id in (SELECT id from lddb where collection = '$collection');"
    psql -h $DBHOST $DBUSER_ARG $WHELKNAME -c \
         "DELETE FROM lddb where collection = '$collection';"
    psql -h $DBHOST $DBUSER_ARG $WHELKNAME -c \
         "DELETE FROM lddb__versions where collection = '$collection';"

    curl -XDELETE http://$ESHOST:9200/$WHELKNAME/$collection/_query \
         -d '{"query": {"match_all": {}}}'
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

if [ "$DBUSER" ]; then
    DBUSER_ARG="-U ${DBUSER}"
fi


if [ "$RECREATE_DB" = true ]; then
    echo "(Re)creating PostgreSQL database..."
    echo ""

    $TOOLDIR/postgresql/drop-tables-and-indexes-sql.sh | psql -h $DBHOST $DBUSER_ARG $WHELKNAME

    psql -h $DBHOST $DBUSER_ARG $WHELKNAME < $TOOLDIR/postgresql/tables.sql
    psql -h $DBHOST $DBUSER_ARG $WHELKNAME < $TOOLDIR/postgresql/indexes.sql

    echo ""
    echo "(Re)creating ElasticSearch database..."
    echo ""


    # Assert that we have definitions data built.
    DEFS_FILE=../definitions/build/definitions.jsonld.lines
    if [ ! -f $DEFS_FILE ]; then
	echo "Please use setup-dev-whelk.sh or build definitions manually before proceeding."
	exit 1
    fi

    # Build ES config generator.
    pushd importers
    JAR=build/libs/vcopyImporter.jar
    if [ "$FORCE_REBUILD" = true ] || [ ! -f $JAR ]; then
	gradle jar
    fi

    # Generate the ES config
    java -jar $JAR generateEsConfig \
	 ../librisxl-tools/elasticsearch/libris_config.json \
	 ../../definitions/source/vocab/display.jsonld \
	 ../../definitions/build/vocab.jsonld \
	 ../generated_es_config.json
    popd

    curl -XDELETE http://$ESHOST:9200/$WHELKNAME
    curl -XPOST http://$ESHOST:9200/$WHELKNAME \
         -d@generated_es_config.json

    echo ""
    echo ""

fi


if [ "$NUKE_DEFINITIONS" = true ]; then
    echo ""
    echo "Nuking definitions data..."
    echo ""

    delete_es_collection 'definitions'

    echo ""
    echo ""
fi

