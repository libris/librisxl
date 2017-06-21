#!/bin/bash
set -e

TOOLDIR=$(dirname $(dirname $0))
WORKDIR=$(pwd)/work

usage () {
    cat <<EOF
Usage: ./setup-dev-whelk.sh -n <NAME> [-D <DATABASE USER>] [-C <CREATEDB USER>]

    -n, --whelk-name
        Instance name. Used as database name and ElasticSearch index.

    -D, --db-user
        PostgreSQL database user.

        If unset, no username will be explicitly supplied to PostgreSQL.

    -C, --createdb-user
        User to run 'createdb' as ('sudo -u <CREATEDB USER> createdb ...').

        If unset, runs 'createdb' without 'sudo'.

    -F, --force-rebuild
        If set, rebuild virtualenvs, definitions, OAIPMH files, and JAR files
EOF
}

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        -n|--whelk-name)
            WHELKNAME="$2"
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
        -F|--force-rebuild)
            FORCE_REBUILD=true
            ;;
        *)
            usage
            exit 1
            ;;
    esac
    shift
done


# Prepare databases
if [ "$DBUSER" ]; then
    REBUILD_ARGS="-D ${DBUSER}"
fi

if [ "$CREATEDB_USER" ]; then
    REBUILD_ARGS="${REBUILD_ARGS} -C ${CREATEDB_USER}"
fi

$TOOLDIR/scripts/manage-whelk-storage.sh -n $WHELKNAME $REBUILD_ARGS -R


# Verify that mandatory arguments have been supplied
if [ -z "$WHELKNAME" ]; then
    echo "ERROR: Whelk name not specified!"
    echo ""
    usage
    exit 1
fi

# Recreate es index with definitions-based ES config
JAR=build/libs/vcopyImporter.jar
if [ "$FORCE_REBUILD" = true ] || [ ! -f $JAR ]; then
    pushd importers
    gradle jar
    java -jar $JAR generateEsConfig ../librisxl-tools/elasticsearch/libris_config.json ../../definitions/source/vocab/display.jsonld ../../definitions/build/vocab.jsonld generated_es_config.json
    echo ""
    echo "Removing es configuration..."
    echo ""
    curl -XDELETE http://localhost:9200/$WHELKNAME
    echo ""
    echo "Done."
    echo ""
    echo "Adding definitions-based ES config..."
    echo ""
    curl -XPUT http://localhost:9200/$WHELKNAME \
        -d@generated_es_config.json \
        --header "Content-Type: application/json"
    echo ""
    echo "Done"
    echo ""    
    popd
fi

# Create definitions dataset unless it exists
DEFS_FILE=../definitions/build/definitions.jsonld.lines
if [ "$FORCE_REBUILD" = true ] || [ ! -f $DEFS_FILE ]; then
    echo ""
    echo "Creating definitions dataset..."
    echo ""

    pushd ../definitions
    git pull
    if [ "$FORCE_REBUILD" = true ]; then
        rm -rf .venv
    fi
    if [ ! -d .venv ]; then
        virtualenv -p python2.7 .venv
        .venv/bin/pip install -r requirements.txt
    fi
    .venv/bin/python datasets.py -l
    popd
fi

# Import OAIPMH data
pushd importers
java -Dxl.secret.properties=../secret.properties -jar $JAR \
     defs ../$DEFS_FILE 
java -Dxl.secret.properties=../secret.properties -jar $JAR reindex definitions
echo ""
echo "Importing example data..."
echo ""
java -Dxl.secret.properties=../secret.properties \
    -Dxl.mysql.properties=../mysql.properties \
     -jar build/libs/vcopyImporter.jar \
     vcopyloadexampledata ../librisxl-tools/scripts/example_records.tsv

popd
echo ""
echo "All Done!"
echo ""
