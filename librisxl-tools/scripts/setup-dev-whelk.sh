#!/bin/bash
TOOLDIR=$(dirname $(dirname $0))
WORKDIR=$(pwd)/work

usage () {
    cat <<EOF
Usage: ./setup-dev-whelk.sh -n <NAME> -O <OAIPMH CREDENTIALS> [-D <DATABASE USER>] [-C <CREATEDB USER>]

    -n, --whelk-name
        Instance name. Used as database name and ElasticSearch index.

    -O, --oaipmh-credentials
        Credentials for OAIPMH. On the form "<USER>:<PASSWORD>".

    -D, --db-user
        PostgreSQL database user.

        If unset, no username will be explicitly supplied to PostgreSQL.

    -C, --createdb-user
        User to run 'createdb' as ('sudo -u <CREATEDB USER> createdb ...').

        If unset, runs 'createdb' without 'sudo'.

    -F, --force-rebuild
        If set, rebuild definitions, OAIPMH files, and import JAR.
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
        -O|--oaipmh-credentials)
            OAIPMH_CREDENTIALS="$2"
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


# Verify that mandatory arguments have been supplied
if [ -z "$WHELKNAME" ]; then
    echo "ERROR: Whelk name not specified!"
    echo ""
    usage
    exit 1
fi

if [ -z "$OAIPMH_CREDENTIALS" ]; then
    echo "ERROR: OAIPMH credentials not specified!"
    echo ""
    usage
    exit 1
fi


# Set up PostgreSQL database
echo "Setting up PostgreSQL..."
echo ""

if [ -z "$CREATEDB_USER" ]; then
    dropdb $WHELKNAME
    createdb $WHELKNAME
else
    sudo -u $CREATEDB_USER dropdb $WHELKNAME
    sudo -u $CREATEDB_USER createdb $WHELKNAME
fi

if [ -z "$DBUSER" ]; then
    psql $WHELKNAME < $TOOLDIR/postgresql/tables.sql
    psql $WHELKNAME < $TOOLDIR/postgresql/indexes.sql
else
    psql -U $DBUSER -h localhost $WHELKNAME < \
         $TOOLDIR/postgresql/tables.sql
    psql -U $DBUSER -h localhost $WHELKNAME < \
         $TOOLDIR/postgresql/indexes.sql
fi


# Set up ElasticSearch
echo ""
echo "Setting up ElasticSearch..."
echo ""

curl -XDELETE http://localhost:9200/$WHELKNAME
curl -XPOST http://localhost:9200/$WHELKNAME \
     -d@$TOOLDIR/elasticsearch/libris_config.json


# Create OAI-PMH files from examples unless they exist
if [ "$FORCE_REBUILD" = true ] || [ ! -f $WORKDIR/oaidump/bib/oaipmh ]; then
    echo ""
    echo "Setting up OAIPMH dump..."
    echo ""

    mkdir -p $WORKDIR/oaidump
    if [ ! -d .venv ]; then
        virtualenv .venv
        source .venv/bin/activate
        pip install -r librisxl-tools/scripts/requirements.txt
    else
	source .venv/bin/activate
    fi
    python librisxl-tools/scripts/assemble_oaipmh_records.py \
           $OAIPMH_CREDENTIALS \
           librisxl-tools/scripts/example_records.tsv $WORKDIR/oaidump/
    deactivate
fi


# Create definitions dataset unless it exists
DEFS_FILE=../definitions/build/definitions.jsonld.lines
if [ "$FORCE_REBUILD" = true ] || [ ! -f $DEFS_FILE ]; then
    echo ""
    echo "Creating definitions dataset..."
    echo ""

    pushd ../definitions
    if [ ! -d .venv ]; then
        virtualenv .venv
        source .venv/bin/activate
        pip install -r requirements.txt
    else
        source .venv/bin/activate
    fi
    python datasets.py -l
    deactivate
    popd
fi


# Import OAIPMH data
echo ""
echo "Importing OAIPMH data..."
echo ""

pushd importers
JAR=build/libs/importers.jar
if [ "$FORCE_REBUILD" = true ] || [ ! -f $JAR ]; then
    gradle jar
fi

java -Dxl.secret.properties=../secret.properties -jar $JAR \
     defs ../$DEFS_FILE

for source in auth bib hold; do
    java -Dxl.secret.properties=../secret.properties \
         -jar build/libs/importers.jar \
         harvest file:///$WORKDIR/oaidump/$source/oaipmh
done
popd


echo ""
echo "Done!"
echo ""
