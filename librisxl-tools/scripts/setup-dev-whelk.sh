#!/bin/bash
TOOLDIR=$(dirname $(dirname $0))
WORKDIR=$(pwd)/work

WHELKNAME=$1

if [ "$WHELKNAME" == "" ]; then
    echo "Provide a whelk name (to be used for the local psql db AND elasticsearch index)"
    exit 1
fi

set -x

# Drop and create PostgreSQL DB
dropdb $WHELKNAME
createdb $WHELKNAME
psql $WHELKNAME < $TOOLDIR/postgresql/tables.sql
psql $WHELKNAME < $TOOLDIR/postgresql/indexes.sql

# Drop and create ES index
curl -XDELETE http://localhost:9200/$WHELKNAME
curl -XPOST http://localhost:9200/$WHELKNAME -d@$TOOLDIR/elasticsearch/libris_config.json

# Create OAI-PMH files from examples unless they exist
if [ ! -f $WORKDIR/oaidump/bib/oaipmh ]; then
    mkdir -p $WORKDIR/oaidump
    OAIPMH_PASS=work/oaipmh.txt
    if [ ! -f $OAIPMH_PASS ]; then
        echo "Missing $OAIPMH_PASS"
        exit 1
    fi
    python librisxl-tools/scripts/assemble_oaipmh_records.py $(cat $OAIPMH_PASS) librisxl-tools/scripts/example_records.tsv $WORKDIR/oaidump/
fi

# Create definitions dataset unless it exists
DEFS_FILE=../definitions/build/definitions.jsonld.lines
if [ ! -f $DEFS_FILE ]; then
    pushd ../definitions
        python datasets.py -l
    popd
fi

pushd importers
    JAR=build/libs/importers.jar
    if [ ! -f $JAR ]; then
        gradle jar
    fi

    java -jar $JAR defs ../$DEFS_FILE

    for source in auth bib hold; do
        java -jar build/libs/importers.jar harvest file:///$WORKDIR/oaidump/$source/oaipmh
    done
popd
