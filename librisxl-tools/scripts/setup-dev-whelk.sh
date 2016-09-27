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

if [ -z "$OAIPMH_CREDENTIALS" ]; then
    echo "ERROR: OAIPMH credentials not specified!"
    echo ""
    usage
    exit 1
fi



# Create OAI-PMH files from examples unless they exist
if [ "$FORCE_REBUILD" = true ] || [ ! -f $WORKDIR/oaidump/bib/oaipmh ]; then
    echo ""
    echo "Setting up OAIPMH dump..."
    echo ""

    mkdir -p $WORKDIR/oaidump

    if [ "$FORCE_REBUILD" = true ]; then
        rm -rf .venv
    fi
    if [ ! -d .venv ]; then
        virtualenv -p python2.7 .venv
        .venv/bin/pip install -r librisxl-tools/scripts/requirements.txt
    fi

    .venv/bin/python librisxl-tools/scripts/assemble_oaipmh_records.py \
           $OAIPMH_CREDENTIALS \
           librisxl-tools/scripts/example_records.tsv $WORKDIR/oaidump/
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
