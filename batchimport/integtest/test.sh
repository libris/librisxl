#!/bin/bash

set -eux

# NEVER RUN THIS TEST CASE ON AN ENVIRONMENT WHERE THE DATA MATTERS!
# This test case assumes you have built the batch_import jar file.
# It also assumes that 'jq' is installed on the system.
# It also assumes it is ok to delete all data with changedIn = 'importtest' as a method for cleaning up!

SECRET_PROPS_PATH="$HOME/secret.properties-local"
DB_USER=whelk
DB_HOST=localhost
DB_NAME=whelk_dev

usage () {
    cat <<EOF

Usage: ./test.sh [-s <SECRET PROPERTIES PATH>][-h <POSTGRES HOST>][-u <POSTGRES USER>][-n <POSTGRES DB NAME>]
    -s, --secret-properties
        Path to secret.properties
        Defaults to $SECRET_PROPS_PATH
    
    -u, --db-user
        PostgreSQL database user.
        Defaults to $DB_USER
    
    -n, --db-name
        PostgreSQL database name. 
        Defaults to $DB_NAME

    -h, --db-host
        PostgreSQL host to connect to.
        Defaults to $DB_HOST
EOF
}

VALID_ARGS=$(getopt -o s:u:h:n: --long secret-properties:,db-user:,db-host:,db-name: "$@")
if [[ $? -ne 0 ]]; then
    usage
    exit 1;
fi

while [[ $# -gt 0 ]]
do
    key="$1"

    case $key in
        -s|--secret-properties)
            SECRET_PROPS_PATH="$2"
            shift
            ;;
        -u|--db-user)
            DB_USER="$2"
            shift
            ;;
        -h|--db-host)
            DB_HOST="$2"
            shift
            ;;
        -n|--db-name)
            DB_NAME="$2"
            shift
            ;;
        *)
            usage
            exit 1
            ;;
    esac
    shift
done

SECRET_PROPS="-Dxl.secret.properties=$SECRET_PROPS_PATH"
PSQL="psql -h $DB_HOST -U $DB_USER $DB_NAME"

function cleanup {
    $PSQL <<< "delete from lddb__identifiers where id in (select id from lddb where changedIn = 'importtest');"
    $PSQL <<< "delete from lddb__dependencies where id in (select id from lddb where changedIn = 'importtest');"
    $PSQL <<< "delete from lddb where changedIn = 'importtest';"
}

OUTCOME=$"Failures if any: "


function fail {
    OUTCOME=$"${OUTCOME}[Failed: $1], "
}

# Start with clean slate
cleanup

pushd ../

######## NORMAL IMPORT
# Check created bib and attached hold
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --changedBy=Utb1
bibResourceId=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["@id"]')
itemOf=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'hold'" | jq '.["@graph"]|.[1].itemOf|.["@id"]')
if [ "$itemOf" != "$bibResourceId" ]; then
    fail "Normal import"
fi

######## RERUN SAME FILE, NO MORE DATA
# Re-run same file, should result in no changes
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --changedBy=Utb1
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'bib'")
if (( $rowCount != 1 )) ; then
    fail "Expected single bib record"
fi
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'hold'")
if (( $rowCount != 1 )) ; then
    fail "Expected single hold record"
fi

######## REPLACE RECORD. batch1.xml contains the same data but with a changed title for the bib record and another 035$a
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch1.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --replaceBib --changedBy=Utb1
newBibResourceId=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["@id"]')
if [ $newBibResourceId != $bibResourceId ]; then
    fail "Bib-replace altered the ID!"
fi
mainTitle=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["hasTitle"]|.[0]|.["mainTitle"]')
expect="\"Polisbilen får INTE ett larm\""
if [ "$mainTitle" != "$expect" ]; then
    fail "Data was not replaced!"
fi
systemNumbers=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[0]|.["identifiedBy"]|.[]|.["value"]')
if [[ "$systemNumbers" != *"NEW"* ]]; then
    fail "Systemnumber was not added!"
fi
if [[ "$systemNumbers" != *"OLD"* ]]; then
    fail "Systemnumber was not retained!"
fi

cleanup
######## ISBN 10/13 matching. Batch 2 and 3 contain the same data but with varying forms of the same ISBN.
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch2.xml --format=xml --live --changedIn=importtest --changedBy=Utb1
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch3.xml --format=xml --dupType=ISBNA --live --changedIn=importtest --changedBy=Utb1
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'bib'")
if (( $rowCount != 1 )) ; then
    fail "Expected single bib record (after IBSN10/13 test)"
fi
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'hold'")
if (( $rowCount != 1 )) ; then
    fail "Expected single hold record (after IBSN10/13 test)"
fi

cleanup
######## ISBN $z (hidden value) matching. Batch 4 contain the same data but with its ISBN in 020$z
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch4.xml --format=xml --live --changedIn=importtest --changedBy=Utb1
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch4.xml --format=xml --dupType=ISBNZ --live --changedIn=importtest --changedBy=Utb1
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'bib'")
if (( $rowCount != 1 )) ; then
    fail "Expected single bib record (after IBSN hidden value test)"
fi
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'hold'")
if (( $rowCount != 1 )) ; then
    fail "Expected single hold record (after IBSN hidden value test)"
fi

cleanup
######## ISSN $z (hidden value) matching. Batch 4 contain the same data but with its ISSN in 022$z
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch5.xml --format=xml --live --changedIn=importtest --changedBy=Utb1
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch5.xml --format=xml --dupType=ISSNZ --live --changedIn=importtest --changedBy=Utb1
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'bib'")
if (( $rowCount != 1 )) ; then
    fail "Expected single bib record (after ISSN hidden value test)"
fi
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'hold'")
if (( $rowCount != 1 )) ; then
    fail "Expected single hold record (after ISSN hidden value test)"
fi

cleanup
######## Introduce multiple duplicates (without checking) and make sure incoming records afterwards still place their
# holdings on at least one of the detected duplicates
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --live --changedIn=importtest --changedBy=Utb1
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --live --changedIn=importtest --changedBy=Utb1
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch1.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --changedBy=Utb1
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'bib'")
if (( $rowCount != 2 )) ; then
    fail "Expected exactly 2 bib records"
fi
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'hold'")
if (( $rowCount != 3 )) ; then
    fail "Expected exactly 3 hold records"
fi

cleanup
######## ISBN $z 10/13 matching. Batch 4 and 8 contain the same data but with varying forms of the same ISBN in 020$z.
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch4.xml --format=xml --live --changedIn=importtest --changedBy=Utb1
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch8.xml --format=xml --dupType=ISBNZ --live --changedIn=importtest --changedBy=Utb1
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'bib'")
if (( $rowCount != 1 )) ; then
    fail "Expected single bib record (after IBSN$z 10/13 test)"
fi
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'hold'")
if (( $rowCount != 1 )) ; then
    fail "Expected single hold record (after IBSN$z 10/13 test)"
fi

cleanup
# Should match an existing example record (on Voyager-style LIBRIS-ID), and only add the holding
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch10.xml --format=xml --dupType=LIBRIS-ID --live --changedIn=importtest --changedBy=Utb1
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'bib'")
if (( $rowCount != 0 )) ; then
    fail "Expected no bib record"
fi
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'hold'")
if (( $rowCount != 1 )) ; then
    fail "Expected single hold record"
fi

cleanup
# Test EAN match
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch11.xml --format=xml --live --changedIn=importtest --changedBy=Utb1
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch11.xml --format=xml --dupType=EAN --live --changedIn=importtest --changedBy=Utb1
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'bib'")
if (( $rowCount != 1 )) ; then
    fail "Expected single bib record (after EAN test)"
fi
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'hold'")
if (( $rowCount != 1 )) ; then
    fail "Expected single hold record (after EAN test)"
fi


cleanup
# Test special encoding level rules
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --changedBy=Utb1
bibResourceId=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["@id"]')

# batch13.xml is the same as batch 0, but with encoding level 5 instead of 8, and another title
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch13.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --replaceBib --changedBy=Utb1 --specialRule=5+8

newBibResourceId=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["@id"]')
if [ $newBibResourceId != $bibResourceId ]; then
    fail "Bib-replace altered the ID!"
fi
mainTitle=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["hasTitle"]|.[0]|.["mainTitle"]')
expect="\"Polisbilen får INTE ett larm\""
if [ "$mainTitle" != "$expect" ]; then
    fail "Data was not replaced!"
fi


cleanup
# (Sanity-)test the dynamic merge rules.
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --changedBy=Utb1
# Batch 14 adds (relative to batch 0) a summary, and has a different title. The title should be replaced, and the summary added as specified in mergerules0.json
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch14.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --changedBy=Utb2 --mergeBibUsing=./integtest/mergerules0.json
mainTitle=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["hasTitle"]|.[0]|.["mainTitle"]')
expect="\"Polisbilen får INTE ett larm\""
if [ "$mainTitle" != "$expect" ]; then
    fail "Merge: Data was not replaced!"
fi
summary=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["summary"]|.[0]|.["label"]')
expect="\"En JÄTTEJÄTTEFIN sammanfattning\""
if [ "$summary" != "$expect" ]; then
    fail "Merge: Data was not replaced!"
fi


cleanup
# Test the --ignoreNewBib flag. Import the second time without matching any thing. No new bib should be created, and the associated holds should also be discarded.
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --live --changedIn=importtest --changedBy=Utb1
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch15.xml --format=xml --live --changedIn=importtest --changedBy=Utb1 --ignoreNewBib
mainTitle=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["hasTitle"]|.[0]|.["mainTitle"]')
expect="\"ANNAN TITEL\""
if [ "$mainTitle" == "$expect" ]; then
    fail "Bib should not have been added/replaced/merged, yet the new title is there!"
fi
rowCount=$($PSQL -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'importtest' and collection = 'hold'")
if (( $rowCount != 1 )) ; then
    fail "Expected single hold record, as the other two holdings sat on a bib we're choosing to discard."
fi


#Rerun the simple bib-replace test, but with the --ignoreNewBib flag, to check that it does not interfere. batch1.xml contains the same data but with a changed title for the bib record and another 035$a
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --changedBy=Utb1
bibResourceId=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["@id"]')
java "$SECRET_PROPS" -jar build/libs/batchimport.jar --path=./integtest/batch1.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --changedIn=importtest --replaceBib --ignoreNewBib --changedBy=Utb1
newBibResourceId=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["@id"]')
if [ $newBibResourceId != $bibResourceId ]; then
    fail "Bib-replace altered the ID!"
fi
mainTitle=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["hasTitle"]|.[0]|.["mainTitle"]')
expect="\"Polisbilen får INTE ett larm\""
if [ "$mainTitle" != "$expect" ]; then
    fail "Data was not replaced!"
fi
systemNumbers=$($PSQL -qAt whelk_dev <<< "select data from lddb where changedIn = 'importtest' and collection = 'bib'" | jq '.["@graph"]|.[0]|.["identifiedBy"]|.[]|.["value"]')
if [[ "$systemNumbers" != *"NEW"* ]]; then
    fail "Systemnumber was not added!"
fi
if [[ "$systemNumbers" != *"OLD"* ]]; then
    fail "Systemnumber was not retained!"
fi


cleanup
popd
echo $OUTCOME
