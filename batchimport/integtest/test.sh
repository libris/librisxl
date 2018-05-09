#!/bin/bash

# NEVER RUN THIS TEST CASE ON AN ENVIRONMENT WHERE THE DATA MATTERS!
# This test case assumes you have built the batch_import jar file with a relevant secret.properties baked in.
# It also assumes that 'jq' is installed on the system.
# It also assumes it is ok to delete all data with changedIn = 'batch import' as a method for cleaning up!

function cleanup {
    psql whelk_dev <<< "delete from lddb__identifiers where id in (select id from lddb where changedIn = 'batch import');"
    psql whelk_dev <<< "delete from lddb__dependencies where id in (select id from lddb where changedIn = 'batch import');"
    psql whelk_dev <<< "delete from lddb where changedIn = 'batch import';"
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
java -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live
bibResourceId=$(psql -qAt whelk_dev <<< "select data from lddb where changedIn = 'batch import' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["@id"]')
itemOf=$(psql -qAt whelk_dev <<< "select data from lddb where changedIn = 'batch import' and collection = 'hold'" | jq '.["@graph"]|.[1].itemOf|.["@id"]')
if [ $itemOf != $bibResourceId ]; then
    fail "Normal import"
fi

######## RERUN SAME FILE, NO MORE DATA
# Re-run same file, should result in no changes
java -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live
rowCount=$(psql -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'batch import' and collection = 'bib'")
if (( $rowCount != 1 )) ; then
    fail "Expected single bib record"
fi
rowCount=$(psql -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'batch import' and collection = 'hold'")
if (( $rowCount != 1 )) ; then
    fail "Expected single hold record"
fi

######## REPLACE RECORD. batch1.xml contains the same data but with a changed title for the bib record.
java -jar build/libs/batchimport.jar --path=./integtest/batch1.xml --format=xml --dupType=ISBNA,ISBNZ,ISSNA,ISSNZ,035A --live --replaceBib
newBibResourceId=$(psql -qAt whelk_dev <<< "select data from lddb where changedIn = 'batch import' and collection = 'bib'" | jq '.["@graph"]|.[1]|.["@id"]')
if [ $newBibResourceId != $bibResourceId ]; then
    fail "Bib-replace altered the ID!"
fi



popd
#cleanup
echo $OUTCOME
