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



cleanup
######## Electronic/Instance should not match. Batch 6 contains the same record, but instance type is electronic
java -jar build/libs/batchimport.jar --path=./integtest/batch0.xml --format=xml --live
java -jar build/libs/batchimport.jar --path=./integtest/batch6.xml --format=xml --dupType=ISBNA --live
rowCount=$(psql -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'batch import' and collection = 'bib'")
if (( $rowCount != 2 )) ; then
    fail "Expected 2 bib records after importing Instance and Electronic"
fi
rowCount=$(psql -qAt whelk_dev <<< "select count(*) from lddb where changedIn = 'batch import' and collection = 'hold'")
if (( $rowCount != 2 )) ; then
    fail "Expected 2 bib records after importing Instance and Electronic"
fi


popd
echo $OUTCOME
