#!/bin/bash
set -euo pipefail

# A testfolder located ../../ two levels up from whelktool, i.e. beside the librisxl repo
TESTFOLDER=$1
TESTRECORDS=${2:-./scripts/typenormalization/tailored_examples.jsonl}

echo "Using tailored examples file: $TESTRECORDS"
echo "Saving results to test folder: $TESTFOLDER"

# Eun this from the whelktool folder
# Zip the tailored examples file and save it in the parent folder of the librixl reop
echo "Creating compressed tailored examples file..."
gzip -c -k -f $TESTRECORDS > ../../$TESTFOLDER/qa-instances.jsonl.gz

# Run the typenormalization script on the tailored examples and save as qa-NORMALIZED-with-category.jsonl
echo "Running typenormalization on tailored examples..."
time java -Xmx2G -Dxl.secret.properties=../secret.properties-dev2 -Dtypenormalization=simple-types-algorithm -DaddCategory=true -DreplaceIssuanceTypes=false -Dnowhelk.datadir=../../$TESTFOLDER -Dnowhelk.basename=qa -jar build/libs/whelktool.jar --dry-run --no-threads scripts/typenormalization/nowhelk.groovy

# Run tests on the output
echo "Running tests on normalized tailored examples..."
python3 scripts/typenormalization/run_tailored_tests.py ../../$TESTFOLDER/qa-NORMALIZED-with-category.jsonl