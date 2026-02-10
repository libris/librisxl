#!/bin/bash
set -euo pipefail

# A testfolder (suggestion: ../../testdata, i.e. beside the librisxl repo)
ENV=$1
TESTFOLDER=$2
TESTRECORDS=${3:-./scripts/typenormalization/tailored_examples.jsonl}

echo "Running normalization and tests against environment: $ENV"
echo "Using tailored examples file: $TESTRECORDS"
echo "Saving results to test folder: $TESTFOLDER"

# Eun this from the whelktool folder
# Zip the tailored examples file and save it in the parent folder of the librixl reop
echo "Creating compressed tailored examples file..."
gzip -c -k -f $TESTRECORDS > $TESTFOLDER/tailored-instances.jsonl.gz

# Empty works file (all test works are unlinked, local works)
echo -n '' | gzip > $TESTFOLDER/tailored-works.jsonl.gz

echo "Rebuild whelktool"
../gradlew jar

# Run the typenormalization script on the tailored examples and save as tailored-normalized.jsonl
echo "Running typenormalization on tailored examples..."
time java -Xmx2G -Dxl.secret.properties=../secret.properties-$ENV -Dtypenormalization=simple-types-algorithm -Dnowhelk.datadir=$TESTFOLDER -Dnowhelk.basename=tailored -jar build/libs/whelktool.jar --dry-run --no-threads scripts/typenormalization/nowhelk.groovy

# Run tests on the output
echo "Running tests on normalized tailored examples..."
python3 scripts/typenormalization/run_tailored_tests.py $TESTFOLDER/tailored-normalized.jsonl
