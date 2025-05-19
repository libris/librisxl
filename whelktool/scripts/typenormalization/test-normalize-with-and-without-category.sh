#!/bin/bash
set -euo pipefail

LIBRISENV=$1
TESTFOLDER=$2

# 1. One way or another, create files qa-instance.jsonl and qa-work.jsonl
# For example, you could use dump-ldbb-excerpts.sh or download a single record as jsonl from the UI
# Or using curl:
# curl -XGET -H "Accept: application/ld+json" https://libris-qa.kb.se/bvnqr36n2w6jl6f\?embellished\=false

# 2. Create zipped copies of files qa-instance.jsonl and qa-work.jsonl, retaining the original jsnol file
gzip -k -f ../../${TESTFOLDER}/qa-instances.jsonl > ../../${TESTFOLDER}/qa-instances.jsonl.gz
gzip -k -f ../../${TESTFOLDER}/qa-works.jsonl > ../../${TESTFOLDER}/qa-works.jsonl.gz

# 3. Run the typenormalization script on instances and works to nromalize with category as qa-NORMALIZED-with-category.jsonl
time java -Xmx2G -Dxl.secret.properties=../secret.properties-${LIBRISENV} -Dtypenormalization=simple-types-algorithm -DaddCategory=true -Dnowhelk.datadir=../../${TESTFOLDER} -Dnowhelk.basename=qa -jar build/libs/whelktool.jar --report ../../${TESTFOLDER}/reports/with-category --dry-run --no-threads scripts/typenormalization/nowhelk.groovy

# 4. Run the typenormalization on instances and works script to nromalize without category as qa-NORMALIZED.jsonl
time java -Xmx2G -Dxl.secret.properties=../secret.properties-${LIBRISENV} -Dtypenormalization=simple-types-algorithm -DaddCategory=false -Dnowhelk.datadir=../../${TESTFOLDER} -Dnowhelk.basename=qa -jar build/libs/whelktool.jar --report ../../${TESTFOLDER}/reports/without-category --dry-run --no-threads scripts/typenormalization/nowhelk.groovy

# 5. Skriv ut testsammanfattningar
python3 ./scripts/typenormalization/review_tests.py ../../${TESTFOLDER}/qa-instances.jsonl

# 6. Compare the original works and instances files, qa-NORMALIZED-with-category.jsonl and qa-NORMALIZED.jsonl

