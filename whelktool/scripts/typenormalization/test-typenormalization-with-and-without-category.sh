#!/bin/bash
set -euo pipefail

LIBRISENV=$1
TESTFOLDER=$2

gzip -k ../../${TESTFOLDER}/qa-instances.jsonl > ../../${TESTFOLDER}/qa-instances.jsonl.gz
gzip -k ../../${TESTFOLDER}/qa-works.jsonl > ../../${TESTFOLDER}/qa-works.jsonl.gz

time java -Xmx2G -Dxl.secret.properties=../secret.properties-${LIBRISENV} -Dtypenormalization=simple-types-algorithm -DaddCategory=true -Dnowhelk.datadir=../../${TESTFOLDER} -Dnowhelk.basename=qa -jar build/libs/whelktool.jar --report ../../${TESTFOLDER}/reports/typenorm-with-category$(date +%Y%m%d) --dry-run --no-threads scripts/typenormalization/nowhelk.groovy

time java -Xmx2G -Dxl.secret.properties=../secret.properties-${LIBRISENV} -Dtypenormalization=simple-types-algorithm -DaddCategory=false -Dnowhelk.datadir=../../${TESTFOLDER} -Dnowhelk.basename=qa -jar build/libs/whelktool.jar --report ../../${TESTFOLDER}/reports/typenorm-$(date +%Y%m%d) --dry-run --no-threads scripts/typenormalization/nowhelk.groovy