#!/bin/bash
set -euo pipefail

BASEPATH=$1
DIFFLAG="${2:-}"
CONTEXT=$(dirname $0)/../../../../definitions/build/sys/context/kbv.jsonld

skiprecords() {
  awk -v RS=$'\n\n' -v ORS=$'\n\n' '$0 !~ "> a :Record"'
}

(
  zcat ${BASEPATH}-instances.jsonl.gz | trld -indjson -c $CONTEXT -ottl
  zcat ${BASEPATH}-works.jsonl.gz | trld -indjson -c $CONTEXT -ottl
) | skiprecords > /tmp/datain.ttl
(
  cat $BASEPATH-NORMALIZED-with-category.jsonl | trld -indjson -c $CONTEXT -ottl
) | skiprecords > /tmp/normout.ttl

if [[ "$DIFFLAG" = "-D" ]]; then
  exit
fi

diffld -b /tmp/datain.ttl /tmp/normout.ttl -ottl |
  sed '
    s! {| :addedIn </tmp/datain> |}!!g
    s! :addedIn </tmp/datain> ;!!g
    s/:issuance\(Type\)\? "\(\w\+\)"/:issuance\1 :\2/
    s/"\(marc:[^"]\+\)"/\1/
    s|</tmp/normout>|<#typenormalization>|
  ' |
  skiprecords |
  cat - > /tmp/diff.ttl
