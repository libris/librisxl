#!/bin/bash
set -euo pipefail
BASEPATH=$1
CONTEXT=$(dirname $0)/../../../../definitions/build/sys/context/kbv.jsonld
(
  zcat ${BASEPATH}-instances.jsonl.gz | trld -indjson -c $CONTEXT -ottl
  zcat ${BASEPATH}-works.jsonl.gz | trld -indjson -c $CONTEXT -ottl
) > /tmp/datain.ttl
(
  cat $BASEPATH-NORMALIZED.jsonl | trld -indjson -c $CONTEXT -ottl
) > /tmp/normout.ttl

diffld -b /tmp/datain.ttl /tmp/normout.ttl -ottl |
  sed '
    s! {| :addedIn </tmp/datain> |}!!g
    s! :addedIn </tmp/datain> ;!!g
    s/:issuance\(Type\)\? "\(\w\+\)"/:issuance\1 :\2/
    s/"\(marc:[^"]\+\)"/\1/
    s|</tmp/normout>|<#typenormalization>|
  ' |
  awk -v RS=$'\n\n' -v ORS=$'\n\n' '$0 !~ "a :Record"' |
  cat - > /tmp/diff.ttl
