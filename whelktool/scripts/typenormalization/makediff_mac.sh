#!/bin/bash
set -euo pipefail

BASEPATH=$1
CONTEXTPATH=$2
echo ${BASEPATH}-instances.jsonl.gz

skiprecords() {
  awk -v RS=$'\n\n' -v ORS=$'\n\n' '$0 !~ "a :Record"'
}

(
  gzcat ${BASEPATH}-instances.jsonl.gz | trld -indjson -c $CONTEXTPATH -ottl
  gzcat ${BASEPATH}-works.jsonl.gz | trld -indjson -c $CONTEXTPATH -ottl
) > ${BASEPATH}-datain.ttl
(
  cat ${BASEPATH}-NORMALIZED.jsonl | trld -indjson -c $CONTEXTPATH -ottl
)  > ${BASEPATH}-normout.ttl

diffld -b ${BASEPATH}-datain.ttl ${BASEPATH}-normout.ttl -ottl |
  sed '
    s! {| :addedIn <qa-datain> |}!!g
    s! :addedIn <qa-datain> ;!!g
    s/:issuance\(Type\)\? "\(\w\+\)"/:issuance\1 :\2/
    s/"\(marc:[^"]\+\)"/\1/
    s|<qa-normout>|<#typenormalization>|
  ' |
  cat - > ${BASEPATH}-diff.ttl
