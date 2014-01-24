#!/usr/bin/env bash

WHELK="http://localhost:8180/whelk-webapi"
INPUTPATH="/tmp"
RESOURCE=$1

for f in $INPUTPATH/$RESOURCE/*; do
    s=`basename -s .jsonld $f`
    curl -XPUT -H "Content-Type:application/ld+json" ${WHELK}/def/${RESOURCE}/$s --data-binary @$f
done
