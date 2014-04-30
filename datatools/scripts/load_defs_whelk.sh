#!/usr/bin/env bash

WHELK="http://localhost:8180/whelk-webapi"
BUILDBASE="datatools/build"

for dataset in languages countries enum/{content,carrier,record}; do
    for f in $BUILDBASE/$dataset/*.jsonld; do
        s=$(basename -s .jsonld $f)
        curl -XPUT -H "Content-Type:application/ld+json" ${WHELK}/def/${dataset}/$s --data-binary @$f
    done
done
