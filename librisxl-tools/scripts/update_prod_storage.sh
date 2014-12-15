#!/usr/bin/env bash

datadir=$(dirname $0)/../../whelk-extensions/src/test/resources/marc2jsonld
for d in $datadir/in/*; do
    datatype=`basename $d`
    for f in $datadir/in/$datatype/*.json; do
        id=`basename -s .json $f`
        dest_url=http://hp01.libris.kb.se:8080/whelk/$datatype/$id
        curl -XPUT -H "Content-type:application/x-marc-json+$datatype" $dest_url --data-binary @$f
    done
done
