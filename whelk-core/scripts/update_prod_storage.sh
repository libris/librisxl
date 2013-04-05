#!/usr/bin/env bash

datadir=$(dirname $0)/../src/test/resources/marc2jsonld
for d in $datadir/in/*; do
    datatype=`basename $d`
    for f in $datadir/in/$datatype/*.json; do
        id=`basename -s .json $f`
        fmt=marc21
        dest_url=http://hp04.libris.kb.se:8080/whelk-core/$datatype/$id
        curl -XPUT -H "Content-type:application/json" -H "format:$fmt" $dest_url --data-binary @$f
    done
done
