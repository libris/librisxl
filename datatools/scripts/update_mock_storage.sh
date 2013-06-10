#!/usr/bin/env bash

datadir=$(dirname $0)/../../whelk-extensions/src/test/resources/marc2jsonld
use_expected=$1

for d in $datadir/in/*; do
    datatype=`basename $d`
    for f in $datadir/in/$datatype/*.json; do
        id=`basename -s .json $f`
        fmt=marc21
        expected=$datadir/expected/$datatype/$id.json
        if [[ -e $expected && $use_expected == "-e" ]]; then
            f=$expected
            fmt=jsonld
        fi
        dest_url=http://localhost:8080/whelk-webapi/$datatype/$id
        echo "Putting $datatype/$id to <$dest_url> .."
        curl -XPUT -H "Content-type:application/x-marc-json" $dest_url --data-binary @$f
    done
done
