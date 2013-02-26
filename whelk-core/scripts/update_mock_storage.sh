#!/usr/bin/env bash

datadir=$(dirname $0)/../src/test/resources/marc2jsonld

for f in $datadir/in/bib/*.json; do
    id=`basename -s .json $f`
    fmt=marc21
    expected=$datadir/expected/bib/$id.json
    if [[ -e $expected ]]; then
        f=$expected
        fmt=jsonld
    fi
    dest_url=http://localhost:8080/whelk-core/bib/$id
    curl -XPUT -H "Content-type:application/json" -H "format:$fmt" $dest_url --data-binary @$f
done
