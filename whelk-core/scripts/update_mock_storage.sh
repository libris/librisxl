#!/usr/bin/env bash

datadir=$(dirname $0)/../src/test/resources/marc2jsonld
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
        dest_url=http://localhost:8080/whelk-core/$datatype/$id
#        if [ "$datatype" ==  "hold" ]; then
#            curl -XPUT -H "Content-type:application/json" -H "format:$fmt" -H "link:/bib/7149593" $dest_url --data-binary @$f
#        else    
            curl -XPUT -H "Content-type:application/json" -H "format:$fmt" $dest_url --data-binary @$f
#        fi
    done
done
