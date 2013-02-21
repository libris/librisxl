#!/usr/bin/env bash

for f in ../src/test/resources/marc2jsonld/in/bib/*.json; do id=`basename $f|awk -F'\.' '{print $1}'`; curl -XPUT -H "Content-type:application/json" -H "format:marc21" http://localhost:8080/whelk-core/bib/$id --data-binary @$f; done
