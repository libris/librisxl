#!/usr/bin/env bash

dest_url=http://localhost:8080/whelk-core/bib
datadir=$(dirname $0)/../src/test/resources/marc2jsonld/in/bib
f=$datadir/7149593.json
curl -XPUT -H "Content-type:application/json" -H "format:marc21" -H "link:/auth/191503" $dest_url/7149593 --data-binary @$f


