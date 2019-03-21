#!/bin/bash
ENV=${1:-dev}
LIMIT=${2:-1000}
BIBDUMP=/tmp/lddb-$ENV-bib-records

# Download a temporary dump of (embellished) instance records.
# Takes a couple of minutes.
if [[ ! -d $BIBDUMP ]]; then
    mkdir -p $BIBDUMP
    pushd $BIBDUMP
    curl -s "https://libris-$ENV.kb.se/find.jsonld?@type=Instance&_limit=$LIMIT" | python -c '
import sys, json
data = json.load(sys.stdin)
for item in data["items"]:
    url = item["@id"].rsplit("#", 1)[0]
    xlid = url.rsplit("/", 1)[-1]
    print("curl -s -HAccept:application/ld+json -o {}.jsonld {}".format(xlid, url))
' | bash
    popd
fi

# Revert all in the dump (takes ~30 s).
# Writes to stdout (redirect this to a file of your choice to diff).
pushd $(dirname $0)/../../whelk-core
../gradlew runMarcFrame -Dargs="revert $(find $BIBDUMP -name '*.jsonld')" 2>/tmp/stderr.txt
popd
