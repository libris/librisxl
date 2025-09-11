#!/bin/sh
offset=${1:-0}
curl -s https://libris.kb.se/sparql -HAccept:text/csv --data-urlencode query="
$(cat get-candidates-for-isni-enrichment.rq)
limit 10000 offset $offset
"
