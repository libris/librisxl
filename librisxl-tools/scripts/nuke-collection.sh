#!/bin/bash
COLLECTION_ID=$1
[[ "$COLLECTION_ID" == '' ]] && echo "USAGE $0 COLLECTION_ID" && exit 1
set -x
psql whelk -c "delete from lddb where manifest->>'collection' = '$COLLECTION_ID';"
curl -XDELETE "http://localhost:9200/whelk/$COLLECTION_ID/_query" -d '{"query":{"match_all": {}}}'
