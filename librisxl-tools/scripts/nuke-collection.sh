#!/bin/bash
WHELK_ID=$1
COLLECTION_ID=$2
[[ "$COLLECTION_ID" == '' ]] && echo "USAGE $0 WHELK_ID COLLECTION_ID" && exit 1
set -x
psql $WHELK_ID -c "delete from lddb where manifest->>'collection' = '$COLLECTION_ID';"
curl -XDELETE "http://localhost:9200/$WHELK_ID/$COLLECTION_ID/_query" -d '{"query":{"match_all": {}}}'
