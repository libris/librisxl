#!/bin/bash

usage () {
    cat <<EOF
Usage: $0 -n <WHELK_NAME> -c <COLLECTION_ID>
EOF
}

while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--whelk-name)
            WHELK_ID="$2"
            shift
            ;;
        -c|--collection)
            COLLECTION_ID="$2"
            shift
            ;;
        *)
            usage
            exit 1
            ;;
    esac
    shift
done

if [ -z "$WHELK_ID" ] || [ -z "$COLLECTION_ID" ]; then
    usage
    exit 1
fi

set -x
psql $WHELK_ID -c "delete from lddb where collection = '$COLLECTION_ID';"
psql $WHELK_ID -c "DELETE FROM lddb__identifiers WHERE id IN (SELECT id FROM lddb WHERE collection = '$COLLECTION_ID');"
psql $WHELK_ID -c "DELETE FROM lddb__versions WHERE collection = '$COLLECTION_ID';"

curl -XDELETE "http://localhost:9200/$WHELK_ID/$COLLECTION_ID/_query" -d '{"query":{"match_all": {}}}'
