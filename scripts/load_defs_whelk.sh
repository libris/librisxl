#!/usr/bin/env bash

WHELK="http://localhost:8180/whelk-webapi"
BUILDBASE="datatools/build"

put() {
    echo "PUT '$1' to <$3> as $2"
    curl -XPUT -H "Content-Type:$2" --data-binary @$1 $3?data
}

# Load JSON-LD contexts
for ctx in owl skos; do
    put datatools/sys/context/${ctx}.jsonld application/ld+json ${WHELK}/sys/context/${ctx}.jsonld
done

# Load standalone documents
for doc in terms schemes; do
    put $BUILDBASE/${doc}.jsonld application/ld+json ${WHELK}/def/$doc
done

# Load datasets
for dataset in languages countries nationalities relators enum/{content,carrier,record}; do
    for file in $BUILDBASE/$dataset/*.jsonld; do
        slug=$(basename -s .jsonld $file)
        put $file application/ld+json ${WHELK}/def/${dataset}/$slug
    done
done
