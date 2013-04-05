#!/usr/bin/env bash

RESOURCE=$1
GOBACK=$2
if [[ "$RESOURCE" == "" ]] || [[ $GOBACK == "" ]]; then
    echo "Usage $(basename $0) <OAIPMH resource> <time back in days (d) or hours (H)>"
    echo ""
    echo "  Example: Start an OAIPMH import from bib since $(date -v-1d +'%Y-%m-%d')"
    echo "  \$ $(basename $0) bib 1d"
    echo ""
    exit
fi

CLASSES=wartest/WEB-INF/classes
LIBS=wartest/WEB-INF/lib
CONFIG=file:../librisxl/whelk-core/src/main/resources/mock_whelks.json
DATE=$(date -v-$GOBACK +"%Y-%m-%dT%H:%M:%SZ")

echo "get OAIPMH for $RESOURCE since $DATE"

#java -Dwhelk.config.uri=$CONFIG -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator import auth auth
