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

WEBAPPS=/var/lib/tomcat6/webapps/whelk-core
CONFIG=file:/data/librisxl/whelks.json

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib
DATE=$(date -v-$GOBACK +"%Y-%m-%dT%H:%M:%SZ")

echo "get OAIPMH for $RESOURCE since $DATE"

#java -Dwhelk.config.uri=$CONFIG -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator import auth auth
