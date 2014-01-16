#!/usr/bin/env bash

DATASET=$1
SINCE=""
EXTRA_OPTS=$3
if [[ "$DATASET" == "" ]]; then
    echo "Usage $(basename $0) <OAIPMH resource> <since date>"
    echo ""
    echo "  Example: Start an OAIPMH import from bib since july 24th 2012:"
    echo "  \$ $(basename $0) bib 2012-07-24T14:02:00Z"
    echo ""
    echo "  Start an OAIPMH import from hold since the beginning of time:"
    echo "  \$ $(basename $0) bib"
    echo ""
    exit
fi
if [[ "$2" != "" ]]; then
    SINCE="-s $2 --silent"
fi

WEBAPPS=/var/lib/tomcat6/webapps/whelk
SETENV=%(envscript)s

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib

source $SETENV

java $JAVA_OPTS -Ddisable.plugins="indexingprawn" $EXTRA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator -o import -w libris -d $DATASET $SINCE
