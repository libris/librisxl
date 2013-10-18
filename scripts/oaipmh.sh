#!/usr/bin/env bash

RESOURCE=$1
GOBACK=$2
if [[ "$RESOURCE" == "" ]] || [[ $GOBACK == "" ]]; then
    echo "Usage $(basename $0) <OAIPMH resource> <time back hours>"
    echo ""
    echo "  Example: Start an OAIPMH import from bib since $(date -d '-24 hour' +'%Y-%m-%d')"
    echo "  \$ $(basename $0) bib 24"
    echo ""
    exit
fi

WEBAPPS=/var/lib/tomcat6/webapps/whelk-core
SETENV=/usr/share/tomcat6/bin/setenv.sh

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib
SINCE=$(date -d "-$GOBACK hour" +"%Y-%m-%dT%H:%M:%SZ")

source $SETENV

java $JAVA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator import $RESOURCE $RESOURCE $SINCE
