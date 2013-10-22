#!/usr/bin/env bash

RESOURCE=$1
SINCE=$2
EXTRA_OPTS=$3
if [[ "$RESOURCE" == "" ]] || [[ $SINCE == "" ]]; then
    echo "Usage $(basename $0) <OAIPMH resource> <since date>"
    echo ""
    echo "  Example: Start an OAIPMH import from bib since july 24th 2012"
    echo "  \$ $(basename $0) bib 2012-07-24T14:02:00Z"
    echo ""
    exit
fi

WEBAPPS=/var/lib/tomcat6/webapps/whelk
SETENV=%(envscript)s

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib

source $SETENV

java $JAVA_OPTS $EXTRA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator import libris $RESOURCE $SINCE
