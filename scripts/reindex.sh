#!/usr/bin/env bash

TYPE=$1
RESOURCE=$2
ORIGIN=$3
EXTRA_OPTS=$4
if [[ "$RESOURCE" == "" ]] || [[ $ORIGIN == "" ]]; then
    echo "Usage $(basename $0) <importfile|importdump> <file|url> <bib|hold|auth>"
    echo ""
    exit
fi

WEBAPPS=/var/lib/tomcat6/webapps/whelk
SETENV=/etc/tomcat6/tomcat6.conf

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib

source $SETENV

java $JAVA_OPTS $EXTRA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator reindex libris $RESOURCE $ORIGIN
