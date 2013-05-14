#!/usr/bin/env bash

RESOURCE=$1
ORIGIN=$2
if [[ "$RESOURCE" == "" ]] || [[ $ORIGIN == "" ]]; then
    echo "Usage $(basename $0) <dump resource>"
    echo ""
    exit
fi

WEBAPPS=/var/lib/tomcat6/webapps/whelk
SETENV=/etc/tomcat6/tomcat6.conf

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib

source $SETENV

java $JAVA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator importdump libris $RESOURCE $ORIGIN
