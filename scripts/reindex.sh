#!/usr/bin/env bash

RESOURCE=$1
EXTRA_OPTS=$2

WEBAPPS=/var/lib/tomcat6/webapps/whelk
SETENV=%(envscript)s

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib

source $SETENV

java $JAVA_OPTS $EXTRA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator reindex libris $RESOURCE
