#!/usr/bin/env bash

DATASET=""
EXTRA_OPTS=$2
if [[ "$1" != "" ]]; then
    DATASET="-d $1"
fi

WEBAPPS=%(tomcat_webapps)s/whelk
SETENV=%(envscript)s

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib

source $SETENV

java $JAVA_OPTS -Ddisable.plugins="indexingprawn" $EXTRA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator -o reindex -w libris $DATASET
