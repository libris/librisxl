#!/usr/bin/env bash

DATASET=$1
EXTRA_OPTS=$2
if [[ "$DATASET" == "" ]]; then
    echo "Usage $(basename $0) <name of dataset>"
    echo ""
    exit
fi

WEBAPPS=/var/lib/tomcat6/webapps/whelk
SETENV=%(envscript)s

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib

source $SETENV

java $JAVA_OPTS -Ddisable.plugins="indexingprawn" $EXTRA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator -o linkfindandcomplete -w libris -d $DATASET
