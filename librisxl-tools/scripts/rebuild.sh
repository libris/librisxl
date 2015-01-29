#!/usr/bin/env bash

DATASET=""
EXTRA_OPTS=$2
if [[ "$1" != "" ]]; then
    DATASET="-d $1"
fi

WEBAPPS=/var/lib/tomcat6/webapps/whelk
SETENV=%(envscript)s

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib

source $SETENV

java $JAVA_OPTS -Ddisable.plugins="indexingprawn" -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false $EXTRA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator -o reindex --fromStorage oaipmhstorage -w libris $DATASET
