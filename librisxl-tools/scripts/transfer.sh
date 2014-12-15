#!/usr/bin/env bash

FROM_STORAGE=$1
TO_STORAGE=$2
EXTRA_OPTS=$3

WEBAPPS=%(tomcat_webapps)s/whelk
SETENV=%(envscript)s

CLASSES=$WEBAPPS/WEB-INF/classes
LIBS=$WEBAPPS/WEB-INF/lib

source $SETENV

java $JAVA_OPTS -Ddisable.plugins="indexingprawn" -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false $EXTRA_OPTS -cp $CLASSES:$LIBS/* se.kb.libris.whelks.WhelkOperator -o transfer -w libris --fromStorage $FROM_STORAGE --toStorage $TO_STORAGE
