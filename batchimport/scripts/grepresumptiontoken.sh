#!/bin/sh

HOME=`dirname $0`/..
JAVA=java

$JAVA -classpath $HOME/dist/import2.jar se.kb.libris.import2.GrepResumptionToken $*
