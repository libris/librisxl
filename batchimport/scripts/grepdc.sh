#!/bin/sh

HOME=`dirname $0`/..
JAVA=java
#JAVA=/System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Home/bin/java
CLASSPATH=$HOME/lib/marc.jar:$HOME/lib/vrlin.jar:$HOME/dist/import2.jar
 
$JAVA -classpath $CLASSPATH se.kb.libris.import2.GrepDc $*
