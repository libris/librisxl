#!/bin/sh

HOME=`dirname $0`/..
JAVA=/usr/jdk/jdk1.6.0_11/bin/java
#JAVA=/System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Home/bin/java
CLASSPATH=$HOME/dist/import2.jar

$JAVA -Xmx512m -classpath $CLASSPATH se.kb.libris.import2.XsltTransform $*
