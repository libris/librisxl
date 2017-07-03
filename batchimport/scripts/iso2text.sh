#!/bin/sh

HOME=`dirname $0`/..
JAVA=java
#JAVA=/System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Home/bin/java 
CLASSPATH=$HOME/lib/marc.jar:$HOME/lib/vrlin.jar
CTYPE=en_US
export CTYPE
 
$JAVA -Dfile.encoding=latin1 -classpath $CLASSPATH se.kb.libris.util.marc.io.InOut -outType=TEXT -inType=ISO2709 $*
