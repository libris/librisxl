#!/bin/sh

HOME=`dirname $0`/..
#JAVA=/usr/jdk/jdk1.6.0_11/bin/java
JAVA=/usr/jdk/latest/bin/java
#JAVA=/System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Home/bin/java
#CLASSPATH=$HOME/dist/import2.jar:$HOME/lib/saxon9he.jar:$HOME/lib/xercesImpl.jar:$HOME/lib/serializer.jar:$HOME/lib/xml-apis.jar
CLASSPATH=$HOME/dist/import2.jar:$HOME/lib/xalan.jar:$HOME/lib/xercesImpl.jar:$HOME/lib/serializer.jar:$HOME/lib/xml-apis.jar
#CLASSPATH=$HOME/dist/import2.jar

#$JAVA -classpath $CLASSPATH org.apache.xalan.xslt.EnvironmentCheck
#$JAVA -verbose:class -Xmx2g -classpath $CLASSPATH se.kb.libris.import2.XsltTransform $*
$JAVA -Xmx2g -classpath $CLASSPATH se.kb.libris.import2.XsltTransform $*
