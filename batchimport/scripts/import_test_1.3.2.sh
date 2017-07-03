#!/bin/sh

HOME=`(cd \`dirname $0\`/..;pwd)`

JAVA=/usr/jdk/latest/bin/java
#JAVA=/System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Home/bin/java

# KP 121215 added xalan2.7.1 libs
CLASSPATH=$HOME/dist/import2.jar:$HOME/lib/xalan.jar:$HOME/lib/xercesImpl.jar:$HOME/lib/serializer.jar:$HOME/lib/xml-apis.jar:$HOME/lib/marc-1.3.2.jar:$HOME/lib/ojdbc14.jar:$HOME/lib/batchcat.jar:$HOME/lib/isbntools.jar

LC_CTYPE=en_US
export LC_CTYPE

#$JAVA -Dfile.encoding=ISO-8859-1 -Xmx512m -classpath $HOME/lib/marc.jar:$HOME/dist/import2.jar:$HOME/lib/ojdbc14.jar:$HOME/lib/batchcat.jar:$HOME/lib/isbntools.jar se.kb.libris.import2.Import $*

$JAVA -Dfile.encoding=ISO-8859-1 -Xmx512m -classpath $CLASSPATH se.kb.libris.import2.Import $*

