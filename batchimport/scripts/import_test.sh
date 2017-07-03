#!/bin/sh

HOME=`(cd \`dirname $0\`/..;pwd)`
JAVA=java
#JAVA=/System/Library/Frameworks/JavaVM.framework/Versions/1.5.0/Home/bin/java

LC_CTYPE=en_US
export LC_CTYPE

flags="--inType=XML --errFile=/tmp/test/err.log --logFile=/tmp/test/out.log --redirectFile=/tmp/test/outputfile -sort -v -debug -merge -firstHolding -mergeHoldings --dupType=LIBRIS-ID,ISNA,ISNZ,ISBNZ,ISBNA,035A --inEncoding=UTF-8 --inType=XML --mergeStylesheet=$HOME/etc/forvarv-merge.xsl --mergeHoldingsStylesheet=$HOME/etc/dawson-holdings-merge.xsl"

$JAVA -Dfile.encoding=ISO-8859-1 -Xmx512m -classpath $HOME/lib/marc.jar:$HOME/dist_test/import2_redirect.jar:$HOME/lib/ojdbc14.jar:$HOME/lib/batchcat.jar:$HOME/lib/isbntools.jar se.kb.libris.import2.Import --inFile=$1 $flags $HOME/etc/import_test.properties
