#!/bin/sh -x

JAVA=/usr/jdk/latest/bin/java
CLASSPATH=/appl/import2/lib

bo() {
	tail ./mtm.log | mailx -s "$1" kai.poykio@kb.se
	exit 1
}

cd /appl/import2/queues/mtm || bo "mtm cd failed"

$JAVA -Xmx2g -cp $CLASSPATH Mtm >./mtm.xml 2>./mtm.log

if [ $? -eq 0 ]; then
	mv ./mtm.xml ./incoming
else
	bo "mtm load error"
fi
