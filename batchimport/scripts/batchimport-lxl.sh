#!/bin/bash

HOME=`dirname $0`
TRANSFORMERS=$HOME/transformers
QUEUES=$HOME/queues
ETC=$HOME/etc
ALERT_MAIL="kai.poykio@kb.se"
MAX_IMPORTS=2

bailout() {

if [ ! -f /tmp/import/bailedout ]; then
	touch /tmp/import/bailedout
	mailx -s "batchimport failed" $ALERT_MAIL <<-EOF
	`hostname`

	$1

	info: remove /tmp/import/bailedout when issue is resolved.
EOF
fi 

exit 1
}

if [ ! -d /tmp/import/running ]; then
	mkdir -p /tmp/import/running
fi

cat $ETC/import-lxl.txt|while read QUEUE FLAGS; do

	# REDO: this check is not idiotsafe...
	NR_IMPORTS=`ps -ef|grep -v grep|grep -c single-import-lxl.sh`
	NR_LOCKFILES=`ls -1 /tmp/import/running|wc -l`
	
	if [ "$NR_IMPORTS" -ne "$NR_LOCKFILES" ]; then
		bailout "fatal: nr of importprocesses don't match nr of lockfiles in /tmp/import/running"
	fi

	if [ "$NR_IMPORTS" -eq "$MAX_IMPORTS" ]; then
		exit 0
	fi

	if [[ "$QUEUE" == \#* ]]; then
		continue
	fi

	FLAGS=${FLAGS//[$'\r\n']} # delete \r\n
	FLAGS=`eval echo -n $FLAGS` # expansion of variables in string like $TRANSFORMERS

	if [ ! -f /tmp/import/running/$QUEUE ] && [ `ls -1 $QUEUES/$QUEUE/incoming|wc -l` -ne 0 ]; then
		( QUEUE=$QUEUE $HOME/single-import-lxl.sh $FLAGS 2>&1 ) &
		sleep 1
	fi
done
