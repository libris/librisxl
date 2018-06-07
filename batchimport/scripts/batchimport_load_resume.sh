#!/bin/bash -x
# batchimport load autoresume
# Kai P, 120117

MAIL='kai.poykio@kb.se'
QUEUE=$1
TYPE=$2 #xml|iso

if [ "$QUEUE" = '' ] || [ "$TYPE" = '' ]; then
	echo "Usage: $0 QUEUE TYPE(xml|iso)"
	exit
fi

bo ()
{
	mailx -s "$QUEUE resume failed" $MAIL <<EOF
		$1
EOF
	exit
}

SCRIPTS="/appl/import/scripts"
QUEUE_PATH="/appl/import/queues/$QUEUE"

LAST_LOG=`ls -tr $QUEUE_PATH/log/*.log|tail -1`
if [ "$LAST_LOG" != '' ]; then
	LAST_LOG=${LAST_LOG##*/}
	LOAD_FILE=${LAST_LOG%.log}
	if [ ! -f $QUEUE_PATH/done/$LOAD_FILE ]; then
		bo "No loadfile"
		#exit
	fi
	RECORDS=`cat $QUEUE_PATH/log/$LAST_LOG | $SCRIPTS/countrecords.pl`
	if [ "$TYPE" = 'xml' ]; then # marcxml
		TO_BE_LOADED=`cat $QUEUE_PATH/done/$LOAD_FILE|perl -e '$i=0;while(<>){$i++ if (/<record(?:\s+type=\"bibliographic\")?>/io);};print $i;'`
		#TO_BE_LOADED=`grep -c '<record>' $QUEUE_PATH/done/$LOAD_FILE`
	elif [ "$TYPE" = 'elibxml' ]; then
		TO_BE_LOADED=`cat $QUEUE_PATH/done/$LOAD_FILE|perl -e '$i=0;while(<>){$i++ if (/<Product>/io);};print $i;'`
	else #iso2709
		TO_BE_LOADED=`cat $QUEUE_PATH/done/$LOAD_FILE|$SCRIPTS/iso2709records.pl`
	fi
else
	bo "No last_log"
	#exit
fi

if [ "$RECORDS" -lt "$TO_BE_LOADED" ]; then

	DATE=`date "+%Y%m%d%H%M"`
	NEW_LOAD_FILE=$QUEUE.$DATE

	cat $QUEUE_PATH/done/$LOAD_FILE | $SCRIPTS/nthrecord.pl $RECORDS $TYPE > $QUEUE_PATH/done/$NEW_LOAD_FILE

	if [ $? -eq 0 ]; then
		mv -f $QUEUE_PATH/done/$NEW_LOAD_FILE $QUEUE_PATH/incoming
		if [ $? -ne 0 ]; then
			bo "critical: mv failed"
			#exit
		fi
		rm -f  $QUEUE_PATH/done/$LOAD_FILE
		rm -f $QUEUE_PATH/log/$LAST_LOG
	else
		bo "critical: nthrecord failed"
		#exit
	fi
else
	# DONE!
	rm -f  $QUEUE_PATH/done/$LOAD_FILE
	rm -f $QUEUE_PATH/log/$LAST_LOG
fi
