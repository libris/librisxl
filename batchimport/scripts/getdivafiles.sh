#!/bin/bash -x
# KP 20180112, reworked to counter failing divaserver

DATE=`date +"%Y-%m-%d"`
#DATE='2017-01-01'
HOME=/appl/import
FAILED_DATE=$HOME/prev/diva_failed
TMP=/tmp/diva
DEST=$HOME/queues/diva/incoming

if [ -f "$FAILED_DATE" ]; then
	DO_DATE=`cat $FAILED_DATE` 
else 
	DO_DATE="$DATE"
fi 

if [ ! -d "$TMP" ]; then
	mkdir $TMP 
else 
	rm $TMP/????-??-??*
fi

$HOME/scripts/oaiharvest.py "http://www.diva-portal.org/dice/oai?verb=ListRecords&metadataPrefix=marc21&set=libris&from=$DO_DATE" $TMP/$DO_DATE

if [ $? -eq 0 ]; then
	FIRST=`ls -1tr $TMP/$DO_DATE*|head -1`
	if [ "$FIRST" != '' ] && [ `grep -ic '<error' $FIRST` -eq 0 ]; then
		echo "info: retrieved diva records." 
		mv $TMP/* $DEST
		rm $FAILED_DATE
	else
		echo "fatal: failed to retrieve diva records." 
		printf "$DO_DATE" > $FAILED_DATE
	fi
else
	echo "fatal: failed to retrieve diva records." 
	printf "$DO_DATE" > $FAILED_DATE
fi

if [ -f "$FAILED_DATE" ]; then
	mailx -s "import diva: failed $DO_DATE" kai.poykio@kb.se </dev/null
fi

#$HOME/scripts/oaiharvest.py "http://www.diva-portal.org/dice/oai?verb=ListRecords&metadataPrefix=marc21&set=libris&from=$DATE" $HOME/queues/diva/incoming/$DATE

# Deprecated, electronic via eplikt
#$HOME/scripts/oaiharvest.py "http://www.diva-portal.org/dice/oai?verb=ListRecords&metadataPrefix=marc21electronic&set=libris-electronic&from=$DATE" $HOME/queues/diva/incoming/"$DATE"_electronic
