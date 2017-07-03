#!/bin/ksh -x
# 120911 KP

CUST=publit
FILE="$CUST.xml"
DEST="/appl/import2/queues/$CUST"
INCOMING="$DEST/incoming"
PREV="$DEST/prev/$FILE.prev"
DATE=`date "+%Y-%m-%d"`
MAIL="kai.poykio@kb.se"

# Get Publit
#/usr/local/bin/curl -G -u kungligabiblioteket:c0c7325P4nN13L "http://www.publit.se/api/products?sigel=true&start=0&end=0" -o $DEST/$FILE
/usr/local/bin/curl -G -u kungligabiblioteket:c0c7325P4nN13L "http://www.publit.se/api/products?start=0&end=0" -o $DEST/$FILE

# Check if file exists, larger than zero
if [ $? -eq 0 ] && [ -s $DEST/$FILE ]; then
	if [ `/usr/bin/tail -12c $DEST/$FILE` != '</response>' ]; then
		mailx -s "Import2: $CUST $DATE failed" $MAIL </dev/null
		exit			
	fi
  echo "$DATE: File found: `ls -l $DEST/$FILE`"
  # prettyprint infile, diff is linebased NOT blockbased...
  cat $DEST/$FILE|perl -p -e 's/></>\n</go' >$DEST/$FILE.pp
  if [ $? -eq 0 ]; then
	mv -f $DEST/$FILE.pp $DEST/$FILE
  else
	echo "Prettyprint failed." | mailx -s "Import2: $CUST $DATE failed" $MAIL
	exit 1
  fi
else
  echo "$DATE: No file found"
  echo "File transfer error." | mailx -s "Import2: $CUST $DATE failed" $MAIL
  exit 1
fi

# create diff
# /appl/import2/queues/$CUST/prev/$FILE.prev får inte tagas bort!!!
if [ -f $PREV ]; then

	/appl/import2/scripts/publit_diff.pl $PREV $DEST/$FILE $DEST/$FILE.diff

	if [ $? -eq 0 ]; then
		#rm -f $PREV
		mv -f $DEST/$FILE $PREV
		cat $DEST/$FILE.diff | /appl/import2/scripts/publit_enc_ent.pl >$DEST/$FILE.duff
		if [ $? -eq 0 ]; then
			mv -f $DEST/$FILE.duff $INCOMING/$FILE
			rm -f $DEST/$FILE.diff
		else
			echo "publit_enc_ent.pl exited abnormally." | mailx -s "Import2: $CUST $DATE failed" $MAIL
			exit 1
		fi
	else
		# If this happens, consult KP
		echo "publit_diff.pl exited abnormally." | mailx -s "Import2: $CUST $DATE failed" $MAIL
		exit 1
	fi
else
	# Initial load, create prev file
	cp -f $DEST/$FILE $PREV
	cat $DEST/$FILE | perl -p -e 's/<product (.*?)>/<product $1>\n<libris_leader>n<\/libris_leader>/go' | perl -p -e 's/<code>/<code status=\"n\">/go' | /appl/import2/scripts/publit_enc_ent.pl > $INCOMING/$FILE
	if [ $? -eq 0 ]; then
        	rm -f $DEST/$FILE
	else
		echo "publit initial load exited abnormally." | mailx -s "Import2: $CUST $DATE failed" $MAIL
	fi
fi
# Diff done!
