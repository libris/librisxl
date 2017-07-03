#!/bin/ksh -x
# 120907 KP

CUST=elib
FILE="$CUST.xml"
DEST="/appl/import2/queues/$CUST/tmp"
INCOMING="/appl/import2/queues/$CUST/incoming"
PREV="/appl/import2/queues/$CUST/prev/$FILE.prev"
DATE=`date "+%Y-%m-%d"`
MAIL="jour@libris.kb.se"

# Get Elib
/usr/local/bin/curl -k -X POST -d "retailerid=977" -d "retailerkeycode=4417427bd1fefeabaf6683e02d5b35ad" -d "countrycode=SE" -d "fromdate=2000-01-01" -d "languagecode=SV" https://www.elib.se/webservices/getproductlist.asmx/GetProductList -o $DEST/$FILE

# Check if file exists, larger than zero
if [ $? -eq 0 ] && [ -s $DEST/$FILE ]; then
  echo "$DATE: File found: `ls -l $DEST/$FILE`"
else
  echo "$DATE: No file found"
  echo "No file transferred with curl" | mailx -s "Import2: $CUST $DATE failed" $MAIL
  exit 1
fi

# create diff
# /appl/import2/queues/$CUST/prev/$FILE.prev får inte tagas bort!!!
if [ -f $PREV ]; then

	/appl/import2/scripts/elib_diff.pl $PREV $DEST/$FILE $DEST/$FILE.diff

	if [ $? -eq 0 ]; then
		#rm -f $PREV
		mv -f $DEST/$FILE $PREV
		mv -f $DEST/$FILE.diff $INCOMING/$FILE
	else
		# If this happens, consult KP
		echo "elib_diff.pl exited abnormally." | mailx -s "Import2: $CUST $DATE failed" $MAIL
		exit 1
	fi
else
	# Initial load, create prev file
	cp -f $DEST/$FILE $PREV
	cat $DEST/$FILE | perl -p -e 's/<product>/<product>\n      <libris_leader>n<\/libris_leader>/go' | perl -p -e 's/<.?xs1:response.*?>//' > $INCOMING/$FILE
	rm -f $DEST/$FILE
fi
# Diff done!
