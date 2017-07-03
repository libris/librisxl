#!/bin/ksh -x
#
# 1) filen ftp://ftp.ur.se/metadata/ims.xml (anvandare/losenord = media62/Zymmer) hamtas varje lordag
#
# 090312 Kai P, skapa diff med ur_diff.pl, spara senaste infilen, skicka diff
# till 2)
# REDONE: NOT ANY MORE! 2) kor skriptet /appl/import2/scripts/transform.sh /appl/import2/etc/ur.xml < [filen] > /appl/import2/queues/ur/incoming/ur.[datum].xml
#

DEST=/appl/import2/queues/ur/tmp
DATE=`date "+%Y-%m-%d"`
MAILREC="jour@libris.kb.se"

CUST=ur
SITE=ftp.ur.se
USER=media62
PASS=Zymmer
FTPPATH=metadata
FTPFILE=ims.xml.zip
FILE=ims.xml

cd $DEST

# Get file
ftp -n $SITE <<EOF
user $USER $PASS
bin
cd $FTPPATH
get $FTPFILE
bye
EOF

# Check if file exists, larger than zero and created today
if [ -s $DEST/$FTPFILE ]; then
  # Unzip file
  unzip $DEST/$FTPFILE
  # Run dos2unix to convert
  #dos2unix $DEST/$FILE $DEST/$FILE.cnv
  if [ $? -eq 0 ] && [ -s $DEST/$FILE ]; then
    #mv $DEST/$FILE.cnv $DEST/$FILE
    rm $DEST/$FTPFILE
  else
  	echo "UR: Unzip failed." | mailx -s "Import2: $CUST $DATE failed" $MAILREC
	exit 1
  fi

  echo "$DATE: File found: `ls -l $DEST/$FILE`"
else
  echo "$DATE: No file found"
  echo "No file transferred with ftp" | mailx -s "Import2: $CUST $DATE failed" $MAILREC
  exit 1
fi

# 091203 Kai P, create diff, reworked 130424 KP for enddate adaptation
# /appl/import2/queues/$CUST/prev/$FILE.prev får inte tagas bort!!!
if [ -f /appl/import2/queues/$CUST/prev/$FILE.prev ]; then
	PREV="/appl/import2/queues/$CUST/prev/$FILE.prev"
else
	rm -f /appl/import2/queues/$CUST/prev/$FILE.0	
	touch /appl/import2/queues/$CUST/prev/$FILE.0
	PREV="/appl/import2/queues/$CUST/prev/$FILE.0"
fi

rm -f $DEST/$FILE.diff

/appl/import2/scripts/ur_diff.pl $PREV $DEST/$FILE $DEST/$FILE.diff

if [ $? -eq 0 ]; then
	rm -f /appl/import2/queues/$CUST/prev/$FILE.prev
	cp -f $DEST/$FILE /appl/import2/queues/$CUST/prev/$FILE.prev
	mv -f $DEST/$FILE.diff $DEST/$FILE
else
	# If this happens, consult KP
	echo "ur_diff.pl exited abnormally." | mailx -s "Import2: $CUST $DATE failed" $MAILREC
	exit 1
fi
# Diff done!

# Split infile and process sequentialy, split=5000 records, ca 50-100 mb
/appl/import2/scripts/ur_split.pl $DEST/$FILE
if [ $? -ne 0 ]; then
	echo "ur_split.pl failed"|mailx -s "Import2:$CUST $DATE" $MAILREC
	exit 1
fi


for tmpfile in `ls -1tr $DEST/$FILE.*`; do

tmps=$tmpfile
tmps=${tmps##*.}

mv $tmpfile /appl/import2/queues/$CUST/incoming/$CUST.$DATE.$tmps.xml
#mv $tmpfile /appl/import2/queues/$CUST/$CUST.$DATE.$tmps.xml

done

rm -f $DEST/$FILE* # ims.xml + tmps
