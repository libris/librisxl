#!/usr/bin/ksh

HOME='/appl/import2'

cd "$HOME/queues/ebsco/prev"

DATE=`date "+%Y%m%d%H%M"`
FILENAME="ebsco-10504.$DATE.xml"
MAIL="jour@libris.kb.se"

# ftp
FTP='atozftp.ebsco.com'
USR='ftpatoz'
PWD='at0Zftp1t'
FILES='ebz-10504-*'

LAST=`ls -1 ebz-10504-*.zip 2>/dev/null|tail -1`
LAST=`basename $LAST`

# Getting filelist from server
ftp -n $FTP <<EOF
user $USR $PWD
bin
ls $FILES /tmp/filelist
bye
EOF

echo $LAST >> /tmp/filelist
LATEST=`sort /tmp/filelist|tail -1`
rm -f /tmp/filelist

if [[ "$LATEST" != "$LAST" ]]; then

# Getting latest from server
ftp -n $FTP <<EOF
user $USR $PWD
bin
get $LATEST
bye
EOF

if [ -s $LATEST ]; then

	unzip $LATEST

	IN="${LATEST%.zip}.marc"

	java -cp $HOME/lib/marc-1.2.2.jar:$HOME/lib/vrlin.jar se.kb.libris.util.marc.io.InOut -outType=XML -inEncoding=ISO-8859-1 -outEncoding=UTF-8 -discardBroken=true -trustDirectory=true < $IN > ebz.xml

	cat ebz.xml | $HOME/scripts/ebsco_normalize.pl > ebz.new

	rm ebz.xml

	# 101105 Kai P, create diff
	# ebz.prev får inte tagas bort!!!
	if [ -s ebz.new ] && [ -f ebz.prev ]; then
        	# Incremental load, diff it
        	rm -f ebz.diff

        	$HOME/scripts/xml_marc_diff.pl ebz.prev ebz.new ebz.diff

        	if [ $? -eq 0 ]; then
                	mv -f ebz.new ebz.prev
                	mv -f ebz.diff ../tmp/$FILENAME.tmp
        	else
                	# If this happens, consult KP
                	echo "ebsco: diff exited abnormally." | mailx -s "Import2: ebsco $DATE failed" $MAIL
                	exit 1
        	fi
	else
        	# Initial load, create prev file
        	mv -f ebz.new ebz.prev
		cp ebz.prev ../tmp/$FILENAME.tmp
	fi
	# Diff done!

        # ebsco charconvert & clean invalid utf chars
	cat ../tmp/$FILENAME.tmp | $HOME/scripts/ebsco_charconvert.pl | $HOME/scripts/utfclean.pl > ../tmp/$FILENAME
	mv -f ../tmp/$FILENAME ../incoming/$FILENAME
	rm -f ../tmp/$FILENAME.tmp

	LAST=${LAST%.zip}
	rm -f $LAST.*

	LATEST=${LATEST%.zip}
	rm -f $LATEST.*
	touch $LATEST.zip # Keep name of latest ebsco download
fi
fi
