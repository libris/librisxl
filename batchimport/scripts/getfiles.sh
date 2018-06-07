#!/bin/bash -x
# 020311 Kai P. Utility to ftp files from different servers.
# 180531 Kai P. Reworked a bit.

echo Timestamp: `date "+%Y-%m-%d %H:%M"`
 
FTPDIR=/data/appl/upload

cd $FTPDIR

cat /appl/import/scripts/getfiles.txt | while read DIR IP USER PASS FILES; do

# '#' first on a line will disable that configuration.
if [ -z ${DIR%%#*} ]; then
	echo "Skipping $DIR."
	continue
fi

#P=.
P=*

if [[ "$FILES" =~ '/' ]]; then
	FILEDIR=${FILES%/*}
else
	FILEDIR=''
	# deprecated
	#if [ -z $FILEDIR ]; then FILEDIR=/; fi
	#if [ $FILEDIR = /NOSP ]; then FILEDIR=NOSP; P=*; fi #Nosp needs special treatment 
fi

PATTERNLIST=${FILES##*/}
PATTERNLIST=`echo "$PATTERNLIST"|sed -e 's/|/ /g'`

# Getting filelist from server
#ftp -T 30 -n $IP <<EOF
echo "user $USER $PASS" >/tmp/ftplist
echo "binary" >>/tmp/ftplist
if [ ! -z "$FILEDIR" ]; then
	echo "cd $FILEDIR" >>/tmp/ftplist
fi
echo "ls $P filelist" >>/tmp/ftplist
echo "bye" >>/tmp/ftplist
ftp -in $IP </tmp/ftplist

if [ $? -ne 0 ]; then
	#TODO
	/usr/local/scripts/larmsms.sh "/scripts/getfiles.sh@gosling, ftp failed for $DIR." </dev/null
	continue
	# Maybe we should collect all failed and keep info for subsequent runs?
fi

# Match and filter filelist
if [ -s filelist ]; then
cat filelist | perl -npe '@l=split; $_ = $l[-1]."\n";' | while read FILE; do
        FILE=${FILE%;*} #Fix for nosp
	for PATTERN in $PATTERNLIST; do
	case $FILE in
		$PATTERN)
		if [ ! -s $FTPDIR/$DIR/$FILE ] && [ ! -f $FTPDIR/$DIR/$FILE.done ]; then
                   echo $FILE >>/tmp/filestoget
                fi 
		;;
	esac
	done
done
rm filelist
fi

# Make ftpscript and ftp files
if [ -s /tmp/filestoget ]; then
	echo "user $USER $PASS" >/tmp/ftpfiles
	echo "bin" >>/tmp/ftpfiles
	if [ ! -z "$FILEDIR" ]; then
		echo "cd $FILEDIR" >>/tmp/ftpfiles
	fi
	echo "lcd $DIR" >>/tmp/ftpfiles
	cat /tmp/filestoget | while read FILE; do
		echo "get $FILE" >>/tmp/ftpfiles
	done
	echo "bye" >>/tmp/ftpfiles
	ftp -in $IP </tmp/ftpfiles #Errorchecking=nada!
	rm /tmp/ftpfiles
	rm /tmp/filestoget
fi

done
