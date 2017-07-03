#!/bin/ksh

starthour=`date "+%H"`

TEMP1=/tmp/batchimport1.$$
TEMP2=/tmp/batchimport2.$$
TEMP3=/tmp/batchimport3.$$
TEMP4=/tmp/batchimport4.$$

HOME=`(cd \`dirname $0\`/..;pwd)`

MAILTO_FILE=$HOME/etc/mail_kunder.txt
MAILTO_ON_ERROR=`grep OnFailMailto $MAILTO_FILE | cut -d "," -f 3`

LOAD=0

date "+%Y-%m-%d %H:%M"
#check lock
if [ -f $HOME/lock/batchimport ] ; then
  echo "lock exists ($HOME/lock/batchimport)"
  exit 0
fi

touch $HOME/lock/batchimport


cat $HOME/etc/convert.txt | while read queue type flags
do
  if [ "$queue" = "" ] ; then
    continue;
  fi

  nowhour=`date "+%H"`
  let "timediff=nowhour-starthour"
  echo Timediff: $timediff
  if [ $timediff -gt 1 ];then
    Echo Skipping because this batchimport has been running too long.
    continue;
  fi

  if [ `echo $queue | cut -b -1` = "#" ] ; then
    continue;
  fi

  for file in $HOME/queues/$queue/incoming/*
  do
    echo Doing $queue $file
    basename=`basename $file`
    baseflags="--outEncoding=UTF-8 --errFile=$HOME/queues/$queue/err/$basename.err --logFile=$HOME/queues/$queue/log/$basename.log"

   #echo "baseflags: $baseflags"
   #echo "flags: $flags"

    if [ -f $file ] ; then

      LOAD=1

      if [ $type = xml ] ; then
        cat $file | /usr/local/bin/sed 's/\x0b/ /g' | eval $HOME/scripts/import.sh --inType=XML $baseflags $flags $HOME/etc/import.properties
      elif [ $type = gzipxml ] ; then
        gunzip -c $file | /usr/local/bin/sed 's/\x0b/ /g' | eval $HOME/scripts/import.sh --inType=XML $baseflags $flags $HOME/etc/import.properties
      elif [ $type = iso2709 ] ; then
        cat $file | /usr/local/bin/sed 's/\x0b/ /g' | eval $HOME/scripts/import.sh --inType=ISO2709 $baseflags $flags $HOME/etc/import.properties
      fi > $TEMP1

      # added a copying to jumper for the xinfo
      if [ $queue = "bokrondellen" ]; then
        # scp $file piff:/data/extrainfo/bokrondellen/tmp_gosling
        /usr/local/bin/scp $file jumper:/appl/extrainfo/bokrondellen/tmp_gosling
        /usr/local/bin/scp $file erlang:/extra/xinfo/work/bokrondellen/data
        #cp $file /appl/mv_to_joe/.
      fi

      mv $file $HOME/queues/$queue/done/`basename $file`

      cat $TEMP1 >> $HOME/queues/$queue/log/$basename.log
# Separate mail to different sigels
      touch $TEMP4
      grep -v "^mul-bib" $HOME/queues/$queue/log/$basename.log | cut -c102-110 | sort | uniq |grep -v "^$" > $TEMP4
      sigelcount=`wc -l $TEMP4 | awk ' { print $1 } '`
      if [ $sigelcount -lt 1 ];then
        echo "NoSigel" > $TEMP4
      fi
      cat $TEMP4 | while read sigel;do
        echo Doing sigel $sigel in file $basename for queue $queue
        /usr/local/bin/gawk ' BEGIN { FIELDWIDTHS = "9 9 83 6" } { if ($4 == insigel ) { if (bibidold == $2) { print x} ; print $0 } } ; { if ($4 == "") {x=$0 ; bibidold=$2 }} ' insigel=$sigel < $HOME/queues/$queue/log/$basename.log > $TEMP2
        grep "^mul-bib" $HOME/queues/$queue/log/$basename.log > $TEMP3
        cat $TEMP3 $TEMP2 > $HOME/dumps/$sigel.$basename
        echo " "
        mail=`awk ' BEGIN {FS=","} {if ($2==sigel) {print $3}} ' sigel=$sigel $HOME/etc/mail_kunder.txt | grep -v "^$"`
        if [ $? -gt 0 ];then
          mail=$MAILTO_ON_ERROR
          addinfo=SIGELNOTFOUND2_$sigel"_"$queue
        else
          addinfo=$sigel"_"$queue
        fi
        if [ $sigelcount -lt 2 ];then
          logsfile=$HOME/queues/$queue/log/$basename.log
        else 
          logsfile=$HOME/dumps/$sigel.$basename
        fi
        $HOME/scripts/sendmail3.sh "$addinfo" $logsfile $basename "$mail"
      done
      rm $TEMP2 $TEMP3 $TEMP4
    fi
  done
done

# Error handling, KP 110524 v2.0
# LOAD introduced to combat alarm sms's on empty loads...

logfile=`ls -1rt /log/import*log|tail -1`

if [ "$LOAD" -eq 1 ]; then
	prevlog=`cat /tmp/batimp.log 2>/dev/null`
	echo $prevlog|read prevlogfile prevlogline
	if [[ "$logfile" > "$prevlogfile" ]]; then prevlogline=0; fi
	logline=`tail +$prevlogline $logfile|/usr/bin/egrep -n 'ArrayIndexOutOfBounds|SocketException|OutOfMemoryError|BatchCatException'|tail -1|cut -f1 -d':'`
	if [[ "$logline" != '' ]]; then
		logline=$((prevlogline+logline+1))
		echo "$logfile $logline" >/tmp/batimp.log
		/usr/local/scripts/larmsms.sh --cc=kai.poykio@kb.se "batchimport failed?" <<-EOF
			Check $logfile $logline
			Edit loadfile+reload
                        Message created by $0 on `uname -n`.
EOF
	fi
fi

rm $HOME/lock/batchimport
