#!/bin/ksh

starthour=`date "+%H"`

TEMP1=/tmp/batchimport1.$$
TEMP2=/tmp/batchimport2.$$
TEMP3=/tmp/batchimport3.$$
TEMP4=/tmp/batchimport4.$$
TEMP5=/tmp/batchimport5.$$

HOME=`(cd \`dirname $0\`/..;pwd)`

MAILTO_FILE=$HOME/etc/mail_kunder.txt
MAILTO_ON_ERROR=`grep -v "^#" $MAILTO_FILE | grep OnFailMailto | cut -d "," -f 3`
MAILTO_ALL_MESSAGES=`grep -v "^#" $MAILTO_FILE | grep MailAllTo | cut -d "," -f 3`

LOAD=0

# N:o of parallel import (inclusive this one)
max_parallel_imports=3

date "+%Y-%m-%d %H:%M"
#check lock

noofimports=`ps -ef | grep $0 | awk ' { print $5 } ' | sort | uniq | wc -l`

echo Imports currently running: $noofimports

if [ $noofimports -gt $max_parallel_imports ];then
  echo Max parallel imports of $max_parallel_imports exceeded.
  exit 0
fi


cat $HOME/etc/convert.txt | while read queue type flags
do
  if [ "$queue" = "" ] ; then
    continue;
  fi

  nowhour=`date "+%H"`
  let "timediff=nowhour-starthour"
  echo Timediff: $timediff
  if [ $timediff -gt 1 ];then
    echo Skipping because this batchimport has been running too long.
    continue;
  fi

  if [ `echo $queue | cut -b -1` = "#" ] ; then
    continue;
  fi

  for file in $HOME/queues/$queue/incoming/*
  do
    echo `date "+%Y-%m-%d_%H:%M"` Doing $queue $file
    basename=`basename $file`
#    baseflags="--outEncoding=UTF-8 --errFile=$HOME/queues/$queue/err/$basename.err --logFile=$HOME/queues/$queue/log/$basename.log"
    baseflags="--errFile=$HOME/queues/$queue/err/$basename.err --logFile=$HOME/queues/$queue/log/$basename.log"

   #echo "baseflags: $baseflags"
   #echo "flags: $flags"

    if [ -f $file ] ; then

      fileinprogress=$HOME/queues/$queue/tmp/$basename
      mv $file $fileinprogress
      LOAD=1

      echo Importing...
      if [ $type = xml ] ; then
        cat $fileinprogress | /usr/local/bin/sed 's/\x0b/ /g' | eval $HOME/scripts/import.sh --inType=XML $baseflags $flags $HOME/etc/import.properties
      elif [ $type = gzipxml ] ; then
        gunzip -c $fileinprogress | /usr/local/bin/sed 's/\x0b/ /g' | eval $HOME/scripts/import.sh --inType=XML $baseflags $flags $HOME/etc/import.properties
      elif [ $type = iso2709 ] ; then
        cat $fileinprogress | /usr/local/bin/sed 's/\x0b/ /g' | eval $HOME/scripts/import.sh --inType=ISO2709 $baseflags $flags $HOME/etc/import.properties
      fi > $TEMP1

      # added a copying to jumper for the xinfo
      if [ $queue = "bokrondellen" ]; then
        echo Sending to xinfo...
        # scp $file piff:/data/extrainfo/bokrondellen/tmp_gosling
        /usr/local/bin/scp $fileinprogress jumper:/appl/extrainfo/bokrondellen/tmp_gosling
#        /usr/local/bin/scp $file erlang:/extra/xinfo/work/bokrondellen/data
        #cp $file /appl/mv_to_joe/.
      fi

      mv $fileinprogress $HOME/queues/$queue/done/$basename

      cat $TEMP1 >> $HOME/queues/$queue/log/$basename.log
      ### Separate mail to different sigels
      echo Generating mail...
      touch $TEMP4
      grep -v "^mul-bib" $HOME/queues/$queue/log/$basename.log | cut -c102-110 | sort | uniq |grep -v "^$" > $TEMP4
      sigelcount=`wc -l $TEMP4 | awk ' { print $1 } '`
      if [ $sigelcount -lt 1 ];then
        echo "NoSigel" > $TEMP4
      else 
        awk ' { print $2 } ' $HOME/queues/$queue/log/$basename.log | uniq -c | awk ' { print $1, $2 } ' | /usr/local/bin/gawk ' $1 == 1 { print $2 } ' | while read bibid; do  
          /usr/local/bin/gawk ' $2 ~ bibid ' bibid=$bibid $HOME/queues/$queue/log/$basename.log
        done > $TEMP5
      fi
      cat $TEMP4 | while read sigel;do
        echo Doing sigel $sigel in file $basename for queue $queue
        /usr/local/bin/gawk ' BEGIN { FIELDWIDTHS = "9 9 83 6" } { if ($4 == insigel ) { if (bibidold == $2) { print x} ; print $0 } } ; { if ($4 == "") {x=$0 ; bibidold=$2 }} ' insigel=$sigel < $HOME/queues/$queue/log/$basename.log > $TEMP2
        grep "^mul-bib" $HOME/queues/$queue/log/$basename.log > $TEMP3
        echo " "
        mail=`grep -v "^#" $MAILTO_FILE | awk ' BEGIN {FS=","} {if ($2==sigel) {print $3}} ' sigel=$sigel | grep -v "^$"`
        if [ $? -gt 0 ];then
          mail=$MAILTO_ON_ERROR
          addinfo=SIGELNOTFOUND_$sigel"_"$queue
        else
          addinfo=$sigel"_"$queue
        fi
        if [ $sigelcount -lt 2 ];then
          logsfile=$HOME/queues/$queue/log/$basename.log
	  RM_LOG=0
        else 
          cat $TEMP3 $TEMP2 $TEMP5 > /data/dumps/$sigel.$basename
          logsfile=/data/dumps/$sigel.$basename
	  RM_LOG=1
        fi
        mail="$mail $MAILTO_ALL_MESSAGES"
        ### Figure out mailing filter
        maillist=""
        for epost in $mail;do
          adress=`echo $epost | cut -d ":" -f 1`
          queuefilter=`echo $epost | awk ' BEGIN {FS=":"} { print $2 } ' | grep -v "^$"`
          noqueuefilter=$?
          if [ $noqueuefilter -gt 0 ];then
            maillist=$maillist" "$adress
          else
            for filter in `echo $queuefilter | tr ";" " "`;do
              fixedqueuefilter=`echo $filter | tr "_" " "`
              if [ $fixedqueuefilter == $queue ];then
                maillist=$maillist" "$adress
              fi
            done
          fi
        done
        # Remove duplicate email adresses:
        maillist=`echo $maillist | tr " " "\n" | sort | uniq | tr "\n" " "`
        # Send mail:
        $HOME/scripts/sendmail3.sh "$addinfo" $logsfile $basename $maillist
	if [ $RM_LOG -eq 1 ]; then #some logs are needed for restart
        	rm $logsfile
	fi
      done
      rm $TEMP2 $TEMP3 $TEMP4 $TEMP5
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
	#logline=`tail +$prevlogline $logfile|/usr/bin/egrep -n 'ArrayIndexOutOfBounds|SocketException|OutOfMemoryError|BatchCatException'|tail -1|cut -f1 -d':'`
	logline=`tail +$prevlogline $logfile|/usr/bin/egrep -n 'ArrayIndexOutOfBounds|SocketException|OutOfMemoryError'|tail -1|cut -f1 -d':'`
	if [[ "$logline" != '' ]]; then
		logline=$((prevlogline+logline+1))
		echo "$logfile $logline" >/tmp/batimp.log
		/usr/local/scripts/larmsms.sh --cc=kai.poykio@kb.se "batchimport failed?" <<-EOF
			Check $logfile $logline
			Edit loadfile+reload
			`let PRELINE=$logline-1;let POSTLINE=$logline+3;sed -n "$PRELINE,$POSTLINE"p $logfile`
                        Message created by $0 on `uname -n`.
EOF
	fi
fi

