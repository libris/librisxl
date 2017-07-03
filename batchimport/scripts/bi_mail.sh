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
      queue=diva
      basename=2012-04-26-1

      ### Separate mail to different sigels
      echo Generating mail...
      touch $TEMP4
      grep -v "^mul-bib" $HOME/queues/$queue/log/$basename.log | cut -c102-110 | sort | uniq |grep -v "^$" > $TEMP4
      sigelcount=`wc -l $TEMP4 | awk ' { print $1 } '`
      if [ $sigelcount -lt 1 ];then
        echo "NoSigel" > $TEMP4
      else 
        awk ' { print $2 } ' $HOME/queues/$queue/log/$basename.log | uniq -c | awk ' { print $1, $2 } ' |  /usr/local/bin/gawk ' $1 == 1 ' | awk ' { print $2 } ' | while read bibid; do  
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
        else 
          cat $TEMP3 $TEMP2 $TEMP5 > $HOME/dumps/$sigel.$basename
          logsfile=$HOME/dumps/$sigel.$basename
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
        echo Trying to execute: $HOME/scripts/sendmail3.sh "$addinfo" $logsfile $basename $maillist
        $HOME/scripts/sendmail3.sh "$addinfo" $logsfile $basename $maillist
      done
      rm $TEMP2 $TEMP3 $TEMP4 $TEMP5

exit 0

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

