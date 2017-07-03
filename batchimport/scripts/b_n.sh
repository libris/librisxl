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

queue=sfx
basename=asdf
TEMP1=$HOME/queues/$queue/log/$basename.log

#      cat $TEMP1 >> $HOME/queues/$queue/log/$basename.log
# Separate mail to different sigels
      touch $TEMP4
      grep -v "MUL-BIB" $TEMP1 | cut -c102-110 | sort | uniq |grep -v "^$" > $TEMP4 
      sigelcount=`wc -l $TEMP4 | awk ' { print $1 } '`
      if [ $sigelcount -lt 2 ];then
        if [ $sigelcount -lt 1 ];then
          echo "No sigels found, sending to $MAILTO_ON_ERROR"
          mail=$MAILTO_ON_ERROR
          addinfo="NOSIGELS"_$queue"_"$basename
        else
          sigel=`head -1 $TEMP4`
          mail=`awk ' BEGIN {FS=","} {if ($2==sigel) {print $3}} ' sigel=$sigel $HOME/etc/mail_kunder.txt | grep -v "^$"`
          if [ $? -gt 0 ];then
            mail=$MAILTO_ON_ERROR
            addinfo=SIGELNOTFOUND1_$sigel"_"$queue
          else
            addinfo=$sigel"_"$queue
          fi
        fi 
        $HOME/scripts/sendmail3.sh $addinfo $HOME/queues/$queue/log/$basename.log $basename "$mail"
      else
        cat $TEMP4 | while read sigel;do
          echo Doing sigel $sigel in file $basename for queue $queue
          /usr/local/bin/gawk ' BEGIN { FIELDWIDTHS = "9 9 83 6" } { if ($4 == insigel ) { if (bibidold == $2) { print x} ; print $0 } } ; { if ($4 == "") {x=$0 ; bibidold=$2 }} ' insigel=$sigel < $TEMP1 > $TEMP2
          grep '*MUL-BIB*' $TEMP1 > $TEMP3
          cat $TEMP3 $TEMP2 > $HOME/dumps/$sigel.$basename
          echo " "
          mail=`awk ' BEGIN {FS=","} {if ($2==sigel) {print $3}} ' sigel=$sigel $HOME/etc/mail_kunder.txt | grep -v "^$"`
          if [ $? -gt 0 ];then
            mail=$MAILTO_ON_ERROR
            addinfo=SIGELNOTFOUND2_$sigel"_"$queue
          else
            addinfo=$sigel"_"$queue
          fi
          $HOME/scripts/sendmail3.sh "$addinfo" $HOME/dumps/$sigel.$basename $basename "$mail"
        done
        rm $TEMP2 $TEMP3 $TEMP4
      fi
