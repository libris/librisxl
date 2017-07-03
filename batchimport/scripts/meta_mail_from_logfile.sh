#!/bin/ksh

if [ $# -ne 2 ] ; then
  echo "usage: $0 <queue (tex dawson)> <logfilename>"
  exit 1
fi


queue=$1
logfile=$2
basename=`echo $logfile | sed "s/.log//g"`

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

### Separate mail to different sigels
echo Generating mail...
touch $TEMP4
grep -v "^mul-bib" $logfile | cut -c102-110 | sort | uniq |grep -v "^$" > $TEMP4
sigelcount=`wc -l $TEMP4 | awk ' { print $1 } '`
if [ $sigelcount -lt 1 ];then
  echo "NoSigel" > $TEMP4
else 
  awk ' { print $2 } ' $logfile | uniq -c | awk ' { print $1, $2 } ' | /usr/local/bin/gawk ' $1 == 1 { print $2 } ' | while read bibid; do  
    /usr/local/bin/gawk ' $2 ~ bibid ' bibid=$bibid $logfile
  done > $TEMP5
fi
cat $TEMP4 | while read sigel;do
  echo Doing sigel $sigel in file $basename for queue $queue
  /usr/local/bin/gawk ' BEGIN { FIELDWIDTHS = "9 9 83 6" } { if ($4 == insigel ) { if (bibidold == $2) { print x} ; print $0 } } ; { if ($4 == "") {x=$0 ; bibidold=$2 }} ' insigel=$sigel < $logfile > $TEMP2
  grep "^mul-bib" $logfile > $TEMP3
  echo " "
  mail=`grep -v "^#" $MAILTO_FILE | awk ' BEGIN {FS=","} {if ($2==sigel) {print $3}} ' sigel=$sigel | grep -v "^$"`
  if [ $? -gt 0 ];then
    mail=$MAILTO_ON_ERROR
    addinfo=SIGELNOTFOUND_$sigel"_"$queue
  else
    addinfo=$sigel"_"$queue
  fi
  if [ $sigelcount -lt 2 ];then
    logsfile=$logfile
  else 
    cat $TEMP3 $TEMP2 $TEMP5 > /data/dumps/$sigel.$basename
    logsfile=/data/dumps/$sigel.$basename
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
done
rm $TEMP2 $TEMP3 $TEMP4 $TEMP5

