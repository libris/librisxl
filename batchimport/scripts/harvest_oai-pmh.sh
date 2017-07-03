#!/bin/sh

if [ $# -ne 2 ] ; then
  echo "usage: $0 <harvest url> <base file>"
  exit 1
fi


URL=$1
BASE=`echo $URL | awk -F\? '{ print $1 }'`
FILE=$2
URLFILE=$1
HOME=`(cd \`dirname $0\`/..;pwd)`
WGET=/usr/local/bin/wget
#WGET=curl
DATE=`date +"%Y%m%d-%H:%M:%S"`
RESUMPTIONTOKEN=

N=0
while [ -n "$RESUMPTIONTOKEN" -o $N -eq 0 ]
do
  N=`expr $N + 1`
  FILENAME=$FILE.$N

  if [ -n "$RESUMPTIONTOKEN" ] ; then
    $WGET -O $FILENAME "$BASE?verb=ListRecords&resumptionToken=$RESUMPTIONTOKEN" > /dev/null 2>&1
  else
    $WGET -O $FILENAME $URL > /dev/null 2>&1
  fi

  RESUMPTIONTOKEN=`$HOME/scripts/grepresumptiontoken.sh < $FILENAME 2> /dev/null`
done

if [ $N -eq 1 ] ; then
  mv $FILENAME $FILE
fi

