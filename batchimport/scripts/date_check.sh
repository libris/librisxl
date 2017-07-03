#!/usr/bin/sh  

TIME_ORG=$1
if [ $# -ne 1 ] ; then
  echo "usage: oai_harvest.sh DATE=<YYYY-MM-DD>"
  echo "usage: and DATE=today"
  exit 1
else	
 TIME=`echo $TIME_ORG | awk '{print substr($0,6,length($0)-5)}'` 
 if [ $TIME = "today" ] ; then
	TIME=`date +"%Y-%m-%d"`
 fi
fi
echo "TIME_ORG= $TIME_ORG"
echo "TIME= $TIME"
TODAY=`date +"%Y%m%d"`
YESTERDAY=`/usr/local/bin/day -before $TODAY` 

YEAR=`echo $YESTERDAY | awk '{print substr($0,0,4)}'`
MONTH=`echo $YESTERDAY | awk '{print substr($0,5,2)}'`
DAY=`echo $YESTERDAY | awk '{print substr($0,7,2)}'`
NEWDATE=$YEAR-$MONTH-$DAY
echo "YESTERDAY = $YESTERDAY"
echo "NEW DATE = $NEWDATE"
