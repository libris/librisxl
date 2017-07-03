#!/bin/sh

DATE1=`date +"%Y%m%d"`
DATE2=$DATE1

if [ $# -eq 1 ] ; then
  DATE1=$1
  DATE2=$DATE1
fi

if [ $# -eq 2 ] ; then
  DATE1=$1
  DATE2=$2
fi

cd /home/marma/java/projects/import2
scripts/import.sh "${DATE1}_00:00:00" "${DATE2}_23:59:59" < arttot.xml > out 2> err
while read a b; do   echo $a; done < out > onr
cd /home/marma/cvs/voyager
scripts/getmarc.sh -dbUrl=jdbc:oracle:thin:@aho:1521:LIBR -dbUser=royallibdb -dbPassword=Droyallibdb etc/exportgroups/BRO.properties < ~marma/java/projects/import2/onr > bokr.marc
mv bokr.marc /home/marma/java/projects/import2
cd /home/marma/java/projects/import2

if [ $# -eq 2 ] ; then
  scripts/export.sh etc/marcxml-bokrondellen.xsl VRLIN < bokr.marc > bokrondellen.$DATE1-$DATE2.xml
else
  scripts/export.sh etc/marcxml-bokrondellen.xsl VRLIN < bokr.marc > bokrondellen.$DATE1.xml
fi
