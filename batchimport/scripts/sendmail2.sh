#!/bin/sh
 
HOME=`dirname $0`/..
MAILTO="anders.cato@kb.se martin.malmsten@kb.se"
#MAILTO="martin.malmsten@kb.se"
ECHO=/usr/bin/echo
#ECHO="/bin/echo -e"
MAIL="mail"

if [ $# -ne 2 ] ; then
  echo "usage: $0 <queuename> <logfile>"
  exit 1
fi

QUEUE=$1
LOGFILE=$2

N_ADDED=`grep -c "add-bib:" $LOGFILE`
N_ADDMFHD=`grep -c "add-hol:" $LOGFILE`
N_SKIPPED=`grep -c "skp-bib:" $LOGFILE`
N_ADDMFHD=`expr $N_ADDMFHD - $N_ADDED`
N_MUL=`grep -c "mul-bib:" $LOGFILE`

grep -v skp-bib $LOGFILE | grep -v exi-hol | cut -b 10- | sort | uniq > /tmp/import.$$.tmp

if [ ! -s /tmp/import.$$.tmp ] ; then
  exit
fi

$ECHO "From: metadatatratten@libris.kb.se\nreply-to: martin.malmsten@kb.se\nsubject: Metadatatratten v2 ($QUEUE) `date +"%Y%m%d-%H:%M:%S"`\nContent-Type: text/html; CHARSET=iso-8859-1\n<body><html><pre>\nDetta är ett automagiskt meddelande från importrutinen för $QUEUE.\n\nResultat:\n  $N_ADDED bibliografiska post(er) med tillhörande beståndspost har lagts till.\n  $N_ADDMFHD beståndspost(er) har lagts till redan befintlig bibliografisk post.\n  $N_SKIPPED post(er) saknade ISSN/ISBN\n\nUtförlig log följer:\n\nBIB_ID         ISXN              TITEL                         UPPHOV\n-------------------------------------------------------------------------------\n`cat /tmp/import.$$.tmp`\n-------------------------------------------------------------------------------\n</pre></body></html>" | $MAIL $MAILTO

rm /tmp/import.$$.tmp
