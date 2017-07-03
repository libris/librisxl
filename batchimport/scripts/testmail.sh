#!/bin/sh -x

HOME=`dirname $0`/..
#MAILTO="anders.cato@kb.se carin.anell@kb.se britt-marie.larsson@kb.se martin.malmsten@kb.se"
#MAILTO="martin.malmsten@kb.se"
ECHO=/usr/bin/echo
#ECHO="/bin/echo -e"
MAIL="cat"

if [ $# -lt 3 ] ; then
  echo "usage: $0 <queuename> <logfile> <filename> <address1> [address2] ... [addressn]"
  exit 1
fi

QUEUE=$1
LOGFILE=$2
FILENAME=$3
shift 3
MAILTO=$*

echo $QUEUE
echo $LOGFILE
echo $FILENAME
echo $MAILTO

$MAIL $MAILTO << endofmessage
From: metadatatratten@libris.kb.se
Reply-To: driften@libris.kb.se
Subject: Metadatatratten v2 ($QUEUE - $FILENAME) `date +"%Y%m%d-%H:%M:%S"`
Content-type: text/html; charset=iso-8859-1
Content-transfer-encoding: 8BIT

<html>
  <body>
    <pre>
Detta <E4>r ett automagiskt meddelande fr<E5>n importrutinen f<F6>r $QUEUE

Resultat:
`cat /tmp/sendmail.$$.tmp`

Utf<F6>rlig logg f<F6>ljer:
BIB_ID         ISXN              TITEL                        UPPHOV
-------------------------------------------------------------------------------
`cat /tmp/import.$$.tmp`
-------------------------------------------------------------------------------
    </pre>
  </body>
</html>

endofmessage

