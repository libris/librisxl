#!/bin/sh
 
HOME=`dirname $0`/..
#MAILTO="anders.cato@kb.se martin.malmsten@kb.se"
MAILTO="martin.malmsten@kb.se"
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

rm /tmp/sendmail.$$.tmp > /dev/null 2>&1;

if [ $N_ADDED -eq 1 ] ; then
  echo "  $N_ADDED bibliografiska post med tillhörande beståndspost har lagts till" >> /tmp/sendmail.$$.tmp
elif [ $N_ADDED -ne 0 ] ; then
  echo "  $N_ADDED bibliografiska post(er) med tillhörande beståndspost har lagts till" >> /tmp/sendmail.$$.tmp
fi

if [ $N_ADDMFHD -eq 1 ] ; then
  echo "  $N_ADDMFHD beståndspost har lagts till redan befintlig bibliografisk post" >> /tmp/sendmail.$$.tmp
elif [ $N_ADDMFHD -ne 0 ] ; then
  echo "  $N_ADDMFHD beståndsposter har lagts till redan befintlig bibliografisk post" >> /tmp/sendmail.$$.tmp
fi

if [ $N_SKIPPED -eq 1 ] ; then
  echo "  $N_SKIPPED post saknade ISSN/ISBN" >> /tmp/sendmail.$$.tmp
elif [ $N_SKIPPED -ne 0 ] ; then
  echo "  $N_SKIPPED poster saknade ISSN/ISBN" >> /tmp/sendmail.$$.tmp
fi

if [ $N_MUL -eq 1 ] ; then
  echo "  $N_MUL post matchade mot fler än en post i LIBRIS (import ej möjlig)" >> /tmp/sendmail.$$.tmp
elif [ $N_MUL -ne 0 ] ; then
  echo "  $N_MUL poster matchade mot fler än en post i LIBRIS (import ej möjlig)" >> /tmp/sendmail.$$.tmp
fi

$MAIL $MAILTO << endofmessage
From: metadatatratten@libris.kb.se
Reply-To: martin.malmsten@kb.se
Subject: Metadatatratten v2 ($QUEUE) `date +"%Y%m%d-%H:%M:%S"`
Content-type: multipart/alternative; boundary=Metadatatratten-Mail-$$

--Metadatatratten-Mail-$$
Content-Transfer-Encoding: 8bit
Content-Type: text/plain;
	charset=ISO-8859-1

Se den bifogade filen

--Metadatatratten-Mail-$$
Content-Transfer-Encoding: 8bit
Content-Type: text/html;
        charset=ISO-8859-1

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <body>
    <pre>
Detta är ett automagiskt meddelande från importrutinen för $QUEUE

Resultat:
`cat /tmp/sendmail.$$.tmp`

Utförlig logg följer:
BIB_ID         ISXN              TITEL                        UPPHOV
-------------------------------------------------------------------------------
`cat /tmp/import.$$.tmp`
-------------------------------------------------------------------------------
    </pre>
  </body>
</html>
endofmessage

rm /tmp/import.$$.tmp
rm /tmp/sendmail.$$.tmp
