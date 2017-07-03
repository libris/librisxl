#!/bin/sh -x
 
HOME=`dirname $0`/..
#MAILTO="anders.cato@kb.se carin.anell@kb.se britt-marie.larsson@kb.se martin.malmsten@kb.se"
#MAILTO="martin.malmsten@kb.se"
ECHO=/usr/bin/echo
#ECHO="/bin/echo -e"
MAIL="mail"

if [ $# -lt 3 ] ; then
  echo "usage: $0 <queuename> <logfile> <filename> <address1> [address2] ... [addressn]"
  exit 1
fi

QUEUE=$1
LOGFILE=$2
FILENAME=$3
shift 3
MAILTO=$*

#N_ADDED=`grep -c "add-bib:" $LOGFILE`
#N_NOT_ADDED=`grep -c "exi-bib:" $LOGFILE`
#N_ADDMFHD=`grep -c "add-hol:" $LOGFILE`
#N_SKIPPED=`grep -c "skp-bib:" $LOGFILE`
#N_ADDMFHD=`expr $N_ADDMFHD - $N_ADDED`
#N_MUL=`grep -c "mul-bib:" $LOGFILE`
#N_MERGE=`grep -c "mrg-bib:" $LOGFILE`
#N_DELMFHD=`grep -c "del-hol:" $LOGFILE`
#N_FIR=`grep -c "fir-bib:" $LOGFILE`
#N_MRGMFHD=`grep -c "mrg-hol:" $LOGFILE`

grep -v skp-bib $LOGFILE | grep -v exi-hol | grep -v nid-bib | grep -v del-bib | sort -n -k 2 | uniq > /tmp/import1.$$.tmp

#grep -v skp-bib $LOGFILE | grep -v exi-bib | grep -v exi-hol | cut -b 10- | grep -v nid-bib | sort | uniq > /tmp/import.$$.tmp

awk ' { print $1 } ' /tmp/import1.$$.tmp | sort | uniq -c > /tmp/import_summary.$$.tmp

N_addbib=`grep add-bib /tmp/import_summary.$$.tmp | awk ' { print $1 } ' | sort -n | tail -1`
N_exibib=`grep exi-bib /tmp/import_summary.$$.tmp | awk ' { print $1 } ' | sort -n | tail -1`
N_addhol=`grep add-hol /tmp/import_summary.$$.tmp | awk ' { print $1 } ' | sort -n | tail -1`
N_delhol=`grep del-hol /tmp/import_summary.$$.tmp | awk ' { print $1 } ' | sort -n | tail -1`
N_mrgbib=`grep mrg-bib /tmp/import_summary.$$.tmp | awk ' { print $1 } ' | sort -n | tail -1`
N_mrghol=`grep mrg-hol /tmp/import_summary.$$.tmp | awk ' { print $1 } ' | sort -n | tail -1`
N_mulbib=`grep mul-bib /tmp/import_summary.$$.tmp | awk ' { print $1 } ' | sort -n | tail -1`
N_rplhol=`grep rpl-hol /tmp/import_summary.$$.tmp | awk ' { print $1 } ' | sort -n | tail -1`

if [ 0$N_exibib -gt 1000 ] || [ 0$N_mrgbib -gt 1000 ];then
  grep -v mrg-bib /tmp/import1.$$.tmp | grep -v exi-bib > /tmp/import2.$$.tmp
  echo "Fler än 1000 mrg-bib eller exi-bib poster, visas inte" >> /tmp/import2.$$.tmp
else
  mv /tmp/import1.$$.tmp /tmp/import2.$$.tmp
fi


if [ ! -s /tmp/import2.$$.tmp ] ; then
  rm /tmp/import2.$$.tmp
  exit
fi

rm /tmp/sendmail.$$.tmp > /dev/null 2>&1;

if [ 0$N_addbib -gt 0 ] ; then
  echo "  $N_addbib bibliografiska poster har lagts till (add-bib)." >> /tmp/sendmail.$$.tmp
fi

if [ 0$N_exibib -gt 0 ] ; then
  echo "  $N_exibib bibliografiska poster fanns redan i LIBRIS (exi-bib, ingen åtgärd)." >> /tmp/sendmail.$$.tmp
fi

if [ 0$N_mrgbib -gt 0 ] ; then
  echo "  $N_mrgbib bibliografiska poster har uppdaterats (mrg-bib)." >> /tmp/sendmail.$$.tmp
fi

if [ 0$N_mulbib -gt 0 ] ; then
  echo "  $N_mulbib bibliografiska poster matchade mot fler än en post i LIBRIS (import ej möjligt) (mul-bib)." >> /tmp/sendmail.$$.tmp
fi

if [ 0$N_addhol -gt 0 ] ; then
  echo "  $N_addhol nya beståndsposter har lagts till (add-hol)." >> /tmp/sendmail.$$.tmp
fi

if [ 0$N_rplhol -gt 0 ] ; then
  echo "  $N_rplhol beståndsposter har uppdaterats/ersatts (rpl-hol)." >> /tmp/sendmail.$$.tmp
fi

if [ 0$N_mrghol -gt 0 ] ; then
  echo "  $N_mrghol beståndsposter har uppdaterats/mergats (mrg-hol)." >> /tmp/sendmail.$$.tmp
fi

if [ 0$N_delhol -gt 0 ] ; then
  echo "  $N_delhol beståndsposter har tagits bort (del-hol)." >> /tmp/sendmail.$$.tmp
fi

$MAIL $MAILTO << endofmessage
From: metadatatratten@libris.kb.se
Reply-To: libris@kb.se
Subject: Metadatatratten v2 ($QUEUE - $FILENAME) `date +"%Y%m%d-%H:%M:%S"`
Content-type: text/html; charset=iso-8859-1
Content-transfer-encoding: 8BIT

<html>
  <body>
    <pre>
Detta är ett automagiskt meddelande från importrutinen för $QUEUE

Mottagare: $MAILTO
Importfilnamn: $FILENAME

Resultat:
`cat /tmp/sendmail.$$.tmp`

Utförlig logg följer:

HANTERING       BIB_ID         ISXN              TITEL                        UPPHOV                     SIGEL
--------------------------------------------------------------------------------------------------------------
`cat /tmp/import2.$$.tmp`
--------------------------------------------------------------------------------------------------------------
    </pre>
  </body>
</html>

endofmessage

rm /tmp/import.$$.tmp
rm /tmp/sendmail.$$.tmp
rm /tmp/import_summary.$$.tmp
