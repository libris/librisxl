#!/bin/ksh

line=`cat qwer`
bibl=`echo $line |cut -d "," -f 1`
sigel=`echo $line | cut -d "," -f 2`
eposter=`echo $line |cut -d "," -f 3`
echo bibl: $bibl " " sigel: $sigel " " eposter: $eposter

for queue in pelle kalle tuula;do
  echo " "
  echo queue: $queue

  for epost in $eposter;do
    continue=0
    adress=`echo $epost | cut -d ":" -f 1`
    queuefilter=`echo $epost | awk ' BEGIN {FS=":"} { print $2 } ' | grep -v "^$"`
    noqueuefilter=$?
    echo adress: $adress " " queuefilter: $queuefilter
    if [ $noqueuefilter -gt 0 ];then
      continue=1
    else
      for filter in `echo $queuefilter | tr ";" " "`;do
        fixedqueuefilter=`echo $filter | tr "_" " "`
        echo fixedqueuefilter: $fixedqueuefilter queue: $queue
        if [ $fixedqueuefilter == $queue ];then
          continue=1
        fi
      done
    fi
    if [ $continue -gt 0 ];then
      echo WOULD MAIL to $adress
    else
      echo WOULD NOT SEND MAIL TO $adress
    fi
  done

done

