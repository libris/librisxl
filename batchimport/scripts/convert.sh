#!/bin/sh

HOME=`(cd \`dirname $0\`/..;pwd)`

cat $HOME/etc/convert.txt | while read queue type xslt comptype cencoding
do
  for file in $HOME/queues/$queue/incoming/*
  do
    if [ $type = xml ] ; then
      if [ $comptype = gzip ] ; then
        gunzip -c $file | $HOME/scripts/transform.sh $HOME/etc/$xslt |$HOME/scripts/hyphenate.sh -inType=XML -outType=XML | $HOME/scripts/xml2iso.sh -outEncoding=VRLIN > $HOME/queues/$queue/tmp/`basename $file`.iso2709
      else
        cat  $file | $HOME/scripts/transform.sh $HOME/etc/$xslt |$HOME/scripts/hyphenate.sh -inType=XML -outType=XML | $HOME/scripts/xml2iso.sh -outEncoding=VRLIN > $HOME/queues/$queue/tmp/`basename $file`.iso2709
      fi
    elif [ $type = iso2709 ] ; then
      $HOME/scripts/iso2xml.sh -inEncoding=$cencoding < $file |  $HOME/scripts/transform.sh $HOME/etc/$xslt |$HOME/scripts/hyphenate.sh -inType=XML -outType=XML | $HOME/scripts/xml2iso.sh -outEncoding=VRLIN > $HOME/queues/$queue/tmp/`basename $file`.iso2709
    fi

    #mv $HOME/queues/$queue/tmp/`basename $file`.iso2709 $HOME/queues/$queue/done/`basename $file`.iso2709
  done
done


#for dir in $HOME/queues/*
#do
#  cat $HOME/etc/convert.txt | grep $dir | read xx 
#  for file in $dir/incoming/*
#    do
#      echo $file
#    done
#done
