#!/bin/sh

HOME=`dirname $0`/..
#ECHO="/bin/echo -e"
ECHO=/usr/bin/echo

if [ $# -ne 1 ] ; then
  echo "usage: $0 <file>"
  exit 1
fi

cat $1 | while read queue type server user password dir getmask deletemask
do
  if [ "$queue" = "" ] ; then
    continue;
  fi

  if [ `echo $queue | cut -b -1` = "#" ] ; then
    continue;
  fi

  if [ $type = "ftp" ] ; then
   $HOME/scripts/create_ftp_input.sh "$user" "$password" "$queue" "$dir" "$getmask" "$deletemask" | ftp -in $server
  fi
done
