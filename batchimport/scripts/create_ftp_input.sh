#!/bin/sh

if [ $# -ne 6 ] ; then
  echo "usage: $0 <user> <password> <queue> <dir> <getmask> <deletemask>"
  exit 1
fi

home=`(cd \`dirname $0\`/..;pwd)`
user=$1
password=$2
queue=$3
dir=$4
getmask=$5
deletemask=$6

echo "user $user $password"
echo "bin"
echo "lcd $home/queues/$queue/incoming"
echo "cd $dir"
echo "dir $getmask"
echo "mget $getmask"
echo "mdel $deletemask"
echo quit
