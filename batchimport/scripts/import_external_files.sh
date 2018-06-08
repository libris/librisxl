#!/bin/bash -x

# Copies loadfiles into the appropriate import directories

IMPS=`ps -ef|grep -c import_external`

if [ $IMPS -gt 3 ]; then 
	echo "Postponing, import_external($IMPS) in progress."
	exit
fi

dry="echo "
recipient=kai.poykio@kb.se

bailout ()
{
	#/usr/local/scripts/larmsms.sh --nosms --cc=kai.poykio@kb.se "/scripts/eholds/bin/eholds.sh failed." <<-EOF

	mailx -s "import_external failed" $recipient << EOF
	$1
EOF
	exit 1
}

# Give directory/prefix/suffix

date "+%Y-%m-%d %H:%M"

distlist="dawson//USM adlibris/adlibris/xml btj/BTJ/xml bokrondellen/artdag/xml sbci/SBCI/iso2709 ebookiso//iso2709 ebookxml//xml bokus/bokus/xml dkage/dkage/iso2709 ferdosi/F/xml ferdosi_info/Ferdosiinfo_/xml delbanco/delbanco/mrc forlagett/ForlagEtt/xml mipp//mrc laromedia/laromedia/xml"
#REDO mipp//iso2709

sourceroot=/appl/upload
targetroot=/appl/import/queues
newfiles=0

for dist in $distlist;do
	echo $dist
	directory=`echo $dist | cut -d "/" -f 1`
	prefix=`echo $dist | cut -d "/" -f 2`
	suffix=`echo $dist | cut -d "/" -f 3`
	loadedfiles=$targetroot/$directory/prev/loadedfiles.idx

	cd $sourceroot/${directory}
	pwd

	ls -1 | grep "^$prefix"| grep "$suffix$" | /appl/import/scripts/filefilter.pl $loadedfiles | while read filename; do
		newfiles=1
		# REDO, bomstrip needed?
		#/appl/import/scripts/bomstrip2.pl < $filename > $targetroot/$directory/incoming/$filename

		mv $filename $targetroot/$directory/incoming
		if [ $? -ne 0 ]; then
			bailout "mv $filename"	
		fi

		echo $filename >> $loadedfiles

	# Needed?
    # Copy dawson gbg files so that they can ftp download them. CHK IF NEEDED
    #if [ $directory == dawson ];then
    #  echo $filename | grep 444300 > /dev/null
    #  if [ $? -lt 1 ];then
    #    echo Found goteborg file $filename, copying...
    #    cp -p $filename /appl/upload/goteborg
    #  fi
    #fi

	done

  #REDO
  # Check if new files haven't arrived last four weeks...
  #sectime=`/usr/local/bin/shellsupport -t s`
  #modtime=`/usr/local/bin/shellsupport -f m loadedfiles.idx`
  #let "moddaysago=(sectime-modtime)/86400"
  #echo Latest import file downloaded $moddaysago days ago...
  #if [ $moddaysago -gt 42 ];then
  #  if [ $moddaysago -lt 47 ];then
  #    if [ ! -f /tmp/external_imports_lockfile.`date "+%Y-%m-%d"` ];then
  #      /usr/local/scripts/larmsms.sh --nosms --cc=arvid.oja@kb.se --cc=christer.larsson@kb.se "SRU load files seem old" << endofmessage
  #      No new SRU load files have appeared in the $sourceroot/${directory} directory for six weeks.
  #      Are the download scripts working properly?
  #      This message sent by script $0 on machine `uname -n`.
  #endofmessage
  #      touch /tmp/external_imports_lockfile.`date "+%Y-%m-%d"`
  #    fi
   # fi
  #fi 

	if [ $newfiles -gt 0 ];then
		mailx -s "New files in $directory import" $recipient << END
		`ls -l $targetroot/$directory/incoming`
END
	fi
  
done
