#!/bin/bash

HOME=/appl/import
JAR=$HOME/lib/batchimport.jar
SECRET=$HOME/etc/secret.properties.qa

FLAGS="$@"

if [ "$QUEUE" != "" ]; then

	RUNNING=/tmp/import/running/$QUEUE

	touch $RUNNING

	FILE=`ls -1tr $HOME/queues/$QUEUE/incoming | head -1` # oldest first

	if [ "$FILE" != '' ]; then

		echo $FILE > $RUNNING

		java -Xmx4G -Dxl.secret.properties=$SECRET -Dlog4j.configurationFile=$HOME/lib/log4j2.xml -jar $JAR --path=$HOME/queues/$QUEUE/incoming/$FILE --parallel --live $FLAGS

		if [ $? -eq 0 ]; then

			# todo? report import
			# todo: check errors in logfile

			if [ ! -d "$HOME/queues/$QUEUE/done" ]; then
				mkdir $HOME/queues/$QUEUE/done
			fi

			mv $HOME/queues/$QUEUE/incoming/$FILE $HOME/queues/$QUEUE/done	
			rm $RUNNING

		else 
			echo "fatal: import-lxl $QUEUE failed."
			# mail someone
			mailx -s "batchimport failed" kai.poykio@kb.se <<-EOF
			$QUEUE $FILE	
EOF
		fi
	else
		rm $RUNNING
	fi
fi
