#!/bin/bash

BATCHIMPORT=/appl/src/librisxl/batchimport
GRADLE=/opt/gradle/bin/gradle
GIT=/bin/git
DEPLOY=/appl/import/lib/

bailout ()
{
	mailx -s "deploy batchimport failed" kai.poykio@kb.se <<-EOF
	$1
EOF
	exit 1
}


if [ ! -f $GRADLE ]; then
	bailout "fatal: install gradle."
fi

if [ ! -f $GIT ]; then
	bailout "fatal: install git."
fi

if [ -d $BATCHIMPORT ]; then
	cd $BATCHIMPORT
	$GIT pull
	if [ $? -ne 0 ]; then
		bailout "fatal: git pull failed."
	fi 
	$GRADLE jar --refresh-dependencies
	if [ $? -ne 0 ]; then
		bailout "fatal: gradle build failed."
	fi 
	cp -f $BATCHIMPORT/build/libs/batchimport.jar $DEPLOY
	if [ $? -ne 0 ]; then
		bailout "fatal: cp batchimport.jar failed."
	fi 
else
	bailout "fatal: clone repo"
	# TODO:
	# mkdir 
	# git clone
fi
