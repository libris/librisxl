#!/bin/bash

BASEDIR=$(pwd)
LIBDIR=$BASEDIR/dep/lib

# create lib dir if not already there
if [ ! -f $LIBDIR ]; then
    mkdir -p $LIBDIR
fi

# whelk-core
cd $BASEDIR/../whelk-core/
gradle clean jar
cp -f build/libs/xlcore.jar $LIBDIR
