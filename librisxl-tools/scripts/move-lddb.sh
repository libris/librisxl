#!/bin/bash
dumpfile=$1
env=$2

cat $dumpfile | sed "s!\(\"@id\": \"https://libris\)\(.kb.se/[bcdfghjklmnpqrstvwxz0-9]\+\)\W!\1-$env\2!g"
