#!/bin/bash
SAXON_JAR=$1
BIBFRAME_REPO=$2
SOURCE=$(pwd)/$3

QUERY=$BIBFRAME_REPO/xquery/saxon.xqy
BASE=http://libris.kb.se/bib/
java -cp $SAXON_JAR net.sf.saxon.Query $QUERY marcxmluri=$SOURCE baseuri=$BASE serialization=rdfxml
