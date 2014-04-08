#!/bin/bash
USAGE="Usage: $(basename $0) VOCAB BUILD_DIR"
[[ $# < 2 ]] && echo $USAGE && exit 1

VOCAB=$1
BUILD_DIR=$2
mkdir -p $BUILD_DIR

D3JS=$BUILD_DIR/d3.v3.min.js
curl http://d3js.org/d3.v3.min.js -o $D3JS -z $D3JS

pushd $(dirname $0)
    python vocab_to_html.py $VOCAB > $BUILD_DIR/index.html
    cp graph.* $BUILD_DIR/
    cp vocab.* $BUILD_DIR/
popd
