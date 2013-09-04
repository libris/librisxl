#!/bin/bash
set -e
SOURCE_MARC=$1
if [[ "$SOURCE_MARC" == "" ]]; then
    echo "Usage $(basename $0) SOURCE_MARC"
    exit
fi

SOURCE_MARC=$(cd $(dirname $SOURCE_MARC); pwd)/$(basename $SOURCE_MARC)

SCRIPT_DIR=$(dirname $0)
cd whelk-extensions/ && gradle --offline -q convertIso2709ToJson -Dargs=$SOURCE_MARC
