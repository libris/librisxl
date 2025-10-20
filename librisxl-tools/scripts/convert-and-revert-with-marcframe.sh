#!/bin/bash
SECRET_PROPS=$1
TEST_FOLDER=$2
RECORD_NAME="$3_$(date +%Y%m%d-%H%M%S)"

FILEPATH="$HOME/$TEST_FOLDER"


../gradlew -Dxl.secret.properties=../$1 runMarcFrame -Dargs="convert ${FILEPATH}/$3.json"  | grep '^{' | python3 -m json.tool > ${FILEPATH}/${RECORD_NAME}_converted.jsonld

../gradlew -Dxl.secret.properties=../$1 runMarcFrame -Dargs="revert ${FILEPATH}/${RECORD_NAME}_converted.jsonld"  | grep '^{' | python3 -m json.tool > ${FILEPATH}/${RECORD_NAME}_reverted.json
