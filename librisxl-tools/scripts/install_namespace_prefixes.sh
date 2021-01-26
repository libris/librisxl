#!/bin/sh

# Remove predefined prefixes
DELETE_PREFIXES=$(isql MESSAGE=OFF BANNER=OFF VERBOSE=OFF "EXEC=SELECT NS_PREFIX FROM DB.DBA.SYS_XML_PERSISTENT_NS_DECL" \
  | awk '{ print "DB.DBA.XML_REMOVE_NS_BY_PREFIX('\''"$0"'\'', 2);"}')
isql EXEC="$DELETE_PREFIXES"

# Load custom prefixes
LOAD_PREFIXES=$(awk '{ print "DB.DBA.XML_SET_NS_DECL('\''"$1"'\'','\''"$2"'\'',2);" }' $1)
isql EXEC="$LOAD_PREFIXES"