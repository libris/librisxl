#!/bin/sh

# Remove predefined prefixes (max 50 statements can be executed at once with isql)
while true; do
  DELETE_PREFIXES=$(isql -U dba -P "$2" MESSAGE=OFF BANNER=OFF VERBOSE=OFF "EXEC=SELECT TOP 50 NS_PREFIX FROM DB.DBA.SYS_XML_PERSISTENT_NS_DECL;" \
    | awk '{ print "DB.DBA.XML_REMOVE_NS_BY_PREFIX('\''"$0"'\'', 2);"}')
  if [ -z "$DELETE_PREFIXES" ]; then
      break
  fi
  isql -U dba -P "$2" EXEC="$DELETE_PREFIXES"
done

# Load custom prefixes
LOAD_PREFIXES=$(awk -F'\t' '{ print "DB.DBA.XML_SET_NS_DECL('\''"$1"'\'','\''"$2"'\'',2);" }' "$1")
isql -U dba -P "$2" EXEC="$LOAD_PREFIXES"