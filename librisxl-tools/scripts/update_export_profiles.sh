# The purpose of this script is to copy export profiles from the legacy Libris environment
# into the XL database.

#!/bin/bash
set -ue

SOURCE_HOST="gosling.libris.kb.se"
SOURCE_PATH="/appl/export2/profiles"
SOURCE_LOGIN="root"
SOURCE_PASSWD=""

TEMP_AREA="/tmp/profiles"

PSQL_HOST=""
PSQL_USERNAME=""
PSQL_DB=""
# note: the PGPASSWORD env variable is a "magic" name looked for by psql.
PGPASSWORD=""

mkdir -p $TEMP_AREA
sshpass -p $SOURCE_PASSWD scp  $SOURCE_LOGIN@$SOURCE_HOST:$SOURCE_PATH/*.properties $TEMP_AREA

# Accept spaces in the below loop
ORIG_IFS=$IFS
IFS=$(echo -en "\n\b")
pushd $TEMP_AREA

query="BEGIN; DELETE FROM lddb__profiles;"

for profile in *.properties
do

    data=`cat $profile`
    profile_no_ext=$(echo $profile | cut -d'.' -f 1)
    query="$query INSERT INTO lddb__profiles (library_id, profile) VALUES ('https://libris.kb.se/library/$profile_no_ext', '$data');"
done
popd
IFS=$ORIG_IFS
query="$query COMMIT;"

echo $query > $TEMP_AREA/reload.sql
psql -h $PSQL_HOST -U $PSQL_USERNAME -d $PSQL_DB < $TEMP_AREA/reload.sql
