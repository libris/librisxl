# The purpose of this script is to copy export profiles from the legacy Libris environment
# into the XL database.

#!/bin/bash
set -e

SOURCE_HOST="gosling.libris.kb.se"
SOURCE_PATH="/appl/export2/profiles"
SOURCE_LOGIN="root"
SOURCE_PASSWD=""

TEMP_AREA="/tmp/profiles"

PSQL_HOST=""
PSQL_USERNAME=""
PSQL_DB=""
# note: the PGPASSWORD env variable is a "magic" name looked for by psql.
export PGPASSWORD=""

mkdir -p $TEMP_AREA
sshpass -p $SOURCE_PASSWD scp  $SOURCE_LOGIN@$SOURCE_HOST:$SOURCE_PATH/*.properties $TEMP_AREA

# Accept spaces in the below loop
ORIG_IFS=$IFS
IFS=$(echo -en "\n\b")
pushd $TEMP_AREA

query="BEGIN; DELETE FROM lddb__profiles;"

declare -A used_sigels

for profile in *.properties
do
    data=`cat $profile`
    sigel_list=`cat $profile | grep locations | sed -e 's/locations=//' | sed -e 's/ /\n/g'`

    # Filter out "locations=*"
    if echo x"$sigel_list" | grep '*' > /dev/null; then
	continue
    fi

    for var in $sigel_list
    do
        profile_no_ext=$(echo $var | cut -d'.' -f 1)
        if [[ ${used_sigels[$profile_no_ext]} != used ]]; then
            used_sigels[$profile_no_ext]="used"
            query="$query INSERT INTO lddb__profiles (library_id, profile) VALUES ('https://libris.kb.se/library/$profile_no_ext', '$data');"
        fi
    done

done

popd
IFS=""
query="$query COMMIT;"

echo $query > $TEMP_AREA/reload.sql
psql -h $PSQL_HOST -U $PSQL_USERNAME -d $PSQL_DB < $TEMP_AREA/reload.sql
