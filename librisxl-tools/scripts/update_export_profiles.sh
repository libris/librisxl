# The purpose of this script is to copy export profiles from the legacy Libris environment
# into the XL database.

#!/bin/bash
set -e

PROFILE_DIR="/appl/exportgui/profiles"
PSQL_HOST=""
PSQL_USERNAME=""
PSQL_DB=""
# note: the PGPASSWORD env variable is a "magic" name looked for by psql.
export PGPASSWORD=""

# Accept spaces in the below loop
ORIG_IFS=$IFS
IFS=$(echo -en "\n\b")
pushd $PROFILE_DIR

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

echo $query > /tmp/reload.sql
psql -h $PSQL_HOST -U $PSQL_USERNAME -d $PSQL_DB < /tmp/reload.sql
