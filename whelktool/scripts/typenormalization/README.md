# Type Normalization

The main.groovy script can be used with whelktool. It runs against all "bib" records (and records of linked works).

## During Development

### Get Example Data

See: ./dump-lddb-excerpts.sh

Then either A or B below.

### A. Convert and load into whelk

See: ./make-lddb-datasets.sh

Then run ./testdatasets.groovy with whelktool.

### B. Dry-run

Pass directly to: ./nowhelk.groovy
and check the results using: ./makediff.sh (requires trld, e.g. installed via pip).

Or run against selected environment (replace SELECTED_ENV as you need):

    $ time java -Xmx2G -Dxl.secret.properties=$HOME/secret.properties-SELECTED_ENV -Dtypenormalization=simple-types-algorithm -DaddCategory=true -DreplaceIssuanceTypes=false -jar build/libs/whelktool.jar --report reports/SELECTED_ENV-dryrun-$(date +%Y%m%d-%H%M) --dry-run --skip-index --no-threads --validation OFF scripts/typenormalization/main.groovy

