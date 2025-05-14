# Type Normalization

The main.groovy script can be used with whelktool. It runs against all "bib" records (and records of linked works).

## During Development

### Update mappings from XL instance [WIP]

To get an up-to-date mappings.json, run:

    $ ../gradlew groovy -Dargs="scripts/typenormalization/makemappings.groovy scripts/typenormalization/mappings.json LIBRIS_SPARQL_ENDPOINT_WITH_UPDATED_MAPPINGS"

(In order for mappings to evolve, this script needs to be replaced by running its logic on whelk startup. (And refresh on changes...))

### Get Example Data

See: ./dump-lddb-excerpts.sh

Then either A or B below.

### A. Convert and load into whelk

See: ./make-lddb-datasets.sh

Then run ./testdatasets.groovy with whelktool.

### B. Dry-run

Pass directly to: ./nowhelk.groovy
and check the results using: ./makediff.sh (requires trld, e.g. installed via pip).
