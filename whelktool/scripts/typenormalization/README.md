# Type Normalization

The main.groovy script can be used with whelktool. It runs against all "bib" records (and records of linked works).

## During Development

## Get Example Data

See: ./dump-lddb-excerpts.sh

Then either A or B below.

## A. Convert and load into whelk

See: ./make-lddb-datasets.sh

Then run ./testdatasets.groovy with whelktool.

## B. Dry-run

Pass directly to: ./nowhelk.groovy
and check the results using: ./makediff.sh (requires trld, e.g. installed via pip).
