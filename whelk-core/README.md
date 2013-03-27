# Whelks Core README


## Dependencies

1. Install gradle from <http://gradle.org/> (or use a package manager, e.g.: brew install gradle).
2. Install elasticsearch from <http://elasticsearch.org/> (or use a package manager, e.g.: brew install elasticsearch).


## Working locally

### Start the whelk

    $ export JAVA_OPTS="-Dfile.encoding=utf-8"
    $ gradle jettyRun

.. Running at <http://localhost:8080/>

This starts a local whelk, using an embedded elasticsearch and storage configured in `src/main/resources/mock_whelks.json`.

### Import/update local storage from test data

Simply run:

    $ scripts/update_mock_storage.sh

to upload all test documents into your local whelk. (See the script for how the actual HTTP PUT is constructed.)

### Run standalone data conversion on a single document

    $ gradle convertMarc2JsonLD -Dargs="/bib/7149593 src/test/resources/marc2jsonld/in/bib/7149593.json build/"
    $ gradle convertJsonLD2Marc -Dargs="build/7149593.json /tmp/"


## Setting up a proper instance

For running a proper instance (e.g. in production), you should use a standalone elasticsearch instance, and deploy a whelk war into a webapp container.

### Configure standalone elasticsearch

First set up configuration of it:

    $ cp src/main/resources/whelks-core.properties.in src/main/resources/whelks-core.properties
    $ vim src/main/resources/whelks-core.properties # ... (ask for directions)

### Perform whelk operations

Run whelkOperation gradle task to import, reindex or rebuild:

    $ gradle whelkOperation -Dargs='<import|reindex|rebuild> <whelkname> <config-url> [resource (for import) or sourcestorage (for rebuild)] [since (for import)] [maxNoOfDocs (for import)]' -Delastic.host='<host>' (-Delastic.cluster='<cluster>') (-Dfile.encoding='<encoding>')'

Example - import of 100 documents to mock-whelk from external sources:

    $ gradle whelkOperation -Dargs='import bib file:src/main/resources/mock_whelks.json bib 100' -Dfile.encoding='utf-8'

