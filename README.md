# Libris-XL

datatools/
    See datatools/README.md

Libris-XL is divided over three subprojects:
    whelk-core:
        contains the core components, whelks and storages, indexes and tripplestores.
    whelk-extensions:
        contains extensions to the core components, such as format converters. Stuff used for specific implementations.
    whelk-webapi:
        contians the sources for the web API.

## Dependencies

1. Install gradle from <http://gradle.org/> (or use a package manager, e.g.: brew install gradle).
2. Install elasticsearch from <http://elasticsearch.org/> (or use a package manager, e.g.: brew install elasticsearch).


## Working locally

### Start the whelk

    $ export JAVA_OPTS="-Dfile.encoding=utf-8"
    $ gradle jettyRun

.. Running at <http://localhost:8080/>

This starts a local whelk, using an embedded elasticsearch and storage configured in `etc/mock_whelks.json`.

### Import/update local storage from test data

Simply run:

    $ scripts/update_mock_storage.sh

to upload all test documents into your local whelk. (See the script for how the actual HTTP PUT is constructed.)

### Import a single record from Libris OAI-PMH (in marcxml format) to locally running whelk (converting it to Libris JSON-Linked-Data format)

1. Configure mock whelk with suitable converters, etc/mock-whelks.json

2. Create a jar-file. From root librisxl folder:
    $ gradle fatjar

3. Run a local mock-configured Http standard whelk. From root librisxl folder:
    $ export JAVA_OPTS="-Dfile.encoding=utf-8"
    $ gradle jettyrun

4. Run get-and-put-record script:
    $ cd datatools
    $ get-and-put-record.sh <bib|auth|hold> <id>

5. To see JsonLD record: http://localhost:8080/whelk-webapi/bib/7149593

### Run standalone data conversion on a single document

    $ gradle convertMarc2JsonLD -Dargs="/bib/7149593 src/test/resources/marc2jsonld/in/bib/7149593.json build/"
    $ gradle convertJsonLD2Marc -Dargs="build/7149593.json /tmp/"


## Setting up a proper instance

For running a proper instance (e.g. in production), you should use a standalone elasticsearch instance, and deploy a whelk war into a webapp container.

### Configure standalone elasticsearch

First set up configuration of it:

    $ cp whelk-extensions/src/main/resources/whelks-core.properties.in whelk-extensions/src/main/resources/whelks-core.properties
    $ vim whelk-extensions/src/main/resources/whelks-core.properties # ... (ask for directions)

### Perform whelk operations

Run whelkOperation gradle task to import, reindex or rebuild:

    $ gradle whelkOperation -Dargs='<import|reindex|rebuild> <whelkname> [resource (for import) or sourcestorage (for rebuild)] [since (for import)]]' -Dwhelk.config.uri=<uri-to-config-json> (-Delastic.host='<host>') (-Delastic.cluster='<cluster>') (-Dfile.encoding='<encoding>')

Example - import documents from 2000-01-01 using etc/whelks.json to configure the whelks from external sources:

    $ gradle whelkOperation -Dargs='import bib bib 2000-01-01T00:00:00Z' -Dfile.encoding='utf-8' -Dwhelk.config.uri=file:../etc/whelks.json

