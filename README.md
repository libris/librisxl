# Libris-XL

datatools/
    See datatools/README.md

Libris-XL is divided over three subprojects:
    whelk-core:
        contains the core components, whelks and storages, indexes and tripplestores.
    whelk-extensions:
        contains extensions to the core components, such as format converters. Stuff used for specific implementations.
    whelk-webapi:
        contains the sources for the web API.

## Dependencies

1. Install gradle from <http://gradle.org/> (or use a package manager, e.g.: brew install gradle).
2. Install elasticsearch from <http://elasticsearch.org/> (or use a package manager, e.g.: brew install elasticsearch).

Optionally, see details about using a Graph Store at the end of this document.

## Working locally

### Start the whelk

    $ export JAVA_OPTS="-Dfile.encoding=utf-8"
    $ gradle jettyRun

.. Running at <http://localhost:8180/>

This starts a local whelk, using an embedded elasticsearch and storage configured in `etc/environment/dev/whelks.json`.

### Import/update local storage from test data

Simply run:

    $ scripts/update_mock_storage.sh

to upload all test documents into your local whelk. (See the script for how the actual HTTP PUT is constructed.)

Or better, create a local OAI-PMH dump of examples and run a full import:

    $ python scripts/assemble_oaipmh_records.py *******:**** scripts/example_records.tsv /tmp/oaidump
    $ (cd /tmp/oaidump && python -m SimpleHTTPServer) &
    $ for d in auth bib; do gradle whelkOperation -Dargs="-o import -w libris -d $d -u http://localhost:8000/$d"; done
    $ gradle whelkOperation -Dargs='-o linkfindandcomplete -w libris -d bib'
    $ fg
    <CTRL-C>

(The benefit of this is that out-of-band metadata is available, which is necessary to create links from bib data to auth data.)

You need to add `-Ddisable.plugins="indexingprawn,sesamegraphstore"` to the invokations above unless you have set up a graph store (see below).


### Import a single record from Libris OAI-PMH (in marcxml format) to locally running whelk (converting it to Libris JSON-Linked-Data format)

1. Configure mock whelk with suitable converters, etc/environment/dev/whelks.json

2. Create a jar-file. From root librisxl folder:
    $ gradle fatjar

3. Run a local mock-configured Http standard whelk. From root librisxl folder:
    $ export JAVA_OPTS="-Dfile.encoding=utf-8"
    $ gradle jettyrun

4. Run get-and-put-record script:
    $ cd scripts
    $ get-and-put-record.sh <bib|auth|hold> <id>

5. To see JsonLD record: http://localhost:8180/whelk-webapi/bib/7149593

### Run standalone data conversion on a single document

    $ gradle convertMarc2JsonLD -Dargs=src/test/resources/marc2jsonld/in/bib/7149593.json

## Setting up a proper instance

For running a proper instance (e.g. in production), you should use a standalone elasticsearch instance, and deploy a whelk war into a webapp container.

## Environment Configuration

The application requires a couple of environment variables to be defined:

    file.encoding=utf8
    whelk.config.uri=file:///<path-to-whelks.json>
    plugin.config.uri=file:///<path-to-plugins.json>
    elastic.host=<hostname>
    elastic.cluster=<clustername>
    info.aduna.platform.appdata.basedir=/<path-to-aduna>

If you are serving the Web API using Tomcat, you may be able to define these (via JAVA_OPTS), in:

    $CATALINA_HOME/bin/setenv.sh

### Configure standalone elasticsearch

First set up configuration of it:

    $ cp whelk-extensions/src/main/resources/oaipmh.properties.in whelk-extensions/src/main/resources/oaipmh.properties
    $ vim whelk-extensions/src/main/resources/oaipmh.properties # ... (ask for directions)

### Perform whelk operations

Run whelkOperation gradle task to import, reindex or rebuild:

    $ gradle whelkOperation -Dargs='ARGS' -Dwhelk.config.uri=<uri-to-config-json> (-Delastic.host='<host>') (-Delastic.cluster='<cluster>') (-Dfile.encoding='<encoding>')
   
   Where ARGS is:
     -d,--dataset <arg>      dataset (bib|auth|hold)
     -n,--num <arg>          maximum number of document to import
     -o,--operation <arg>    which operation to perform (import|reindex|etc)
     -p,--picky <arg>        picky (true|false)
     -s,--since <arg>        since Date (yyyy-MM-dd'T'hh:mm:ss) for OAIPMH
     -u,--serviceUrl <URL>   serviceUrl for OAIPMH
     -w,--whelk <arg>        the name of the whelk to perform operation on
                             e.g. libris
     -c,--component <arg>    which components to use for reindexing (defaults to all)

Example - import a maximum of 10000 documents since 2000-01-01 using etc/whelksoperations.json to configure the whelks from external sources:

    $ gradle whelkOperation -Dargs='-o import -w libris -d bib -s 2000-01-01T00:00:00Z -n 10000 -p true' -Dfile.encoding='utf-8' -Dwhelk.config.uri=file:etc/whelkoperations.json

Example - "reindex" tripple store, performing load from storage, turtle conversion and adding to tripple store:

    $ gradle whelkOperation -Dargs='-o reindex -w libris -c sesamegraphstore'

## Using a Graph Store

In principle, any Graph Store supporting the SPARQL 1.1 Graph Store HTTP Protocol will work.

1. Install Tomcat, e.g. using Homebrew:
    $ brew install tomcat

2. Download Sesame from <http://openrdf.org/>
    - Unpack and put the two war files into a running Tomcat
    - Go to <http://localhost:8080/openrdf-workbench/> and create a new repository (e.g. "dev-libris")

3. Test the endpoint:

        $ curl -L http://bibframe.org/vocab -o bibframe.rdf
        $ curl -X PUT -H "Content-Type:application/rdf+xml" "http://localhost:8080/openrdf-sesame/repositories/test-mem/rdf-graphs/service?graph=http%3A%2F%2Fbibframe.org%2Fvocab%2F" --data @bibframe.rdf

