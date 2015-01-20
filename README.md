# Libris-XL

The project layout is as follows:

* etc/
    Configuration files for different environments and configurations. The files from here are copied into src/main/resources on project build.
* src/
    Standard gradle/maven source-layout
* dep/
    Third party libraries not available from maven central or other online repositories.
* librisxl-tools/
    Various scripts and datatools used for maintenance and operations.
    See librisxl-tools/datatools/README.md


## Dependencies

1. Install gradle from <http://gradle.org/> (or use a package manager, e.g.: brew install gradle).
2. Install elasticsearch from <http://elasticsearch.org/> (or use a package manager, e.g.: brew install elasticsearch).

Optionally, see details about using a Graph Store at the end of this document.

## Working locally

### Setup whelk.properties

Copy etc/resources/libris/whelk.properties.in into etc/resources/libris/whelk.properties and replace the placeholder values with proper ones.
Ask for directions if you don't know the proper settings.

### Start the whelk

    $ export JAVA_OPTS="-Dfile.encoding=utf-8"
    $ gradle jettyRun

.. Running at <http://localhost:8180/>

This starts a local whelk, using an embedded elasticsearch and storage configured in `etc/environment/dev/whelks.json`.

### Install script requirements

    $ pip install -r scripts/requirements.txt

### Get/create/update and load Definifion Datasets

Get/create/update datasets:

    $ cd librisxl-tools/
    $ python librisxl-tools/datatools/scripts/compile_defs.py -c librisxl-tools/datatools/cache/ -o librisxl-tools/datatools/build/

Load into the running whelk:

    $ scripts/load_defs_whelk.sh http://localhost:8180/whelk


### Import/update local storage from test data

Create a local OAI-PMH dump of examples and run a full import, load into running whelk:

    $ python librisxl-tools/scripts/assemble_oaipmh_records.py *******:**** librisxl-tools/scripts/example_records.tsv /tmp/oaidump
    $ (cd /tmp/oaidump && python -m SimpleHTTPServer) &

    Make sure whelk is running
    
    $ gradle jettyrun
    
    and go to http://localhost:8180/whelk/_operations using a browser


(Using the OAI-PMH dump makes out-of-band metadata is available, which is necessary to create links from bib data to auth data.)

Unless you have set up a graph store (see below), you need to add `-Ddisable.plugins="fusekigraphstore"` to the invocations above to avoid error messages.

There is also a script, `librisxl-tools/scripts/update_mock_storage.sh`, for uploading test documents into your local whelk. (See the script for how the actual HTTP PUT is constructed.) However, this does not create the necessary links between bib and auth.


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

5. To see JsonLD record, go to <http://localhost:8180/whelk/bib/7149593>

### Run standalone data conversion on a single document

In order to 1) Get a source record and 2) convert it to "marc-as-json", use this snippet:

    BIB_ID=7149593
    curl -s http://libris.kb.se/data/bib/$BIB_ID?format=ISO2709 -o /tmp/bibtest.iso2709
    gradle -q convertIso2709ToJson -Dargs=/tmp/bibtest.iso2709 | grep '^{' > /tmp/bibtest.json

Run the marcframe converter on that to print out the resulting JSON-LD:

    $ gradle -q runMarcFrame -Dargs=/tmp/bibtest.json

## Using a Graph Store

In principle, any Graph Store supporting the SPARQL 1.1 Graph Store HTTP Protocol will work.

### Using Sesame

1. Install Tomcat (unless present on your system). E.g.:

        $ brew install tomcat

2. Download the Sesame distro (SDK) from <http://openrdf.org/>.

3. Unpack and put the two war files into a running Tomcat. E.g.:

        $ cp war/openrdf-{sesame,workbench}.war /usr/local/Cellar/tomcat/7.0.39/libexec/webapps/

4. Go to <http://localhost:8080/openrdf-workbench/> and create a new repository (named e.g. "dev-libris", using indexes "spoc,posc,opsc,cspo").

If you like to, test the repository endpoint:

    # Get the bibframe vocabulary, store it, then remove it:
    $ curl -L http://bibframe.org/vocab -o bibframe.rdf
    $ curl -X PUT -H "Content-Type:application/rdf+xml" "http://localhost:8080/openrdf-sesame/repositories/dev-libris/rdf-graphs/service?graph=http%3A%2F%2Fbibframe.org%2Fvocab%2F" --data @bibframe.rdf
    $ curl -X DELETE "http://localhost:8080/openrdf-sesame/repositories/dev-libris/rdf-graphs/service?graph=http%3A%2F%2Fbibframe.org%2Fvocab%2F"

### Using Fuseki

1. Download Fuseki from <http://jena.apache.org/download/>
2. Unpack it where you prefer 3d party tools/services (e.g. in /opt/fuseki)
3. Start a non-persistent in-memory server for quick local testing:

        $ ./fuseki-server --update --memTDB --set tdb:unionDefaultGraph=true /libris

This is now available as:

    $ GRAPH_STORE=http://localhost:3030/libris/data
    $ ENDPOINT=http://localhost:3030/libris/query

### Using Virtuoso

1. Install Virtuoso; e.g. by running:

    $ brew install virtuoso

2. Launch:

    $ cd cd /usr/local/Cellar/virtuoso/7.1.0/var/lib/virtuoso/db/
    $ virtuoso-t -f

3. Find your way through the Conductor interface at <http://127.0.0.1:8890/>.
   Add a user with `SPARQL_UPDATE` access and set sparql-auth to Basic HTTP
   authentication (due to Camel http4 having problems using Digest ...).

4. Edit whelk.json for the relevant environment:

    "GRAPHSTORE_DATA_URI": "http://127.0.0.1:8890/sparql-graph-crud",
    "GRAPHSTORE_QUERY_URI": "http://127.0.0.1:8890/sparql",
    "GRAPHSTORE_UPDATE_URI": "http://127.0.0.1:8890/sparql-auth",
    "GRAPHSTORE_UPDATE_POST_PARAMETER": "update",
    "GRAPHSTORE_UPDATE_AUTH_USER": "dba",
    "GRAPHSTORE_UPDATE_AUTH_PASS": ...


## Whelk maintenance (rebuilding and reloading)

All whelk maintenance is controlled from the operations interface (<whelkhost>/\_operations).

### New format

If the JSONLD format has been updated, in such a way that the marcframeconverter need to be run, the only options are either to reload the data from a marcxml storage (currently untested and probably not working) or reloading the data from OAIPMH.

#### From OAIPMH

1.  If the changes are such that a new mapping is required for elasticsearch, it's best to remove the old elastic type before starting up the whelk, i.e:

    $ curl -XDELETE http://elastichost:9200/libris/[type] 

    where type is bib, auth or hold. 
    When the whelk starts up, it will detect that the type is missing and create proper mappings for the given type.

2.  In the operations gui, under "import", select which dataset to import (auth/bib/hold). If loading data from data.libris.kb.se, just erase everything from the "service url" field. Finally, hit "go".

3.  The import will now start. The current velocity and import count can be viewed from the operations-page.

##### Alternatively

1.  If the changes are such that a new mapping is required for elasticsearch, it's best to remove the old elastic type before starting up the whelk, i.e:

    $ curl -XDELETE http://elastichost:9200/libris/[type] 

    where type is bib, auth or hold. 
    When the whelk starts up, it will detect that the type is missing and create proper mappings for the given type.

2.  $ curl -XPOST http://localhost:8180/whelk/\_operations -d 'operation=import&dataset=auth,bib,hold&url=http://localhost:8000/{dataset}/oaipmh&importer=oaipmhimporter'


### Reindexing

If no significant changes are made to the format, but the elasticsearch index (the search index) is somehow out of alignment with the storage, a reindexing might be appropriate.

The reindex section of the operations gui has only one meaningful field, namely the "dataset" field. It is possible to reindex just one dataset, such as auth, bib or hold.
If this option is used, the whelk will reindex by loading data from storage according to dataset in the metaindex (.libris\_pairtree for example), and loading the data into the current search index.

If a full reindex is requested (by leaving the dataset field empty), the whelk will first create a brand new index. Once reindexing is completed, the current alias (libris) will be removed from the old index and set to the new index instead. This operation is required if new global settings, filters or mappings must be added to the index.


## Rebuilding meta

If you suspect that the metaentry index is unaligned with storage, you can rebuild it by letting the whelk load all documents from disk and creating new meta entries.

1. It is recommended to remove the old metaindex before rebuilding (but not always necessary).

    $ curl -XDELETE http://elastichost:9200/<metaindex>

    <metaindex> is typically .libris_pairtree for the primary storage metaindex. Check http://elastichost:9200/_plugin/head/ for available indices.

2. Call the rebuild operation

    $ curl http://<whelkhost>/_operations?operation=rebuild

## Re-convert data

To run the conversion from MARC to JSON-LD again for all records, call transfer:

    $ curl http://<whelkhost>/_operations?operation=transfer&fromStorage=pairtreeoaistorage&toStorage=pairtreehybridstorage

## Component synchronization

When starting up the whelk, each component checks if it needs to update it's data. This happens automatically by each component loading it's state, stored in WHELK\_WORK\_DIR/componentname.state. WHELK\_WORK\_DIR is configured in whelk.json.
If no state file can be found for a component, it is automatically created and the "last\_updated" parameter is set to epoch. Since this would be a big operation to catch up at startup, it needs to be forcibly requested by a human to start. 

    $ curl http://<whelkhost>/_operations?operation=ping

This is analogous to a full reindex, except that no new index will be created. 

If the statefiles are missing, but you know that the index is in a reasonable state, you could just create a statefile by hand with a more recent "last\_updated".

## Disaster recovery

Say that the server has melted into a tiny puddle of silicon and plastic. After reinstalling server software, such as elasticsearch, tomcat etc, do this:

1. Don't worry.

2. Deploy the whelk normally.

3. Reload all data from OAIPMH (se above, starting at step 2 (No need to delete anything. There isn't anything to delete.)), starting with auth. Thereafter bib, and finally hold.

## APIs and URLs
whelk, DOCUMENT API:
http://<host>:<PORT>/whelk/bib/7149593

ELASTIC SEARCH, INDEX SEARCH API:
Find 'tove' in libris index, index-typedoc 'person':
http://<host>:9200/libris/auth/_search?q=tove

ELASTIC SEARCH, MAPPING:
PUT mapping with prop config:
curl -XPUT http://<host>:9200/libris/bib/_mapping -d@etc/resources/_all/default_mapping.json

ELASTIC SEARCH, ANALYZE INDEXED VALUES FOR A SPECIFIC FIELD:
curl -XGET http://<host>:9200/libris/auth/_search -d '{ "facets" : { "my_terms" : { "terms" : { "size" : 50, "field" : "about.controlledLabel.untouched" } } } }'

whelk, SEARCH API:
http://<host>:<PORT>/whelk/<indextype>/_search?q=(<field>:)strindberg
OR 
http://<host>:<PORT>/whelk/<indextype>?<field>:strindberg

INDEX TYPES:
bib, auth, person, def, sys

EXPAND AUTOCOMPLETE:
http://<host>:<PORT>/whelk/_expand?name=Jansson,%20Tove,%201914-2001.

REMOTESEARCH
http://<host>:<PORT>/whelk/_remotesearch?q=astrid

HOLDINGCOUNT
http://<host>:<PORT>/whelk/_libcount?id=/resource/bib/7149593

## Dependencies

### Elastic Search Component for Camel

Built for ES 1.3.4, needs to be recompiled. Also, remove the log4j.properties before building to get rid of the target-catalog.
