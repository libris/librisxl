# Libris-XL

The project layout is as follows:

* General modules
    * core/
        The LIBRISXL core. Database, index and basic infrastructure components.
    * converters/
        Module containing data converters.
* Applications
    * rest/
        A servlet web application. The REST and other HTTP APIs
    * harvesters/
        An OAIPMH harvester. Servlet web application.
    * oaipmh/
        Servlet web application. OAIPMH service for LIBRISXL
    * importers/
        Java application to load or reindex data into the system.
    * integration/
        A standalone camel application, responsible for asynchronous tasks performed by
        the other applications
* Other
    * dep/
        Third party libraries not available from maven central or other online repositories.
    * librisxl-tools/
        Various scripts used for maintenance and operations.


## Dependencies

1. Install gradle from \<http://gradle.org/\> (or use a package manager, e.g.: brew install gradle). Check gradle -version and make sure that Groovy version matches groovyVersion in build.gradle.
2. Install elasticsearch from \<http://elasticsearch.org/\> (or use a package manager, e.g.: brew install elasticsearch).
    2.1. For elasticsearch version 2.2 and greater, you must also install the delete-by-query plugin.
        $ bin/plugin install delete-by-query
3. Install postgresql. At least version 9.4 (brew install postgresql)

Optionally, see details about using a Graph Store at the end of this document.

*IMPORTANT*: Some instructions below are obsolete. Some needs updating, some to be removed.

## Working locally

### Using PostgreSQL

1. Install postgresql; e.g. by running:

    $ brew install postgresql

2. Launch:

    $ postgres -D /usr/local/var/postgres

3. Create database

    $ createdb whelk

### Creating tables in postgresql

  $ psql [-U yourdatabaseuser] yourdatabaseschema
   \< librisxl-tools/postgresql/tables.sql
  $ psql [-U yourdatabaseuser] yourdatabaseschema
   \< librisxl-tools/postgresql/indexes.sql

### Creating index and mappings in elasticsearch

From the librisxl repository root

  $ curl -XPOST http://localhost:9200/yourindexname
      -d@librisxl-tools/elasticsearch/libris_config.json

### Setup whelk properties

Any properties bundled in the applications can be overridden by adding a -Dxl."property" system property when starting.

For example:

  $ java -Dxl.component.properties=/srv/component.properties application

If no -D system property is specified, the system will look for the property file in classpath.

#### Core Module

There are two property files in the core module that needs configuring.

* component.properties
  * Determines which components (classes) are used for storage, index and apix implementations.
  The file is preconfigured with sensible defaults and you probably won't need to change them. IF, however you want to use something different. Make a copy of the file and use the -Dxl.component.properties setting to indicate your own custom file.
* secret.properties
  * Contains passwords and and such. Make a copy of the secret.properties.in file and enter proper settings.
  * When running locally, use http://localhost:8180/ as baseUri. Document identifiers will be resolved on this, which will make redirects and such work properly.

#### importers module

* mysql.properties
  * Copy the mysql.properties.in file and supply connection settings for mysql

#### harvesters module

* oaipmh.properties
  * Copy the oaipmh.properties.in file and supply settings for the OAIPMH server

For all modules: ask for directions if you don't know the proper settings.

### Importing data (NEW)

First, create these config files from corresponding ".in"-files in the same
directories, and fill out details:

* core/src/main/resources/component.properties
    Used to decide which implementations are used for e.g. index and storage
* core/src/main/resources/secret.properties
    Passwords and stuff for databases and such
* importers/src/main/resources/mysql.properties
    Passwords and connection uri's for vcopy imports

Then:

    $ cd importers/

and run the following to import and index data into a whelk (psql/es-combo)
from a mysql-backed vcopy:

    $ gradle doRun -Dargs="auth"

### Start the whelk

    $ cd rest/
    $ export JAVA_OPTS="-Dfile.encoding=utf-8"
    $ gradle jettyRun

.. Running at \<http://localhost:8180/\>


### Get/create/update and load Definition Datasets

This requires a checkout of the separate repository called "definitions", located beside this repository.

    $ cd ../definitions/

Install script requirements

    $ pip install -r scripts/requirements.txt

Get/create/update datasets:

    $ python datasets.py -l

Go back to the importers module.

    $ cd ../librisxl/importers

If you want to clear out any existing definitions (for reload or refresh)

    $ psql [-U yourdatabaseuser] yourdatabaseschema -c "DELETE FROM lddb__identifiers WHERE id IN (SELECT id FROM lddb WHERE manifest->>'collection' = 'definitions');"
    $ psql [-U yourdatabaseuser] yourdatabaseschema -c "DELETE FROM lddb WHERE manifest->>'collection' = 'definitions';"
    $ psql [-U yourdatabaseuser] yourdatabaseschema -c "DELETE FROM lddb__versions WHERE manifest->>'collection' = 'definitions';"
    $ curl -XDELETE http://localhost:9200/yourindexname/definitions/_query -d '{"query":{"match_all": {}}}'

Load the resulting resources into the running whelk:

    $ gradle -Dargs="defs ../../definitions/build/definitions.jsonld.lines" doRun


### Import/update local storage from test data

Create a local OAI-PMH dump of examples and run a full import:

    $ python librisxl-tools/scripts/assemble_oaipmh_records.py src/main/resources/secrets.json librisxl-tools/scripts/example_records.tsv /tmp/oaidump
    $ (cd /tmp/oaidump && python -m SimpleHTTPServer) &

Modify your oaipmh.properties to indicate oaipmhServiceUrl as http://localhost:8000/{dataset}/oaipmh

    $ cd oaipmhharvester/

    $ gradle jettyrun

    and go to http://localhost:8180/oaipmhharvester using a browser.

    To make sure all data is loaded, stop all harvester threads, set the "reload from" value to 1970 or equally old and restart them. The data will now load from your mock OAIPMH server.


(Using the OAI-PMH dump makes out-of-band metadata available, which is necessary to create links from bib data to auth data.)

There is also a script, `librisxl-tools/scripts/update_mock_storage.sh`, for uploading test documents into your local whelk. (See the script for how the actual HTTP PUT is constructed.) However, this does not create the necessary links between bib and auth.


### Import a single record from Libris OAI-PMH (in marcxml format) to locally running whelk (converting it to Libris JSON-Linked-Data format)

1. Run a local mock-configured Http standard whelk. From root librisxl/rest folder:

        $ export JAVA_OPTS="-Dfile.encoding=utf-8"
        $ gradle jettyrun

2. Run get-and-put-record script:

        $ cd scripts
        $ get-and-put-record.sh \<bib|auth|hold\> \<id\>

3. To see JsonLD record, go to \<http://localhost:8180/bib/7149593\>

### Run standalone data conversion on a single document

In order to 1) Get a source record and 2) convert it to "marc-as-json", use this snippet:

    BIB_ID=7149593
    curl -s http://libris.kb.se/data/bib/$BIB_ID?format=ISO2709 -o /tmp/bibtest.iso2709
    gradle -q convertIso2709ToJson -Dargs=/tmp/bibtest.iso2709 | grep '^{' \> /tmp/bibtest.json

Run the marcframe converter on that to print out the resulting JSON-LD:

    $ gradle -q runMarcFrame -Dargs=/tmp/bibtest.json

## Using a Graph Store

In principle, any Graph Store supporting the SPARQL 1.1 Graph Store HTTP Protocol will work.

### Using Sesame

1. Install Tomcat (unless present on your system). E.g.:

        $ brew install tomcat

2. Download the Sesame distro (SDK) from \<http://openrdf.org/\>.

3. Unpack and put the two war files into a running Tomcat. E.g.:

        $ cp war/openrdf-{sesame,workbench}.war /usr/local/Cellar/tomcat/7.0.39/libexec/webapps/

4. Go to \<http://localhost:8080/openrdf-workbench/\> and create a new repository (named e.g. "dev-libris", using indexes "spoc,posc,opsc,cspo").

If you like to, test the repository endpoint:

    # Get the bibframe vocabulary, store it, then remove it:
    $ curl -L http://bibframe.org/vocab -o bibframe.rdf
    $ curl -X PUT -H "Content-Type:application/rdf+xml" "http://localhost:8080/openrdf-sesame/repositories/dev-libris/rdf-graphs/service?graph=http%3A%2F%2Fbibframe.org%2Fvocab%2F" --data @bibframe.rdf
    $ curl -X DELETE "http://localhost:8080/openrdf-sesame/repositories/dev-libris/rdf-graphs/service?graph=http%3A%2F%2Fbibframe.org%2Fvocab%2F"

### Using Fuseki

1. Download Fuseki from \<http://jena.apache.org/download/\>
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

3. Find your way through the Conductor interface at \<http://127.0.0.1:8890/\>.
   Add a user with `SPARQL_UPDATE` access and set sparql-auth to Basic HTTP
   authentication (due to Camel http4 having problems using Digest ...).

4. Edit whelk.json for the relevant environment:

    "GRAPHSTORE_DATA_URI": "http://127.0.0.1:8890/sparql-graph-crud",
    "GRAPHSTORE_QUERY_URI": "http://127.0.0.1:8890/sparql",
    "GRAPHSTORE_UPDATE_URI": "http://127.0.0.1:8890/sparql-auth",
    "GRAPHSTORE_UPDATE_POST_PARAMETER": "update",

5. Configure whelk.properties with user credentials for updates.

    # Virtuoso credentials
    graphstoreUpdateAuthUser=dba
    graphstoreUpdateAuthPass=...



## Whelk maintenance (rebuilding and reloading)

### New Index Config

If a new index is to be set up, and unless you run locally in a pristine setup, you need to put the config to the index, like (replace localhost with target machine):

    $ curl -XPUT http://localhost:9200/indexname_versionnumber -d @librisxl-tools/elasticsearch/libris_config.json

Create an alias for your index

    $ curl -XPOST http://localhost:9200/_aliases -d  '{"actions":[{"add":{"index":"indexname_versionnumber","alias":"indexname"}}]}'


(To replace an existing setup with entirely new configuration, you need to delete the index `curl -XDELETE http://localhost:9200/\<indexname\>/` and read all data again (even locally).)

### New format

If the JSONLD format has been updated, in such a way that the marcframeconverter need to be run, the only options is to reload the data from vcopy using vcopyimporter.

### Reindexing (DEPRECATED)

If no significant changes are made to the format, but the elasticsearch index (the search index) is somehow out of alignment with the storage, a reindexing might be appropriate.

The reindex section of the operations gui has only one meaningful field, namely the "dataset" field. It is possible to reindex just one dataset, such as auth, bib or hold.
If this option is used, the whelk will reindex by loading data from storage according to dataset in the metaindex (.libris\_pairtree for example), and loading the data into the current search index.

If a full reindex is requested (by leaving the dataset field empty), the whelk will first create a brand new index. Once reindexing is completed, the current alias (libris) will be removed from the old index and set to the new index instead. This operation is required if new global settings, filters or mappings must be added to the index.


## Disaster recovery

Say that the server has melted into a tiny puddle of silicon and plastic. After reinstalling server software, such as elasticsearch, tomcat etc, do this:

1. Don't worry.

2. Deploy the whelk normally.

3. Reload all data from OAIPMH (se above, starting at step 2 (No need to delete anything. There isn't anything to delete.)), starting with auth. Thereafter bib, and finally hold.

## APIs and URLs
whelk, DOCUMENT API:
http://\<host\>:\<PORT\>/bib/7149593

ELASTIC SEARCH, INDEX SEARCH API:
Find 'tove' in libris index, index-typedoc 'person':
http://\<host\>:9200/libris/auth/\_search?q=tove

ELASTIC SEARCH, ANALYZE INDEXED VALUES FOR A SPECIFIC FIELD:
curl -XGET http://\<host\>:9200/libris/auth/\_search -d '{ "facets" : { "my_terms" : { "terms" : { "size" : 50, "field" : "about.controlledLabel.raw" } } } }'

whelk, SEARCH API:
http://\<host\>:\<PORT\>/\<indextype\>/?\<field\>=\<value\>

INDEX TYPES:
bib, auth, person, def, sys

EXPAND AUTOCOMPLETE:
http://\<host\>:\<PORT\>/\_expand?name=Jansson,%20Tove,%201914-2001.

REMOTESEARCH
http://\<host\>:\<PORT\>/\_remotesearch?q=astrid

HOLDINGCOUNT
http://\<host\>:\<PORT\>/\_libcount?id=/resource/bib/7149593
