# Using a Graph Store

NOTE: this is a work-in-progress. We have not yet set up a final, continuously fed Graph Store upon XL.

In principle, any Graph Store supporting the SPARQL 1.1 Graph Store HTTP Protocol will work.

## Using BlazeGraph

Download the latest blazegraph jar (from: https://sourceforge.net/projects/bigdata/files/bigdata/; see: https://wiki.blazegraph.com/wiki/ for details):

    curl -L -O https://sourceforge.net/projects/bigdata/files/bigdata/2.1.4/blazegraph.jar

### Loading LDDB into BlazeGraph

1. Put required configuration and tools to the loading machine and/or server:

    ```
    scp -r ./librisxl-tools/blazegraph $TARGET:
    ```

2. Go to the target machine (`ssh $TARGET`).

3. Setup variables for your specific environment:

    ```
    SOURCE_HOST=pgsql-SOMEDOMAIN
    DATADIR=/tmp/lddb-loadfiles
    ```

4. Create load files (see comments for required edits and decisions):

    ```
    curl https://id-dev.kb.se/context.jsonld -o $DATADIR/context.jsonld
    curl https://raw.githubusercontent.com/libris/definitions/develop/source/vocab-overlay.jsonld -o $DATADIR/base-context.jsonld
    # TODO:
    # Edit to remove @type: xsd:dateTime (fails at times in blazegraph).
    # Also, decide whether you want context or just base-context below.

    psql -h $SOURCE_HOST -Uwhelk -tc "SELECT data FROM lddb WHERE collection = 'definitions' AND deleted = false;" |
      python ./lddb-to-import.py $DATADIR/parts/definitions 1000 ../context.jsonld
    # TODO: Fix display's multiple use of @graph (change @id of lensGroups to e.g. "_graph").

    for coll in auth bib hold ; do
      echo "Dumping $coll from LDDB"
      psql -h $SOURCE_HOST -Uwhelk -tc "SELECT data FROM lddb WHERE collection = '$coll' AND deleted = false;" |
        python ./lddb-to-import.py $DATADIR/parts/$coll 10000 ../base-context.jsonld
    done
    ```

5. Load into BlazeGraph:

    ```
    time java -Xmx32g -cp blazegraph.jar com.bigdata.rdf.store.DataLoader -verbose -durableQueues quads.properties $DATADIR/parts
    ```

### Running The SPARQL Service

Start:

    java -server -Xmx4g -Dbigdata.propertyFile=quads.properties -Djetty.overriWebXml=readonly.xml -jar blazegraph.jar

----

## Using Sesame

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

## Using Fuseki

1. Download Fuseki from \<http://jena.apache.org/download/\>
2. Unpack it where you prefer 3d party tools/services (e.g. in /opt/fuseki)
3. Start a non-persistent in-memory server for quick local testing:

        $ ./fuseki-server --update --memTDB --set tdb:unionDefaultGraph=true /libris

This is now available as:

    $ GRAPH_STORE=http://localhost:3030/libris/data
    $ ENDPOINT=http://localhost:3030/libris/query

## Using Virtuoso

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


