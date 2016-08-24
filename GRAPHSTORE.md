# Using a Graph Store

In principle, any Graph Store supporting the SPARQL 1.1 Graph Store HTTP Protocol will work.

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


