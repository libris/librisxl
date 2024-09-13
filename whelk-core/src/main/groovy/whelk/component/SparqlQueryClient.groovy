package whelk.component

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.ResultSet
import whelk.JsonLd
import whelk.converter.JsonLdToTrigSerializer

import static java.nio.charset.StandardCharsets.UTF_8
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.RECORD_KEY

@Log
@CompileStatic
class SparqlQueryClient {
    public static final String GRAPH_VAR = 'graph'

    private static final String RECORD_TMP_ID = "TEMP_ID"
    private static final String THING_TMP_ID = "TEMP_ID#it"

    String sparqlEndpoint
    JsonLd jsonLd

    SparqlQueryClient(String sparqlEndpoint, JsonLd jsonLd) {
        this.sparqlEndpoint = sparqlEndpoint
        this.jsonLd = jsonLd
    }

    List<String> queryIdsByPattern(String prefixes, String graphPattern, long limit = -1) {
        if (!sparqlEndpoint) {
            throw new Exception('sparqlEndpoint not configured')
        }

        def queryString = """
            ${prefixes}

            SELECT DISTINCT ?${GRAPH_VAR} WHERE {
                GRAPH ?${GRAPH_VAR} {
                    ${graphPattern}
                }
            }
        """

        if (limit > 0) {
            queryString += "\nLIMIT ${limit}"
        }

        log.debug(queryString)

        QueryExecution qe = QueryExecutionFactory.sparqlService(sparqlEndpoint, queryString)
        ResultSet res = qe.execSelect()
        var ids = res.collect { it.get(GRAPH_VAR).toString() }

        // TODO remove me - for testing locally without virtuoso
        // ids = ids.stream().map(i -> i.replace("https://libris-dev.kb.se/", "http://libris.kb.se.localhost:5000/")).collect(Collectors.toList());

        return ids;
    }

    // TODO should this live here or together with whelk.datatool.form.Form?
    // TODO get prefixes in another way?
    List<String> queryIdsByForm(Map form) {
        def ttl = toTurtle(form, jsonLd.context)
        def strings = separatePrefixes(ttl)
        def (prefixes, ttlGraph) = [strings[0], strings[1]]
        def graphPattern = sparqlify(ttlGraph)

        return queryIdsByPattern(prefixes, graphPattern)
    }

    static String toTurtle(Map form, Map context = null) {
        Map thing = new HashMap(form)
        Map record = new HashMap((Map) thing.remove(RECORD_KEY) ?: [:])

        thing[ID_KEY] = THING_TMP_ID
        record[ID_KEY] = RECORD_TMP_ID

        def ttl = ((ByteArrayOutputStream) JsonLdToTrigSerializer.toTurtle(context, [record, thing]))
                .toByteArray()
                .with { new String(it, UTF_8) }

        return ttl
    }

    static List<String> separatePrefixes(String ttl) {
        ttl.readLines()
                .split { it.startsWith("prefix") }
                .collect { it.join('\n') }
    }

    static String sparqlify(String ttl) {
        var substitutions = [
                ("<$RECORD_TMP_ID>".toString()): "?$GRAPH_VAR".toString(),
                ("<$THING_TMP_ID>".toString()) : "?mainEntity",
        ]
        return ttl.replace(substitutions as Map<CharSequence, CharSequence>)
    }
}