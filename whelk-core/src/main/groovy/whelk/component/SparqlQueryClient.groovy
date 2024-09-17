package whelk.component

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.ResultSet
import whelk.Document
import whelk.JsonLd
import whelk.converter.JsonLdToTrigSerializer
import whelk.util.DocumentUtil

import static java.nio.charset.StandardCharsets.UTF_8
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.RECORD_KEY
import static whelk.JsonLd.THING_KEY
import static whelk.JsonLd.asList

import static trld.trig.Serializer.collectPrefixes

@Log
@CompileStatic
class SparqlQueryClient {
    public static final String GRAPH_VAR = 'graph'

    private static final String RECORD_TMP_ID = "TEMP_ID"
    private static final String THING_TMP_ID = "TEMP_ID#it"
    private static final String EMPTY_BLANK_NODE_TMP_ID = "EMPTY_BN_ID"

    String sparqlEndpoint
    JsonLd jsonLd
    String prefixes

    SparqlQueryClient(String sparqlEndpoint, JsonLd jsonLd) {
        this.sparqlEndpoint = sparqlEndpoint
        this.jsonLd = jsonLd
        this.prefixes = getNsPrefixes(jsonLd.context)
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

        // for experimenting without a local Virtuoso installation
        if ("true" == System.getProperty("xl.test.rewriteSparqlResultIds")) {
            log.warn("Rewriting result ids to ${Document.getBASE_URI()}")
            ids = ids.collect {
                it.replaceFirst("https?://[^/]*/", Document.getBASE_URI().toString())
            }
        }

        return ids;
    }

    // TODO should this live here or together with whelk.datatool.form.Form?
    List<String> queryIdsByForm(Map form) {
        def graphPattern = sparqlify(form, jsonLd.context)
        return queryIdsByPattern(prefixes, graphPattern)
    }

    private static String getNsPrefixes(Map context) {
        String prefixes = ""
        collectPrefixes(context).each { k, v ->
            prefixes += "PREFIX $k: <$v>\n"
        }
        return prefixes
    }

    static String sparqlify(Map form, Map context) {
        markUpEmpty(form)

        Map thing = new HashMap(form)
        Map record = new HashMap((Map) thing.remove(RECORD_KEY) ?: [:])

        thing[ID_KEY] = THING_TMP_ID
        record[ID_KEY] = RECORD_TMP_ID
        record[THING_KEY] = [(ID_KEY): THING_TMP_ID]

        return ((ByteArrayOutputStream) JsonLdToTrigSerializer.toTurtle(context, [record, thing]))
                .toByteArray()
                .with { new String(it, UTF_8) }
        // Add skip prelude flag to JsonLdToTrigSerializer.toTurtle?
                .with { withoutPrefixes(it) }
                .with { insertVars(it) }
    }

    private static String insertVars(String ttl) {
        return ttl.replaceAll("<$RECORD_TMP_ID>", "?$GRAPH_VAR")
                .replaceAll("<$THING_TMP_ID>", "?mainEntity")
                .replaceAll("<$EMPTY_BLANK_NODE_TMP_ID>", "[]")
    }

    private static void markUpEmpty(Map form) {
        DocumentUtil.traverse(form) { value, path ->
            if (asList(value).grep().isEmpty()) {
                return new DocumentUtil.Replace([(ID_KEY): EMPTY_BLANK_NODE_TMP_ID])
            }
        }
    }

    private static String withoutPrefixes(String ttl) {
        ttl.readLines()
                .split { it.startsWith('prefix') }
                .get(1)
                .join('\n')
                .trim()
    }
}