package whelk.component

import groovy.transform.CompileStatic
import groovy.util.logging.Log4j2 as Log
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.ResultSet
import whelk.Document
import whelk.JsonLd
import whelk.converter.JsonLdToTrigSerializer

import static java.nio.charset.StandardCharsets.UTF_8
import static trld.trig.Serializer.collectPrefixes

@Log
@CompileStatic
class SparqlQueryClient {
    public static final String GRAPH_VAR = 'graph'

    String sparqlEndpoint
    JsonLd jsonLd
    String prefixes

    SparqlQueryClient(String sparqlEndpoint, JsonLd jsonLd) {
        this.sparqlEndpoint = sparqlEndpoint
        this.jsonLd = jsonLd
        this.prefixes = getNsPrefixes(jsonLd.context)
    }

    List<String> queryIdsByPattern(String graphPattern, long limit = -1) {
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

        return ids
    }

    private static String getNsPrefixes(Map context) {
        String prefixes = ""
        collectPrefixes(context).each { k, v ->
            prefixes += "PREFIX $k: <$v>\n"
        }
        return prefixes
    }
}