package datatool.bulkchange

import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.ResultSet
import whelk.converter.JsonLdToTrigSerializer


import static java.nio.charset.StandardCharsets.UTF_8
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.RECORD_KEY

class Selection {
    private static final String RECORD_TMP_ID = "TEMP_ID"
    private static final String THING_TMP_ID = "TEMP_ID#it"

    private static final String GRAPH = 'graph'

    private static final int LIMIT = 0
    private static final int ITEMS_PREVIEW_LIMIT = 5

    List<String> recordIds

    Selection(List<String> recordIds) {
        this.recordIds = recordIds
    }

    // TODO: Extract all Sparql stuff (move into Whelktool?)
//    Selection byForm(Map form) {
//        return new Selection(selectBySparqlWhere(toSparqlPattern(form)))
//    }

    Selection byForm(Map form, String sparqlEndpoint, Map context) {
        return new Selection(sparqlQueryIdsByForm(form, sparqlEndpoint, context))
    }

    int size() {
        return recordIds.size()
    }

    boolean isEmpty() {
        return recordIds.isEmpty()
    }

    List<String> getExampleIds() {
        return recordIds.sort().take(ITEMS_PREVIEW_LIMIT)
    }

    static List<String> sparqlQueryIdsByForm(Map form, String sparqlEndpoint, Map context) {
        def ttl = toTurtle(form, context)
        def (prefixes, ttlGraph) = separatePrefixes(ttl)
        def graphPattern = sparqlify(ttlGraph)

        def queryString = """
            ${prefixes}

            SELECT DISTINCT ?${GRAPH} WHERE {
                ${GRAPH} ?${GRAPH} {
                    ${graphPattern}
                }
            }
        """

        if (LIMIT > 0) {
            queryString += "\nLIMIT ${LIMIT}"
        }

        QueryExecution qe = QueryExecutionFactory.sparqlService(sparqlEndpoint, queryString)
        ResultSet res = qe.execSelect()
        return res.collect { it.get(GRAPH).toString() }
    }

    static String toTurtle(Map form, Map context = null) {
        Map thing = (Map) form.clone()
        Map record = (Map) thing.remove(RECORD_KEY) ?: [:]

        thing[ID_KEY] = THING_TMP_ID
        record[ID_KEY] = RECORD_TMP_ID

        def ttl = JsonLdToTrigSerializer.toTurtle(context, [record, thing])
                .toByteArray()
                .with { new String(it, UTF_8) }

        record.remove(ID_KEY)

        return ttl
    }

    static List<String> separatePrefixes(String ttl) {
        ttl.readLines()
                .split { it.startsWith("prefix") }
                .collect { it.join('\n') }
    }

    static String sparqlify(String ttl) {
        def substitutions = [
                "<$RECORD_TMP_ID>": "?$GRAPH",
                "<$THING_TMP_ID>" : "?mainEntity",
        ]
        return ttl.replace(substitutions)
    }
}
