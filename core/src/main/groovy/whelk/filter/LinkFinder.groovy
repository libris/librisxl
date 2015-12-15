package whelk.filter

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.component.PostgreSQLComponent

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

/**
 * Created by markus on 2015-12-15.
 */
@Log
class LinkFinder {

    PostgreSQLComponent postgres

    static String ENTITY_QUERY
    static String ENTRY_PATH = "data->'descriptions'->'entry'->>"
    static final ObjectMapper mapper = new ObjectMapper()

    LinkFinder(PostgreSQLComponent pgsql) {
        postgres = pgsql
        ENTITY_QUERY = "SELECT data->'descriptions'->'items'->0->>'@id' AS uri FROM ${postgres.mainTableName} WHERE " +
                "data->'descriptions'->'items' @> ?"

    }


    Document findLinks(Document doc) {
        boolean found = false
        if (doc && doc.isJsonLd()) {
            // Check entry
            locateSomeEntity(doc.data.get("descriptions").get("entry"))
            for (item in doc.data.get("descriptions").get("items")) {
                found = (locateSomeEntity(item) || found)
            }
        }
        if (found) {
            log.info("New and updated record: ${doc.data}")
        }
        return doc
    }

    boolean locateSomeEntity(Map node) {
        boolean found = false
        for (item in node) {
            if (item.value instanceof Map && item.value.get("@id") ==~ /\/some\?.+/) {
                String foundLink = queryForLink(item.value.get("@id").substring(6))
                if (foundLink) {
                    log.info("Found link: $foundLink")
                    item.value = foundLink
                    found = true
                }
            }
        }
        return found
    }

    String queryForLink(String queryString) {
        def parameterList = []
        StringBuilder queryAppendage = new StringBuilder("(")
        def queryMap = [:]
        boolean first = true
        queryString.split("&").each { keyval ->
            def (key, val) = keyval.split("=")
            if (key == "type") {
                key = "@type"
            }
            if (!first) {
                queryAppendage.append(" AND ")
            }
            queryAppendage.append(ENTRY_PATH+"'$key' = ?")
            parameterList.add(val)
            queryMap.put(key, val)
            first = false
        }
        queryAppendage.append(")")
        Connection connection = postgres.getConnection()
        try {
            log.debug("SQL : " + ENTITY_QUERY + " OR " + queryAppendage.toString())
            log.debug("JSON : " + mapper.writeValueAsString([queryMap]))
            PreparedStatement stmt = connection.prepareStatement(ENTITY_QUERY + " OR " + queryAppendage.toString())
            stmt.setObject(1, mapper.writeValueAsString([queryMap]), java.sql.Types.OTHER)
            int parameterIndex = 2
            for (value in parameterList) {
                stmt.setString(parameterIndex++, value)
            }
            ResultSet rs = stmt.executeQuery()
            if (rs.next()) {
                return rs.getString("uri")
            }
        } finally {
            connection.close()
        }
        return null
    }
}
