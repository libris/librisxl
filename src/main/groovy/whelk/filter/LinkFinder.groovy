package whelk.filter

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.component.PostgreSQLComponent

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.concurrent.ConcurrentHashMap

/**
 * Created by markus on 2015-12-15.
 */
@Log
class LinkFinder {

    PostgreSQLComponent postgres

    static String ENTITY_QUERY
    static final ObjectMapper mapper = new ObjectMapper()

    static Map<String,String> CACHED_LINKS = new ConcurrentHashMap<String,String>()

    LinkFinder(PostgreSQLComponent pgsql) {
        postgres = pgsql
        ENTITY_QUERY = "SELECT data->'@graph'->1->>'@id' AS uri FROM ${postgres.mainTableName} WHERE " +
                "data->'@graph' @> ?"
    }

    int numCalls = 0

    Document findLinks(Document doc) {
        long startTime = System.currentTimeMillis()
        numCalls = 0
        boolean foundLinks = false
        if (doc && doc.isJsonLd()) {
            for (item in doc.data.get("@graph")) {
                foundLinks = locateSomeEntity(item, false) || foundLinks
            }
        }
        log.trace("Cache size: ${CACHED_LINKS.size()}. Document ${doc.id} checked ${numCalls} links. Time elapsed: ${System.currentTimeMillis()-startTime}")
        if (foundLinks) {
            return doc
        }
        return null
    }

    boolean locateSomeEntity(Map node, boolean found) {
        for (item in node) {
            if (item.key == "@id" && item.value ==~ /\/some\?.+/) {
                log.trace("Checking ${item.value}")
                String foundLink = queryForLink(item.value.substring(6))
                if (foundLink) {
                    found = true
                    log.debug("Found link from string: $foundLink")
                    item.value = foundLink
                }
            }
            if (item.value instanceof Map && item.value.get("@id") ==~ /\/some\?.+/) {
                String foundLink = queryForLink(item.value.get("@id").substring(6))
                if (foundLink) {
                    found = true
                    log.debug("Found link: $foundLink")
                    item.value.put("@id", foundLink)
                }
            }
            if (item.value instanceof List) {
                for (listitem in item.value) {
                    if (listitem instanceof Map) {
                        found = locateSomeEntity(listitem, found) || found
                    }
                }
            }
        }
        return found
    }

    String queryForLink(String queryString) {
        numCalls++
        if (CACHED_LINKS.containsKey(queryString)) {
            log.trace("Using cached value ${CACHED_LINKS.get(queryString)} for $queryString")
            return CACHED_LINKS.get(queryString)
        }
        def queryMap = [:]
        queryString.split("&").each { keyval ->
            def (key, val) = keyval.split("=")
            if (key == "type") {
                key = "@type"
            }
            val = URLDecoder.decode(val, "UTF-8")
            queryMap.put(key, val)
        }
        Connection connection = postgres.getConnection()
        String uri = null
        try {
            PreparedStatement stmt = connection.prepareStatement(ENTITY_QUERY)

            log.debug(" SQL : " + ENTITY_QUERY)
            log.debug("JSON: " + mapper.writeValueAsString([queryMap]))
            //log.debug("JSON 2: " + mapper.writeValueAsString(queryMap))
            stmt.setObject(1, mapper.writeValueAsString([queryMap]), java.sql.Types.OTHER)
            //stmt.setObject(2, mapper.writeValueAsString(queryMap), java.sql.Types.OTHER)
            ResultSet rs = stmt.executeQuery()
            if (rs.next()) {
                uri = rs.getString("uri")
            }
        } finally {
            connection.close()
        }
        CACHED_LINKS.put(queryString, uri)
        return uri
    }
}
