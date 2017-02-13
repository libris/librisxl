package whelk.filter


import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.component.PostgreSQLComponent

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.concurrent.ConcurrentHashMap

@Log
class AuthFinder {

    PostgreSQLComponent postgres

    static String ENTITY_QUERY
    static final ObjectMapper mapper = new ObjectMapper()

    static Map<String, String> CACHED_LINKS = new ConcurrentHashMap<String, String>()

    AuthFinder(PostgreSQLComponent pgsql) {
        postgres = pgsql
        ENTITY_QUERY = """SELECT id
                        FROM lddb
                        WHERE data -> '@graph' @> ?
                        AND id IN (SELECT id
                                     FROM lddb__identifiers
                                     WHERE identifier IN (€));"""
    }

    int numCalls = 0

    List<URI> findLinks(List<Map> entities, List<String> recordIds) {
        log.debug "Finding links for ${entities.size()} entities and ${recordIds.size()} record Ids"
        def paramsQuery = ENTITY_QUERY.replace('€', recordIds.collect { it -> '?' }.join(','))
        def connection = postgres.getConnection()
        PreparedStatement stmt = connection.prepareStatement(paramsQuery)

        def foundLinks = entities.collect { entity ->
            int i = 1
            stmt.setObject(i, mapper.writeValueAsString([entity]), java.sql.Types.OTHER)
            recordIds.each { recordId ->
                stmt.setObject(++i, recordId)
            }
            def id
            ResultSet rs = stmt.executeQuery()
            if (rs.next()) {
                id = rs.getString("id")
            }

            return id ? Document.BASE_URI.resolve(id) : null
        }
        log.debug "Found ${foundLinks.count {it->it}} links to replace in entities"
        return foundLinks
    }

    Document findLinks(Document doc) {
        long startTime = System.currentTimeMillis()
        numCalls = 0
        boolean foundLinks = false
        if (doc) {
            for (item in doc.data.get("@graph")) {
                foundLinks = locateSomeEntity(item, false) || foundLinks
            }
        }
        log.trace("Cache size: ${CACHED_LINKS.size()}. Document ${doc.id} checked ${numCalls} links. Time elapsed: ${System.currentTimeMillis() - startTime}")
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
        if (uri)
            CACHED_LINKS.put(queryString, uri)
        return uri
    }
}
