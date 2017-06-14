package whelk.filter


import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.component.PostgreSQLComponent

import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.concurrent.ConcurrentHashMap

@Log
class LinkFinder {

    PostgreSQLComponent postgres

    static String ENTITY_QUERY
    static final ObjectMapper mapper = new ObjectMapper()

    static Map<String, String> CACHED_LINKS = new ConcurrentHashMap<String, String>()

    LinkFinder(PostgreSQLComponent pgsql) {
        postgres = pgsql
        ENTITY_QUERY = """SELECT data -> '@graph' -> 1 ->> '@id' as thingUri
                        FROM lddb
                        WHERE data -> '@graph' @> ?
                        AND id IN (SELECT id
                                     FROM lddb__identifiers
                                     WHERE iri IN (€));"""
    }

    int numCalls = 0

    List<URI> findLinks(List<Map> entities, List<String> recordIds) {
        if(recordIds.any() && entities.any()) {
            log.debug "Finding links for ${entities.size()} entities and ${recordIds.size()} record Ids"

            def paramsQuery = ENTITY_QUERY.replace('€', recordIds.collect { it -> '?' }.join(','))
            def connection = postgres.getConnection()
            PreparedStatement stmt = connection.prepareStatement(paramsQuery)

            def foundLinks = entities.collect { entity ->
                //log.trace "Trying to match entity ${entity.inspect()}"

                int i = 1
                stmt.setObject(i, mapper.writeValueAsString([entity]), java.sql.Types.OTHER)
                recordIds.each { recordId ->
                    stmt.setObject(++i, recordId)
                }
                def id
                log.trace stmt.toString()
                ResultSet rs = stmt.executeQuery()
                if (rs.next()) {
                    id = rs.getString("thingUri")
                }

                return id ? Document.BASE_URI.resolve(id) : null
            }
            log.debug "Found ${foundLinks.count {it->it}} links to replace in entities"
            connection.close()
            return foundLinks
        }
        else{
            log.debug "missing arguments. No linkfinding will be performed."
            return [null]
        }
    }






}
