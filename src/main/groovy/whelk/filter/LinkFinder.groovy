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

    LinkFinder(PostgreSQLComponent pgsql) {
        postgres = pgsql
        ENTITY_QUERY = """SELECT ids2.iri AS thingUri
                        FROM lddb
                        JOIN lddb__identifiers ids1 ON lddb.data#>>'{@graph,1,@id}' = ids1.iri
                        JOIN lddb__identifiers ids2 ON ids1.id = ids2.id
                        WHERE data -> '@graph' @> ?
                        AND ids2.mainid=true
                        AND lddb.id IN (SELECT id
                                     FROM lddb__identifiers
                                     WHERE iri IN (€))
                        ORDER BY ids2.graphindex;"""
    }

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
                    id = rs.getString("thingUri") // set id to record id, as fallback
                    if (rs.next()) {
                        id = rs.getString("thingUri") // if there's a second row (mainEntity), use that id instead.
                    }
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
