package whelk.filter


import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.component.PostgreSQLComponent
import whelk.util.LegacyIntegrationTools

import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.concurrent.ConcurrentHashMap

@Log
class LinkFinder {

    PostgreSQLComponent postgres

    static String ENTITY_QUERY
    static final ObjectMapper mapper = new ObjectMapper()
    static final ConcurrentHashMap<String, String> uriCache = new ConcurrentHashMap()

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

    void normalizeIdentifiers(Document document, boolean cacheAuthForever = false) {

        // Normalize ISBN and ISSN identifiers. No hyphens and upper case.
        List typedIDs = document.get(Document.thingTypedIDsPath)
        for (Object entry: typedIDs) {
            if (entry instanceof Map) {
                Map map = (Map) entry
                String type = map.get("@type")
                if ( type != null && type.equals("ISBN")) {
                    String value = map.get("value")
                    if (value != null)
                        map.put("value", value.replaceAll("-", "").toUpperCase())
                }
                if ( type != null && type.equals("ISSN") ) {
                    String value = map.get("value")
                    if (value != null)
                        map.put("value", value.toUpperCase())
                }
            }
        }

        replaceSameAsLinksWithPrimaries(document.data, cacheAuthForever)
    }

    private void replaceSameAsLinksWithPrimaries(Map data, boolean cacheAuthForever = false) {
        // If this is a link (an object containing _only_ an id)
        String id = data.get("@id")
        if (id != null && data.keySet().size() == 1) {
            String primaryId = lookupPrimaryId(id, cacheAuthForever)
            if (primaryId != null)
                data.put("@id", primaryId)
        }

        // Keep looking for more links
        for (Object key : data.keySet()) {

            // sameAs objects are not links per se, and must not be replaced
            String keyString = (String) key
            if (keyString.equals("sameAs"))
                return

            Object value = data.get(key)

            if (value instanceof List)
                replaceSameAsLinksWithPrimaries( (List) value)
            if (value instanceof Map)
                replaceSameAsLinksWithPrimaries( (Map) value)
        }
    }

    private void replaceSameAsLinksWithPrimaries(List data) {
        for (Object element : data){
            if (element instanceof List)
                replaceSameAsLinksWithPrimaries( (List) element)
            else if (element instanceof Map)
                replaceSameAsLinksWithPrimaries( (Map) element)
        }
    }

    private String lookupPrimaryId(String id, boolean cacheAuthForever) {

        if (!cacheAuthForever) {
            String mainId = postgres.getMainId(id)
                return mainId
        }

        if (id.startsWith("http://libris.kb.se/resource/")) {
            // re-calculate what the correct primary ID should be (better get this right)
            String pathId = id.substring("http://libris.kb.se/resource".length())
            String numericId = pathId.split("/")[2]
            boolean isNumericId = true
            for (char c : numericId.toCharArray()) {
                if (!Character.isDigit(c))
                    isNumericId = false
            }
            if (isNumericId) {
                return Document.BASE_URI.resolve(LegacyIntegrationTools.generateId(pathId)).toString() + "#it"
            } else {
                // The ID is something of the form http://libris.kb.se/resource/cwpqbclp4x4n61k
                String primaryId = Document.BASE_URI.resolve(numericId).toString()
                if (!primaryId.endsWith("#it"))
                    primaryId += "#it"
                return primaryId
            }

        } else if (id.startsWith("https://id.kb.se/")) {
            // cache the ID
            if (uriCache.containsKey(id)) {
                return uriCache.get(id)
            } else {
                String mainId = postgres.getMainId(id)
                if (mainId != null) {
                    uriCache.put(id, mainId)
                    return mainId
                }
            }
        } else { // Fallback -> Look for a primary ID in postgres (expensive)
            String mainId = postgres.getMainId(id)
                return mainId
        }

        return null
    }
}