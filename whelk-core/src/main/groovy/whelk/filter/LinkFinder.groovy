package whelk.filter

import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.Document
import whelk.JsonLd
import whelk.component.PostgreSQLComponent
import whelk.exception.LinkValidationException

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

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
            try {
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
                log.debug "Found ${foundLinks.count { it -> it }} links to replace in entities"
                return foundLinks
            }
            finally {
                connection.close()
            }
        }
        else{
            log.debug "missing arguments. No linkfinding will be performed."
            return [null]
        }
    }

    void normalizeIdentifiers(Document document) {
        Connection connection = postgres.getOuterConnection()
        try {
            normalizeIdentifiers(document, connection)    
        }
        finally {
            connection.close()
        }
    }

    void normalizeIdentifiers(Document document, Connection connection) {
        // Normalize ISBN and ISSN identifiers. No hyphens and upper case.
        for (List<String> path : [Document.thingTypedIDsPath, Document.thingIndirectTypedIDsPath]) {
            List typedIDs = document.get(path)
            for (Object entry : typedIDs) {
                if (entry instanceof Map) {
                    Map map = (Map) entry
                    String type = map.get("@type")
                    if (type != null && type.equals("ISBN")) {
                        String value = map.get("value")
                        if (value != null)
                            map.put("value", value.replaceAll("-", "").toUpperCase())
                    }
                    if (type != null && type.equals("ISSN")) {
                        String value = map.get("value")
                        if (value != null)
                            map.put("value", value.toUpperCase())
                    }
                }
            }
        }

        clearReferenceAmbiguities(document)
        replaceSameAsLinksWithPrimaries(document.data, connection)
        // TODO: check what happens in the sameas table when id:s are changed and changed back!
        restoreNewCanonicalMainEntityUri(document.data)
    }

    /**
     * When {@link #replaceSameAsLinksWithPrimaries} is used, the mainEntity
     * reference is replaced by any already existing primary id. This collides
     * with purposefully changing the primary id, which is therefore restored
     * here.
     */
    private void restoreNewCanonicalMainEntityUri(Map data) {
        List items = data['@graph']
        if (items.size() < 2) {
            return
        }
        items[0][JsonLd.THING_KEY]['@id'] = items[1]['@id']
    }

    private void replaceSameAsLinksWithPrimaries(Map data, Connection connection) {
        // If this is a link (an object containing _only_ an id)
        String id = data.get("@id")
        if (id != null && data.keySet().size() == 1) {
            String primaryId = lookupPrimaryId(id, connection)
            if (primaryId != null)
                data.put("@id", primaryId)
        }

        // Keep looking for more links
        for (Object key : data.keySet()) {

            // sameAs objects are not links per se, and must not be replaced
            String keyString = (String) key
            if (keyString.equals("sameAs"))
                continue

            Object value = data.get(key)

            if (value instanceof List)
                replaceSameAsLinksWithPrimaries( (List) value, connection )
            if (value instanceof Map)
                replaceSameAsLinksWithPrimaries( (Map) value, connection )
        }
    }

    private void replaceSameAsLinksWithPrimaries(List data, Connection connection) {
        for (Object element : data){
            if (element instanceof List)
                replaceSameAsLinksWithPrimaries( (List) element, connection )
            else if (element instanceof Map)
                replaceSameAsLinksWithPrimaries( (Map) element, connection )
        }
    }

    private String lookupPrimaryId(String id, Connection connection) {
        String mainIri = postgres.getMainId(id, connection)

        if (mainIri == null)
            return null

        if (postgres.iriIsLinkable(mainIri, connection))
            return mainIri
        throw new LinkValidationException("Not allowed to link to the deleted resource: " + mainIri)
    }

    /**
     * A heavy-handed last line of defense against confusing embedded entities with references.
     * After running this, 'document' can no longer have both @id and other data in any same
     * embedded entity (root entities are exempt).
     */
    private void clearReferenceAmbiguities(Document document) {
        List graphList = document.data.get(JsonLd.GRAPH_KEY)
        for (Object entry : graphList) {
            clearReferenceAmbiguities_internal(entry, true)
        }
    }

    private void clearReferenceAmbiguities_internal(Map data, boolean isRootEntry) {
        if (!isRootEntry) {
            Object id = data.get("@id")
            // If we have both @id and data (which is bad)
            if (id != null && data.size() > 1) {

                if (postgres.getSystemIdByIri(id) != null) { // If we have such a record, then the link (@id) is enough.
                    data.clear()
                    data.put("@id", id)
                } else if (id.startsWith("https://libris.kb.se/library/")) {
                    // A FUGLY special case/hack for library URIs, which we want as URIs alone, despite them not being XL-URIs.
                    data.clear()
                    data.put("@id", id)
                } else
                { // Otherwise convert @id into sameAs entry
                    Object sameAsList = data["sameAs"]
                    if (sameAsList == null) {
                        data.put("sameAs", [])
                        sameAsList = data["sameAs"]
                    } else if (!sameAsList instanceof List) { // Paranoia check
                        data.put("sameAs", [sameAsList])
                        sameAsList = data["sameAs"]
                    }
                    data.remove("@id")
                    ((List) sameAsList).add(["@id": id])
                }
            }
        }

        // Keep looking
        for (Object key : data.keySet()) {
            Object value = data.get(key)

            if (value instanceof List)
                clearReferenceAmbiguities_internal( (List) value )
            if (value instanceof Map)
                clearReferenceAmbiguities_internal( (Map) value, false )
        }
    }

    private void clearReferenceAmbiguities_internal(List data) {
        for (Object element : data){
            if (element instanceof List)
                clearReferenceAmbiguities_internal( (List) element )
            else if (element instanceof Map)
                clearReferenceAmbiguities_internal( (Map) element, false )
        }
    }
}
