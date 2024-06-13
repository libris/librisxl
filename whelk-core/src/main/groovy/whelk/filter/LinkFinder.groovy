package whelk.filter

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.component.PostgreSQLComponent
import whelk.exception.LinkValidationException
import whelk.util.LegacyIntegrationTools

import java.sql.PreparedStatement
import java.sql.ResultSet

import static whelk.util.Jackson.mapper

@Log
class LinkFinder {

    PostgreSQLComponent postgres

    static String ENTITY_QUERY

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
        return postgres.withDbConnection { notIt -> // Otherwise compliler error with: The current scope already contains a variable of the name it
            if(recordIds.any() && entities.any()) {
                log.debug "Finding links for ${entities.size()} entities and ${recordIds.size()} record Ids"

                def paramsQuery = ENTITY_QUERY.replace('€', recordIds.collect { it -> '?' }.join(','))
                def connection = postgres.getMyConnection()

                PreparedStatement stmt = connection.prepareStatement(paramsQuery)

                def foundLinks = entities.collect { entity ->
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
            else{
                log.debug "missing arguments. No linkfinding will be performed."
                return [null]
            }
        }
    }

    void normalizeIdentifiers(Document document) {
        // Normalize ISBN and ISSN identifiers. No hyphens and upper case.
        for (List<String> path : [Document.thingTypedIDsPath, Document.thingIndirectTypedIDsPath]) {
            List typedIDs = document.get(path)
            for (Object entry : typedIDs) {
                if (entry instanceof Map) {
                    Map map = (Map) entry
                    String type = map.get("@type")
                    if (type != null && type == "ISBN") {
                        String value = map.get("value")
                        if (value != null)
                            map.put("value", value.replaceAll("-", "").toUpperCase())
                    }
                    if (type != null && type == "ISSN") {
                        String value = map.get("value")
                        if (value != null)
                            map.put("value", value.toUpperCase())
                    }
                }
            }
        }

        clearReferenceAmbiguities(document)
        replaceSameAsLinksWithPrimaries(document.data)
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

    private void replaceSameAsLinksWithPrimaries(Map data, List path = []) {
        // If this is a link (an object containing _only_ an id)

        String id = data.get("@id")
        if (id != null && data.keySet().size() == 1) {
            // Path to same form as in lddb__dependencies.relation
            String normalizedPath = (path.take(2) == Document.recordPath
                    ? [JsonLd.RECORD_KEY] + path.drop(2)
                    : (path.take(2) == Document.thingPath ? path.drop(2) : path)
            )
                    .findAll { it instanceof String }
                    .join('.')
            String primaryId = lookupPrimaryId(id, normalizedPath)
            if (primaryId != null)
                data.put("@id", primaryId)
        }

        // Keep looking for more links
        for (Object key : data.keySet()) {

            // sameAs objects are not links per se, and must not be replaced.
            // itemUsed is used to target specific components of holding records. If we let those
            // be upgraded, links to those specific components would be lost (replaced with links
            // to the record as a whole). So we don't upgrade them. This is a cheat/hack. :(
            String keyString = (String) key
            if (keyString == "sameAs" || keyString == "itemUsed")
                continue

            Object value = data.get(key)

            if (value instanceof List)
                replaceSameAsLinksWithPrimaries( (List) value, path + keyString )
            if (value instanceof Map)
                replaceSameAsLinksWithPrimaries( (Map) value, path + keyString )
        }
    }

    private void replaceSameAsLinksWithPrimaries(List data, List path) {
        int idx = 0
        for (Object element : data){
            if (element instanceof List)
                replaceSameAsLinksWithPrimaries( (List) element, path + idx )
            else if (element instanceof Map)
                replaceSameAsLinksWithPrimaries( (Map) element, path + idx )
            idx += 1
        }
    }

    private String lookupPrimaryId(String id, String path) {
        String mainIri = postgres.getMainId(id)

        if (mainIri == null)
            return null

        if (postgres.iriIsLinkable(mainIri, path))
            return mainIri

        throw new LinkValidationException("Forbidden link to deleted resource $mainIri found at $path")
    }

    /**
     * A heavy-handed last line of defense against confusing embedded entities with references.
     * After running this, 'document' can no longer have both @id and other data in any same
     * embedded entity (root entities and root entites under hasComponent are exempt).
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
                } else if (id.startsWith(LegacyIntegrationTools.BASE_LIBRARY_URI)) {
                    // FIXME: de-KBV/Libris-ify
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

            // Components must themselves be considered "root" entities, as they must be allowed both @id and data.
            boolean viewNextAsRoot = key.equals("hasComponent")

            if (value instanceof List)
                clearReferenceAmbiguities_internal( (List) value, viewNextAsRoot )
            if (value instanceof Map)
                clearReferenceAmbiguities_internal( (Map) value, viewNextAsRoot )
        }
    }

    private void clearReferenceAmbiguities_internal(List data, boolean isRootEntry) {
        for (Object element : data){
            if (element instanceof List)
                clearReferenceAmbiguities_internal( (List) element, false )
            else if (element instanceof Map)
                clearReferenceAmbiguities_internal( (Map) element, isRootEntry )
        }
    }
}
