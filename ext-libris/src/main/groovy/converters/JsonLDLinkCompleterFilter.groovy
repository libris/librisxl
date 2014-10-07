package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.component.ElasticQuery
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Whelk

import static se.kb.libris.conch.Tools.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDLinkCompleterFilter extends BasicFilter implements WhelkAware {

    static String BNODE_ID_PREFIX = "_:"

    ObjectMapper mapper = new ObjectMapper()

    String requiredContentType = "application/ld+json"
    Whelk whelk
    Map entityShapes
    Map bnodeIdMap

    public JsonLDLinkCompleterFilter() {
        def entityShapesCfgPath = "entityshapes.json"
        getClass().classLoader.getResourceAsStream(entityShapesCfgPath).withStream {
            this.entityShapes = mapper.readValue(it, Map)
        }
    }

    public JsonLDLinkCompleterFilter(Map entityShapes) {
        this.entityShapes = entityShapes
    }

    protected loadRelatedDocs(Document doc) {
        def relatedDocs = [:]
        for (spec in doc.meta.get("oaipmhSetSpecs", [])) {
            log.trace("Doc has link to ${spec}")
            if (spec.startsWith("authority:")) {
                def idStr = spec.replace("authority:", "/auth/")
                def linkedDoc = whelk.get(new URI(idStr))
                if (linkedDoc) {
                    relatedDocs[idStr] = linkedDoc.dataAsMap
                } else {
                    log.trace("Missing document for ${idStr}")
                }
            }
        }
        return relatedDocs
    }

    @Override
    boolean valid(Document doc) {
        if (doc && doc.isJson() && doc.contentType == "application/ld+json") {
            return true
        }
        return false
    }

    Document doFilter(Document doc) {
        log.trace("Running JsonLDLinkCompleterFilter on ${doc.identifier}")
        bnodeIdMap = [:]
        def changedData = false
        def relatedDocs = loadRelatedDocs(doc)

        def json = doc.dataAsMap
        def resource = json.get("about")

        if (relatedDocs.size() > 0) {
            resource.each { key, value ->
                log.trace("trying to find and update entity $key")
                changedData = findAndUpdateEntityIds(value, relatedDocs) || changedData
            }
            log.trace("Changed data? $changedData")
            if (changedData) {
                if (!bnodeIdMap.isEmpty()) {
                    log.trace("second pass to try to match unmatched bnode @id:s")
                    resource.each { key, value ->
                        log.trace("trying to match bnode @id:s for $key")
                        findAndUpdateEntityIds(value, ["fakedoc":["@type":"dummy"]])
                    }
                }
                return doc.withData(json)
            }
        }

        log.debug("Checking for controlNumbers")
        boolean altered = false
        for (key in ["precededBy", "succeededBy"]) {
            for (item in resource.get(key)) {
                def describedBy = item.get("describedBy")
                if (describedBy) {
                    for (cn in describedBy) {
                        if (cn.get("@type") == "Record") {
                            item.put("@id", new String("/resource/bib/${cn.controlNumber}"))
                        }
                    }
                    item.remove("describedBy")
                    altered = true
                }
            }
        }

        if (altered) {
            return doc.withData(json)
        }

        return doc
    }

    /**
     * For given object and each nested object within, call updateEntityId.
     * (An object is a List or a Map (with a @type, but not a @value).)
     */
    boolean findAndUpdateEntityIds(obj, relatedDocs) {
        if (obj instanceof List) {
            def changedData = false
            for (o in obj) {
                changedData = findAndUpdateEntityIds(o, relatedDocs) || changedData
            }
            return changedData
        }
        // skip native literals
        if (!(obj instanceof Map)) {
            return false
        }
        // skip expanded literals
        if (obj.get("@type") && obj.containsKey("@value")) {
            return false
        }
        // TODO: if (!prop.containsKey("@id")) return // or if @id is a "known unknown"
        def changedData = updateEntityId(obj, relatedDocs)
        obj.each { key, value ->
            if (key == "sameAs")
                return
            changedData = findAndUpdateEntityIds(value, relatedDocs) || changedData
        }
        return changedData
    }

    // TODO: 1
    // - if property value instanceof Map, require match and update of that as well
    // - change relatedDocs to relatedDocsIndex [byType, byIdOrSameAs]
    // - check byIdOrSameAs before looping byType
    // TODO: 2
    // - also match on embedded (anon) sameAs of relatedItem (for e.g. persons)
    // - also match across properties (prefLabel/label, aliases, nicknames or similar?)

    boolean updateEntityId(obj, relatedDocs) {
        boolean updated = false
        def objType = obj.get("@type")
        def currentObjId = obj["@id"]
        relatedDocs.each { docId, relatedDoc ->
            boolean match = false
            def relatedItem = relatedDoc.about ?: relatedDoc
            def relatedSameAsIds = collectItemIds(relatedItem["sameAs"])
            if (currentObjId && currentObjId in relatedSameAsIds) {
                match = true
            } else if (collectItemIds(obj["sameAs"]).intersect(relatedSameAsIds).size() > 0) {
                match = true
            } else if (relatedItem.get("@type") == objType) {
                def properties = entityShapes[objType]
                if (properties) {
                    match = matchEntities(properties, obj, relatedItem)
                }
            }
            if (match) {
                obj["@id"] = relatedItem["@id"]
                updated = true
            }
        }
        if (updated && currentObjId) {
            if (currentObjId.startsWith(BNODE_ID_PREFIX)) {
                bnodeIdMap[currentObjId] = obj["@id"]
            } else if (currentObjId != obj["@id"]) {
                obj["sameAs"] = ["@id": currentObjId]
            }
        } else if (!obj.containsKey("@type") && obj.get("@id")?.startsWith(BNODE_ID_PREFIX)) {
            if (bnodeIdMap.containsKey(obj["@id"])) {
                obj["@id"] = bnodeIdMap[obj["@id"]]
                updated = true
            }
        }
        return updated
    }

    boolean matchEntities(properties, item, relatedItem) {
        log.trace("Updating entity $item")
        // criteria: all properties in item must be equal to those in relatedItem
        int shared = 0
        for (prop in properties.keySet()) {
            def value = item[prop]
            if (value instanceof String) {
                shared++
                if (value != relatedItem[prop]) {
                    return false
                }
            }
        }
        return shared > 0
    }

    List collectItemIds(itemOrItems) {
        if (itemOrItems instanceof List)
            return itemOrItems*.get("@id")
        if ("@id" in itemOrItems)
            return [itemOrItems["@id"]]
        else
            return []
    }

    /*
    def updateSameAsAndId(item, relatedItem) {
        item["sameAs"] = ["@id": item["@id"]]
        item["@id"] = relatedItem["@id"]
    }

    boolean updateConceptId(item, relatedItem, relatedDocId=null) {
        boolean changed = false
        def authItemSameAsIds = collectItemIds(relatedItem.get("sameAs"))
        if (item["@id"] && item["@id"] in authItemSameAsIds) {
            updateSameAsAndId(item, relatedItem)
            return true
        }
        def sameAsIds = collectItemIds(item.get("sameAs"))
        boolean same = sameAsIds.intersect(authItemSameAsIds).size() > 0
        if (same || (item["prefLabel"] && item["prefLabel"].equalsIgnoreCase(relatedItem["prefLabel"]))) {
            item["@id"] = relatedItem["@id"]
            return true
        }
        def broader = item.get("broader")
        if (broader && broader instanceof List) {
            broader.each {
                if (it.get("prefLabel") && it["prefLabel"].equalsIgnoreCase(relatedItem["prefLabel"])) {
                    updateSameAsAndId(it, relatedItem)
                    changed = true
                }
            }
        }
        def narrower = item.get("narrower")
        if (narrower && narrower instanceof List) {
            narrower.each {
                if (it.get("prefLabel") && it["prefLabel"].equalsIgnoreCase(relatedItem["prefLabel"])) {
                    updateSameAsAndId(it, relatedItem)
                    changed = true
                }
            }
        }

        return changed
    }

    boolean updateConceptualWorkId(item, relatedItem, relatedDocId) {
        if (item["uniformTitle"] == relatedItem["uniformTitle"]) {
            def attributedTo = item.attributedTo
            if (attributedTo instanceof List)
                attributedTo = attributedTo[0]
            def authObject = relatedItem.attributedTo
            if (authObject instanceof List)
                authObject = authObject[0]
            if ((!attributedTo && !authObject) ||
                    ((attributedTo && authObject) &&
                     updatePersonId(attributedTo, authObject))) {
                item["@id"] = relatedItem["@id"] ?: relatedDocId
                return true
            }
        }
        return false
    }


    //TODO: lookup for example NB=Kungl. biblioteket ??
    boolean updateOrganizationId(item, relatedItem, relatedDocId=null) {
        if (item["name"] == relatedItem["name"]) {
            item["@id"] = relatedItem["@id"]
            return true
        }
        if (item["label"] == relatedItem["label"]) {
            item["@id"] = relatedItem["@id"]
            return true
        }
        return false
    }
    */

}
