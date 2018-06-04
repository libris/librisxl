package whelk.converter

import groovy.util.logging.Log4j2 as Log

import whelk.Document

//import static whelk.util.Tools.*

import org.codehaus.jackson.map.ObjectMapper

@Log
@Deprecated
class JsonLDLinkCompleterFilter {

    static String BNODE_ID_PREFIX = "_:"

    ObjectMapper mapper = new ObjectMapper()

    String requiredContentType = "application/ld+json"

    Map entityShapes
    Map bnodeIdMap

    public JsonLDLinkCompleterFilter() {
        def entityShapesCfgPath = "ext/entityshapes.json"
        getClass().classLoader.getResourceAsStream(entityShapesCfgPath).withStream {
            this.entityShapes = mapper.readValue(it, Map)
        }
    }

    public JsonLDLinkCompleterFilter(Map entityShapes) {
        this.entityShapes = entityShapes
    }

    protected buildRelatedItemsIndex(Document doc) {
        def byType = [:]
        def byIdOrSameAs = [:]
        def relatedItemsIndex = [byType: byType, byIdOrSameAs: byIdOrSameAs]
        def oaipmhSetSpecs = doc.manifest?.extraData.get("oaipmhSetSpecs") ?: []
        for (spec in oaipmhSetSpecs) {
            log.trace("Doc has link to ${spec}")
            if (spec.startsWith("authority:")) {
                def idStr = spec.replace("authority:", "/auth/")
                def linkedDoc = whelk.get(idStr, null, [], false)
                if (linkedDoc) {
                    def data = linkedDoc.data
                    def about = data.about ?: data
                    def id = about["@id"]
                    log.trace("found item @id: $id")
                    if (id) {
                        byIdOrSameAs[id] = about
                    }
                    def sameAsIds = collectItemIds(about["sameAs"])
                    sameAsIds.each {
                        byIdOrSameAs[it] = about
                    }
                    collectTypes(about).each {
                        byType.get(it, []) << about
                    }
                } else {
                    log.trace("Missing document for ${idStr}")
                }
            }
        }
        return relatedItemsIndex
    }

    boolean valid(Document doc) {
        if (doc && doc.contentType == "application/ld+json") {
            return true
        }
        return false
    }

    Document filter(Document doc) {
        log.trace("Running JsonLDLinkCompleterFilter on ${doc.identifier}")
        bnodeIdMap = [:]
        def changedData = false
        def relatedItemsIndex = buildRelatedItemsIndex(doc)

        def json = doc.data
        def resource = json.get("about")

        if (relatedItemsIndex.byType.size() > 0) {
            resource.each { key, value ->
                log.trace("trying to find and update entity $key")
                changedData = findAndUpdateEntityIds(value, relatedItemsIndex) || changedData
            }
            // TODO: optimize or remove need for bnode rewriting
            log.trace("Changed data? $changedData")
            if (changedData) {
                if (!bnodeIdMap.isEmpty()) {
                    log.trace("second pass to try to match unmatched bnode @id:s")
                    resource.each { key, value ->
                        log.trace("trying to match bnode @id:s for $key")
                        findAndUpdateEntityIds(value, [byType: [:], byIdOrSameAs: [:]])
                    }
                }
                return doc.withData(json)
            }
        }

        return doc
    }

    Map doFilter(Map docmap, String dataset) { throw new UnsupportedOperationException("Not implemented.") }

    /**
     * For given object and each nested object within, call updateEntityId.
     * (An object is a List or a Map (with a @type, but not a @value).)
     */
    boolean findAndUpdateEntityIds(item, relatedItemsIndex) {
        if (item instanceof List) {
            def changedData = false
            for (o in item) {
                changedData = findAndUpdateEntityIds(o, relatedItemsIndex) || changedData
            }
            return changedData
        }
        // skip native literals
        if (!(item instanceof Map)) {
            return false
        }
        // skip expanded literals
        if (item.get("@type") && item.containsKey("@value")) {
            return false
        }
        boolean changedData = updateEntityId(item, relatedItemsIndex)
        if (!changedData) {
            item.each { key, value ->
                changedData = findAndUpdateEntityIds(value, relatedItemsIndex) || changedData
            }
        }
        return changedData
    }

    boolean updateEntityId(item, relatedItemsIndex) {
        def currentItemId = item["@id"]
        def matchedId = null
        def sameItem = relatedItemsIndex.byIdOrSameAs[currentItemId]
        if (!sameItem) {
            for (id in collectItemIds(item["sameAs"])) {
                sameItem = relatedItemsIndex.byIdOrSameAs[id]
                if (sameItem)
                    break
            }
        }
        if (sameItem) {
            matchedId = sameItem["@id"]
        } else {
            def objType = item["@type"]
            relatedItemsIndex.byType[objType].each { relatedItem ->
                if (matchByShape(item, relatedItem)) {
                    matchedId = relatedItem["@id"]
                }
            }
        }
        if (matchedId) {
            item["@id"] = matchedId
            if (currentItemId) {
                if (currentItemId.startsWith(BNODE_ID_PREFIX)) {
                    bnodeIdMap[currentItemId] = item["@id"]
                } else if (currentItemId != item["@id"]) {
                    item["sameAs"] = ["@id": currentItemId]
                }
            }
            return true
        } else if (!item.containsKey("@type") && item.get("@id")?.startsWith(BNODE_ID_PREFIX)) {
            if (bnodeIdMap.containsKey(item["@id"])) {
                item["@id"] = bnodeIdMap[item["@id"]]
                return true
            }
        }
        return false
    }

    boolean matchItems(item, relatedItem) {
        def itemId = item["@id"]
        if (itemId && itemId == relatedItem["@id"]) {
            return true
        }
        def relatedSameAsIds = collectItemIds(relatedItem["sameAs"])
        if (itemId && itemId in relatedSameAsIds) {
            return true
        }
        if (collectItemIds(item["sameAs"]).intersect(relatedSameAsIds).size() > 0) {
            return true
        }
        return matchByShape(item, relatedItem)
    }

    boolean matchByShape(item, relatedItem) {
        def objType = item["@type"]
        if (relatedItem.get("@type") != objType)
            return false
        def properties = entityShapes[objType]
        if (!properties)
            return false
        // criteria: all properties in item must be equal to those in relatedItem
        int shared = 0
        for (prop in properties.keySet()) {
            def value = item[prop]
            def relatedValue = relatedItem[prop]
            if (value instanceof String) {
                shared++
                if (value != relatedValue) {
                    return false
                }
            } else if (value instanceof Map && relatedValue instanceof Map) {
                if (matchItems(value, relatedValue)) {
                    value["@id"] = relatedValue["@id"]
                    return true
                } else {
                    return false
                }

            }
        }
        return shared > 0
    }

    List collectTypes(item) {
        def type = item["@type"]
        if (type instanceof List)
            return type
        else if (type instanceof String)
            return [type]
        else if (type instanceof Map && "@id" in itemOrItems)
            return [type["@id"]]
        else
            return []
    }

    List collectItemIds(itemOrItems) {
        if (itemOrItems instanceof List)
            return itemOrItems*.get("@id").findAll()
        if ("@id" in itemOrItems)
            return [itemOrItems["@id"]]
        else
            return []
    }

}
