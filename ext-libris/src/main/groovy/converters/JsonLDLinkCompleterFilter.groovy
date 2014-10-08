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

    protected buildRelatedItemsIndex(Document doc) {
        def byType = [:]
        def byIdOrSameAs = [:]
        def relatedItemsIndex = [byType: byType, byIdOrSameAs: byIdOrSameAs]
        def oaipmhSetSpecs = doc.meta.get("oaipmhSetSpecs") ?: []
        for (spec in oaipmhSetSpecs) {
            log.trace("Doc has link to ${spec}")
            if (spec.startsWith("authority:")) {
                def idStr = spec.replace("authority:", "/auth/")
                def linkedDoc = whelk.get(new URI(idStr))
                if (linkedDoc) {
                    def data = linkedDoc.dataAsMap
                    def about = data.about ?: data
                    def id = about["@id"]
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
        def relatedItemsIndex = buildRelatedItemsIndex(doc)

        def json = doc.dataAsMap
        def resource = json.get("about")

        if (relatedItemsIndex.byType.size() > 0) {
            resource.each { key, value ->
                log.trace("trying to find and update entity $key")
                changedData = findAndUpdateEntityIds(value, relatedItemsIndex) || changedData
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
        if (sameItem) {
            matchedId = sameItem["@id"]
        } else {
            def objType = item["@type"]
            relatedItemsIndex.byType[objType].each { relatedItem ->
                // TODO: we don't need to check @id or sameAs here if
                // byIdOrSameAs above is enough. Only for nested items do we
                // need to do such a check...
                if (matchItems(item, relatedItem)) {
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
            return itemOrItems*.get("@id")
        if ("@id" in itemOrItems)
            return [itemOrItems["@id"]]
        else
            return []
    }

}
