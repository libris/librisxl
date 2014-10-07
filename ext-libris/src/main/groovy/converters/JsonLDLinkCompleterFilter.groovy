package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.component.ElasticQuery
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Whelk

import static se.kb.libris.conch.Tools.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDLinkCompleterFilter extends BasicFilter implements WhelkAware {

    String requiredContentType = "application/ld+json"
    String ANONYMOUS_ID_PREFIX = "_:"
    Whelk whelk

    def anonymousIds

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
        anonymousIds = [:]
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
                if (!anonymousIds.isEmpty()) {
                    log.trace("second pass to try to match unmatched anonymous @id:s")
                    resource.each { key, value ->
                        log.trace("trying to match anonymous @id:s for $key")
                        findAndUpdateEntityIds(value, ["fakedoc":["@type":"dummy"]])
                    }
                }
                return doc.withData(json)
            }
        }
        log.debug("Checking for controlNumbers")

        boolean altered = false
        for (key in ["precededBy", "succeededBy"]) {
            for (item in work.get(key)) {
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
            changedData = findAndUpdateEntityIds(value, relatedDocs) || changedData
        }
        return changedData
    }

    boolean updateEntityId(obj, relatedDocs) {
        boolean updated = false
        def relatedItem, updateAction
        relatedDocs.each { docId, relatedDocMap ->
            relatedItem = relatedDocMap.about ?: relatedDocMap
            if (relatedItem.get("@type") == obj.get("@type")) {
                try {
                    updateAction = "update" + obj["@type"] + "Id"
                    def oldEntityId = obj["@id"]
                    updated = "$updateAction"(obj, relatedItem, docId)
                    if (updated && oldEntityId && oldEntityId.startsWith(ANONYMOUS_ID_PREFIX)) {
                        log.trace("saving $oldEntityId == ${obj["@id"]}")
                        anonymousIds[oldEntityId] = obj["@id"]
                    }
                } catch (Exception e) {
                    log.trace("Could not update object of type ${obj["@type"]}")
                    updated = false
                }
            } else if (!obj.containsKey("@type") && obj.get("@id")?.startsWith(ANONYMOUS_ID_PREFIX)) {
                if (anonymousIds.containsKey(obj["@id"])) {
                    obj["@id"] = anonymousIds[obj["@id"]]
                    updated = true
                    log.trace("replaced old @id with ${obj["@id"]}")
                } else {
                    log.trace("unmatched anonymous id: ${obj["@id"]}")
                }
            }
        }
        return updated
    }

    boolean updatePersonId(item, relatedItem, relatedDocId=null) {
        log.trace("Updating person $item")
        def properties = [
            "name",
            "familyName",
            "givenName",
            "birthYear",
            "deathYear",
            "notation",
            "personTitle",
        ]
        // criteria: all properties in item must be equal to those in relatedItem
        int shared = 0
        for (prop in properties) {
            def value = item[prop]
            if (value instanceof String) {
                shared++
                if (value != relatedItem[prop]) {
                    return false
                }
            }
        }
        if (shared == 0) {
            return false
        } else {
            item["@id"] = relatedItem["@id"]
            return true
        }
    }

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
        // TODO: sameAsIds intersect authItemSameAsIds
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

    List collectItemIds(itemOrItems) {
        if (itemOrItems instanceof List)
            return itemOrItems*.get("@id")
        if ("@id" in itemOrItems)
            return [itemOrItems["@id"]]
        else
            return []
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

}
