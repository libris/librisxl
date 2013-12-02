package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.component.ElasticQuery
import se.kb.libris.whelks.basic.BasicFormatConverter
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Whelk

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDLinkCompleterFilter extends BasicFilter implements WhelkAware {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    Whelk whelk

    def loadRelatedDocs(Document doc) {
        def relatedDocs = [:]
        for (link in doc.getLinks()) {
            log.trace("Doc has ${link.type}-link to ${link.identifier}")
            def linkedDoc = whelk.get(new URI(link.identifier))
            if (linkedDoc) { //&& link.type == "auth" ??
                //type=authority from importer, type=<relation> from linkfinders
                relatedDocs[link.identifier] = linkedDoc
            } else {
                log.trace("Missing document for ${link.identifier}")
            }
        }
        return relatedDocs
    }

    Document doFilter(Document doc) {
        log.trace("Running JsonLDLinkCompleterFilter on ${doc.identifier}")
        def changedData = false
        def entity, json, work, deepMap
        def relatedDocs = loadRelatedDocs(doc)

        if (relatedDocs.size() > 0) {
            json = mapper.readValue(doc.dataAsString, Map)
            work = json.about?.instanceOf ?: json.about

            work.each { key, value ->
                changedData = findAndUpdateEntityIds(value, relatedDocs) || changedData
            }
            log.trace("Changed data? $changedData")
            if (changedData) {
                return doc.withData(mapper.writeValueAsString(json))
            }
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
        def relatedDocMap, relatedItem, updateAction
        relatedDocs.each { docId, doc ->
            relatedDocMap = doc.dataAsMap
            relatedItem = relatedDocMap.about ?: relatedDocMap
            if (relatedItem.get("@type") == obj.get("@type")) {
                try {
                    updateAction = "update" + obj["@type"] + "Id"
                    log.trace("$updateAction")
                    updated = "$updateAction"(obj, relatedItem)
                    log.trace("$updated")
                } catch (Exception e) {
                    log.trace("Could not update object of type ${obj["@type"]}")
                    updated = false
                }
            }
        }
        return updated
    }

    boolean updatePersonId(item, relatedItem) {
        if (item["controlledLabel"] == relatedItem["controlledLabel"]) {
            item["@id"] = relatedItem["@id"]
            return true
        }
        return false
    }

    boolean updateConceptId(item, relatedItem) {
        def authItemSameAs = relatedItem.get("sameAs")?.get("@id")
        if (item["@id"] && item["@id"] == authItemSameAs) {
            item["sameAs"] = ["@id": item["@id"]]
            item["@id"] = relatedItem["@id"]
            return true
        }
        def same = item.get("sameAs")?.get("@id") == authItemSameAs
        if (same || (item["prefLabel"] && item["prefLabel"] == relatedItem["prefLabel"])) {
            item["@id"] = relatedItem["@id"]
            return true
        }
        return false
    }

    boolean updateWorkId(item, relatedItem) {
        if (item["uniformTitle"] == relatedItem["uniformTitle"]) {
            def attributedTo = item.attributedTo
            if (attributedTo instanceof List)
                attributedTo = attributedTo[0]
            def authObject = relatedItem.attributedTo
            if (authObject instanceof List)
                authObject = authObject[0]
            if (
                    (!attributedTo && !authObject) ||
                    ((attributedTo && authObject) &&
                     (attributedTo["@type"] == "Person" && authObject["@type"] == "Person" &&
                      attributedTo["controlledLabel"] == authObject["controlledLabel"]))
            ) {
                item["@id"] = relatedItem["@id"]
                return true
            }
        }
        return false
    }

}
