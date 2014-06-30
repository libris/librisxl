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
    String ANONYMOUS_ID_PREFIX = "_:t-"
    Whelk whelk

    def anonymousIds

    def loadRelatedDocs(Document doc) {
        def relatedDocs = [:]
        for (link in doc.getLinks()) {
            log.trace("Doc has ${link.type}-link to ${link.identifier}")
            def idStr = link.identifier.replace("/resource", "")
            def linkedDoc = whelk.get(new URI(idStr))
            if (linkedDoc) { //&& link.type == "auth" ??
                //type=authority from importer, type=<relation> from linkfinders
                relatedDocs[link.identifier] = linkedDoc.dataAsMap
            } else {
                log.trace("Missing document for ${link.identifier}")
            }
        }
        return relatedDocs
    }

    Document doFilter(Document doc) {
        log.trace("Running JsonLDLinkCompleterFilter on ${doc.identifier}")
        anonymousIds = [:]
        def changedData = false
        def json, work
        def relatedDocs = loadRelatedDocs(doc)

        if (relatedDocs.size() > 0) {
            json = doc.dataAsMap
            work = json.get("about")
            work.each { key, value ->
                log.trace("trying to find and update entity $key")
                changedData = findAndUpdateEntityIds(value, relatedDocs) || changedData
            }
            if (work.get("instanceOf")) {
                work."instanceOf".each {  k, v ->
                    log.trace("trying to find and update instanceof entity $k")
                    changedData = findAndUpdateEntityIds(v, relatedDocs) || changedData
                }
            }
            log.trace("Changed data? $changedData")
            if (changedData) {
                if (!anonymousIds.isEmpty()) {
                    log.trace("second pass to try to match unmatched anonymous @id:s")
                    work.each { key, value ->
                        log.trace("trying to match anonymous @id:s for $key")
                        findAndUpdateEntityIds(value, ["fakedoc":["@type":"dummy"]])
                    }
                    if (work.get("instanceOf")) {
                        work."instanceOf".each {  k, v ->
                            log.trace("trying to match anonymous instanceof @id:s for $key")
                            findAndUpdateEntityIds(v, ["fakedoc":["@type":"dummy"]])
                        }
                    }
                }
                return doc.withData(json)
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
            } else if (!obj.containsKey("@type") && obj.get("@id", "").startsWith(ANONYMOUS_ID_PREFIX)) {
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

    boolean updatePersonId(item, relatedItem, relatedDocId) {
        log.trace("Updating person $item")
        if (item["controlledLabel"] == relatedItem["controlledLabel"]) {  //ignore case in comparison?
            item["@id"] = relatedItem["@id"]
            return true
        }
        return false
    }

    def updateSameAsAndId(item, relatedItem, relatedDocId) {
        item["sameAs"] = ["@id": item["@id"]]
        item["@id"] = relatedItem["@id"]
        log.trace("${item}")
    }

    boolean updateConceptId(item, relatedItem, relatedDocId) {
        def changed = false
        def authItemSameAs = relatedItem.get("sameAs")?.get("@id")
        if (item["@id"] && item["@id"] == authItemSameAs) {
            updateSameAsAndId(item, relatedItem)
            return true
        }
        def same = item.get("sameAs")?.get("@id") == authItemSameAs
        if (same || (item["prefLabel"] && item["prefLabel"].equalsIgnoreCase(relatedItem["prefLabel"]))) {
            item["@id"] = relatedItem["@id"]
            return true
        }
        def broader = item.get("broader")
        def narrower = item.get("narrower")
        if (broader && broader instanceof List) {
            broader.each { bConcept ->
                if (bConcept.get("prefLabel") && bConcept["prefLabel"].equalsIgnoreCase(relatedItem["prefLabel"])) {
                    updateSameAsAndId(bConcept, relatedItem)
                    changed = true
                }
            }
        }
        if (narrower && narrower instanceof List) {
            narrower.each { nConcept ->
                if (nConcept.get("prefLabel") && nConcept["prefLabel"].equalsIgnoreCase(relatedItem["prefLabel"])) {
                    updateSameAsAndId(nConcept, relatedItem)
                    changed = true
                }
            }
        }

        return changed
    }

    boolean updateWorkId(item, relatedItem, relatedDocId) {
        if (item["uniformTitle"] == relatedItem["uniformTitle"]) { //ignore case in comparison?
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
                      attributedTo["controlledLabel"] == authObject["controlledLabel"]))  //ignore case in comparison?
            ) {
                item["@id"] = relatedItem["@id"] ?: relatedDocId
                return true
            }
        }
        return false
    }


    //TODO: lookup for example NB=Kungl. biblioteket ??
    boolean updateOrganizationId(item, relatedItem, relatedDocId) {
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
