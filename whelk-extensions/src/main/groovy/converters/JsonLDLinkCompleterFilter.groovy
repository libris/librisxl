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
    def whelk

    public void setWhelk(Whelk whelk) {
        this.whelk = whelk
    }

    def loadRelatedDocs(Document doc) {
        def relatedDocs = [:]
        def links = doc.getLinks()
        if (links.size() > 0) {
            for (link in links) {
                log.debug("Doc has ${link.type}-link to ${link.identifier}")
                def linkedDoc = whelk.get(new URI(link.identifier))
                if (linkedDoc) {  //&& link.type == "auth" ?? type=authority from importer, type=<relation> from linkfinders
                    relatedDocs[link.identifier] = linkedDoc
                }
            }
        }
        return relatedDocs
    }

    def findMapProperty(prop) {
        if (prop instanceof List) {
            for (p in prop) {
                if (!prop instanceof String)
                    findMapProperty(p)
            }
        }
        if (prop instanceof Map) {
            if (prop.get("@type") && !prop.containsKey("@value")) //&& !prop.containsKey("@id")  ?
                return prop
            else {
                prop.each { k, v ->
                   findMapProperty(v)
                }
            }
        }
        return null
    }

    Document doFilter(Document doc) {
        log.debug("Running JsonLDLinkCompleterFilter on ${doc.identifier}")
        def changedData = false
        def entity, json, work
        def relatedDocs = loadRelatedDocs(doc)

        if (relatedDocs.size() > 0) {
            json = mapper.readValue(doc.dataAsString, Map)
            work = json.about?.instanceOf ? json.about.instanceOf : json.about

            //For each property in incoming document, find entity-object property (is a Map and has a @type, but not a @value)
            work.each { propKey, propValue ->

                if (!propValue instanceof String) {

                    entity = findMapProperty(propValue)
                    log.trace("Entity $entity")

                    //Try to update entity.@id with matching linked documents
                    changedData = updatePropertyWithLinks(entity, relatedDocs)

                    //Find entity-object property within entity
                    //TODO: better recursive way
                    def deepMap
                    entity.each { k, v ->
                        if (!v instanceof String)
                            deepMap = findMapProperty(v)
                    }

                    if (deepMap) {
                        log.debug("$deepMap")
                        changedData = updatePropertyWithLinks(deepMap, relatedDocs) || changedData
                    }
                }
            }
            if (changedData) {
                return doc.withData(mapper.writeValueAsString(json))
            }
        }

        return doc

    }

    boolean updatePropertyWithLinks(property, relatedDocs) {
        boolean updated = false
        def relatedDocMap, relatedItem, updateAction
        relatedDocs.each { docId, doc ->
                relatedDocMap = doc.dataAsMap
                relatedItem = relatedDocMap.about ?: relatedDocMap
                if (relatedItem.get("@type") == property.get("@type")) {
                    try {
                        updateAction = "update" + property["@type"] + "Id"
                        log.trace("$updateAction")
                        updated = "$updateAction"(property, relatedItem)
                        log.trace("$updated")
                    } catch (Exception e) {
                        log.trace("Could not update property of type ${property["@type"]}")
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
            def creator = item.creator
            if (creator instanceof List)
                creator = creator[0]
            def authCreator = relatedItem.creator
            if (authCreator instanceof List)
                authCreator = authCreator[0]
            if (
                    (!creator && !authCreator) ||
                            ((creator && authCreator) &&
                                    (creator["@type"] == "Person" && authCreator["@type"] == "Person" &&
                                            creator["controlledLabel"] == authCreator["controlledLabel"]))
            ) {
                item["@id"] = relatedItem["@id"]
                return true
            }
        }
        return false
    }
}
