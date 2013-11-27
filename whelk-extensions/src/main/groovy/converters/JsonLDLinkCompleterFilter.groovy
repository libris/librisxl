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
        for (link in doc.links) {
            log.debug("Doc has ${link.type} -link to ${link.identifier}")
            def linkedDoc = whelk.get(new URI(link.identifier))
            if (linkedDoc && link.type == "auth") {  //handling links to "auth" docs
                relatedDocs[link.identifier] = linkedDoc
            }
        }
        return relatedDocs
    }

    Document doFilter(Document doc) {
        log.info("Running JsonLDLinkCompleterFilter on ${doc.identifier}")
        def changedData = false
        def entityType, entity
        def relatedDocs = loadRelatedDocs(doc)

        def json = mapper.readValue(doc.dataAsString, Map)
        def work = json.about?.instanceOf ? json.about.instanceOf : json.about

        //For each property in incoming document
        work.each { propKey, propValue ->

            //Find entity
            if (propValue instanceof List) {
                propValue.each {
                    if (it instanceof Map) {
                        entity = it
                        entityType = it.get("@type")
                    }
                }
            }  else if (propValue instanceof Map) {
                entity = propValue
                entityType = propValue.get("@type")
            }

            //Try to update entity.@id with matching linked documents
            if (entityType && !entity.containsKey("@value"))  {   //&& !entity.containsKey("@id")  ?
                changedData = updatePropertyWithLinks(entity, relatedDocs)
            }

        }
        if (changedData) {
            return doc.withData(mapper.writeValueAsString(json))
        }

        return doc

    }

    boolean updatePropertyWithLinks(property, relatedDocs) {
        boolean updated = false
        def relatedDocMap, relatedItem, updateAction
        if (relatedDocs.size() > 0) {
            relatedDocs.each { docId, doc ->
                relatedDocMap = doc.dataAsMap
                relatedItem = relatedDocMap.about ?: relatedDocMap
                if (relatedItem.get("@type") == property.get("@type")) {
                    try {
                        updateAction = "update" + property["@type"] + "Id"
                        updated = "$updateAction"(property, relatedItem)
                        log.trace("$updated")
                    } catch (Exception e) {
                        log.debug("Could not update property of type ${property["@type"]}")
                        updated = false
                    }
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
