package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.ElasticQuery

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDLinkEnhancerFormatConverter extends BasicFormatConverter implements WhelkAware {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def whelk

    public void setWhelk(Whelk whelk) {
        this.whelk = whelk
    }

    def searchLabel = ["Person" : "controlledLabel", "Concept" : "prefLabel"]

    Document doConvert(Document doc) {
        def changedData = false
        def entityType, entity, labelKey, searchStr, esQuery, result

        def json = mapper.readValue(doc.dataAsString, Map)
        def work = json.about?.instanceOf ? json.about.instanceOf : json.about

        //For each property in incoming document
        work.each { propKey, propValue ->

            //Find entity
            if (propValue instanceof List) {
                propValue.each {
                    if (entity instanceof Map) {
                        entity = it
                        entityType = it.get("@type")
                    }
                }
            }  else if (propValue instanceof Map) {
                entity = propValue
                entityType = propValue.get("@type")
            }

            log.info("$entityType")
            log.info("$entity")

            //Try to update entity.@id with matching linked documents
            if (entityType && !entity.containsKey("@value"))  {

                changedData = updatePropertyWithLinks(entity, doc.links)

                //Or using search
                if (!changedData) {

                    labelKey = searchLabel.get(entityType)
                    searchStr = "$labelKey:${entity.get(labelKey)}"
                    esQuery = new ElasticQuery(searchStr)
                    esQuery.indexType = entityType

                    log.info("Performing search on: $searchStr ...") //or search auth with @type?
                    result = whelk.search(esQuery)

                    log.info("Number of hits: ${result.numberOfHits}")

                    if (result.numberOfHits == 1) {
                        entity["@id"] = "/resource" + result.hits[0]["identifier"]
                    }  //TODO: else if more than 1 hit?

                }

            }

        }

        if (changedData) {
            return doc.withData(mapper.writeValueAsString(json))
        }

        return doc
    }

    boolean updatePropertyWithLinks(property, links) {
        boolean updated = false
        def relatedDoc, relatedDocJson, relatedItem, updateAction
        if (links.size() > 0) {
            for (link in links) {
                log.debug("Link type: ${link.type}")
                log.debug("Trying to get document with identifier ${link.identifier}")
                relatedDoc = whelk.get(new URI(link.identifier))
                if (relatedDoc == null) {
                    continue
                }
                relatedDocJson = mapper.readValue(relatedDoc.dataAsString, Map)
                relatedItem = relatedDocJson.about?.instanceOf ?: relatedDocJson.about
                if (relatedItem.get("@type") == property.get("@type")) {
                    try {
                        updateAction = "update" + property["@type"] + "Id"
                        updated = "$updateAction"(property, relatedItem)
                        log.debug("$updated")
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
