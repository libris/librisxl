package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.ElasticQuery

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Deprecated
@Log
class JsonLDLinkEnhancerFormatConverter extends BasicFormatConverter implements WhelkAware {

    String requiredContentType = "application/ld+json"
    String resultContentType = "application/ld+json"
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
                    if (it instanceof Map) {
                        entity = it
                        entityType = it.get("@type")
                    }
                }
            }  else if (propValue instanceof Map) {
                    entity = propValue
                    entityType = propValue.get("@type")
            }

            if (entityType && !entity.containsKey("@value"))  {
                //Try to update entity.@id with matching authority entities using es-search
                labelKey = searchLabel.get(entityType)
                if (labelKey) {
                    searchStr = "$labelKey:${entity.get(labelKey)}"
                    esQuery = new ElasticQuery(searchStr)
                    esQuery.indexType = entityType.toLowerCase() //or search auth with @type?

                    log.trace("Performing search on: $searchStr ...")
                    result = whelk.search(esQuery)

                    log.trace("Number of hits: ${result.numberOfHits}")

                    if (result.numberOfHits == 1) {
                        entity["@id"] = "/resource" + result.hits[0]["identifier"]
                    }  //TODO: else if more than 1 hit?
                }

            }

        }

        if (changedData) {
            log.debug("Document has changed.")
            return doc.withData(mapper.writeValueAsString(json))
        }

        return doc
    }

}
