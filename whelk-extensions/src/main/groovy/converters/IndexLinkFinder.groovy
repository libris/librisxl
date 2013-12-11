package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.component.ElasticQuery
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Link

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

@Log
class IndexLinkFinder extends BasicPlugin implements LinkFinder, WhelkAware {
    String requiredContentType = "application/ld+json"
    ObjectMapper mapper
    def whelk

    def searchLabel = ["Person" : "controlledLabel", "Concept" : "prefLabel", "Organization" : "label"]

    public IndexLinkFinder(def whelk) {
        this.whelk = whelk
    }

    void setWhelk(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    Set<Link> findLinks(Document doc) {
        log.trace("Running IndexLinkFinder, trying to find links for ${doc.identifier} ...")
        def links = []
        mapper = new ObjectMapper()

        if (doc && (doc.contentType == "application/json" || doc.contentType == "application/ld+json")) {

            log.trace("Doc is ${doc.contentType}. Collecting ids ... dataAsMap is of type ${doc.dataAsMap.getClass()}")
            links = collectIds(doc.dataAsMap, "", doc.identifier)
        }

        return links as Set
    }

    def performSearch(prop, propValue) {
        def labelKey, searchStr, esQuery, searchTerm

        log.trace("Property entity type: $propValue")

        labelKey = searchLabel.get(propValue)

        if (labelKey && prop.containsKey(labelKey)) {

            searchTerm = URLEncoder.encode(prop.get(labelKey), "UTF-8")
            searchStr = "$labelKey:$searchTerm"
            esQuery = new ElasticQuery(searchStr)
            esQuery.indexType = propValue.toLowerCase() //or search auth with @type?

            log.trace("Performing search on: $searchStr ... using indextype ${esQuery.indexType}")
            return whelk.search(esQuery)
        }
    }

    def collectIds(prop, type, selfId) {
        def ids = []
        def result, resultJson

        if (prop instanceof Map) {

            prop.each { propKey, propValue ->

                if (propKey == "@type" && !prop.containsKey("@value")) {
                    result = performSearch(prop, propValue)
                } else if (propKey == "@id" && (type == "broader" || type == "narrower") && !prop.containsKey("@value")) {
                    type = "Concept"
                    result = performSearch(prop, type)
                }

                if (result) {
                    log.trace("Number of hits: ${result.numberOfHits}")
                    resultJson = mapper.readValue(result.toJson(), Map)
                    if (resultJson.hits > 0) {
                        for (r in resultJson.list) {
                            if (r["identifier"] != selfId && r["identifier"] != "/resource" + selfId)  {
                                ids << new Link(new URI(r["identifier"]), type)
                                log.debug("Added link of type: $type to ${r["identifier"]} to doc with id $selfId")
                            }
                        }
                        log.trace("Result data: " + mapper.writeValueAsString(resultJson))
                    }

                    result = null
                }

                if (propValue instanceof Map) {
                    ids.addAll(collectIds(propValue, propKey, selfId))
                }

                if (propValue instanceof List) {
                    propValue.each {
                        ids.addAll(collectIds(it, propKey, selfId))
                    }
                }

            }

        }

        return ids
    }
}
