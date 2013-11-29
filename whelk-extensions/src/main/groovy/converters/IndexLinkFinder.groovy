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

    def searchLabel = ["Person" : "controlledLabel", "Concept" : "prefLabel"]

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
            log.trace("Doc is ${doc.contentType}. Collecting ids ...")
            links = collectIds(doc.dataAsMap, "", doc.identifier)
        }
        return links as Set
    }

    def collectIds(prop, type, selfId) {
        def ids = []
        def labelKey, searchStr, esQuery, result, resultJson

        if (prop instanceof Map) {
            prop.each { propKey, propValue ->
                if (propValue instanceof Map && propValue.containsKey("@type") && !propValue.containsKey("@value")) {

                    //Try to to find matching authority entities using es-search
                    labelKey = searchLabel.get(propValue["@type"])
                    log.trace("labelKey: $labelKey")
                    if (labelKey && propValue.containsKey(labelKey)) {
                        log.trace("Examining ...")
                        def searchTerm = URLEncoder.encode(propValue.get(labelKey), "UTF-8")
                        searchStr = "$labelKey:$searchTerm"
                        esQuery = new ElasticQuery(searchStr)
                        esQuery.indexType = propValue["@type"].toLowerCase() //or search auth with @type?

                        log.trace("Performing search on: $searchStr ...")
                        result = whelk.search(esQuery)

                        log.trace("Number of hits: ${result.numberOfHits}")
                        resultJson = mapper.readValue(result.toJson(), Map)

                        if (resultJson.hits > 0) {
                            for (r in resultJson.list) {
                                if (r["identifier"] != selfId && r["identifier"] != "/resource" + selfId)  {
                                    ids << new Link(new URI(r["identifier"]), propKey)
                                    log.debug("Added link of type: $propKey to ${r["identifier"]} to doc with id $selfId")
                                }
                            }
                            log.trace("Result data: " + mapper.writeValueAsString(resultJson))
                        }
                    }
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

