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
        log.debug("Trying to find links for ${doc.identifier}")
        def links = []
        mapper = new ObjectMapper()
        def json = mapper.readValue(doc.dataAsString, Map)
        if (doc && (doc.contentType == "application/json" || doc.contentType == "application/ld+json")) {
            links = collectIds(json, "", doc.identifier)
        }
        return links as Set
    }

    def collectIds(map, type, selfId) {
        def ids = []
        def labelKey, searchStr, esQuery, result, resultJson

        if (map instanceof Map) {

            map.each { propKey, propValue ->

                if (propValue instanceof List) {
                    propValue.each {
                        propValue.each {
                            ids.addAll(collectIds(it, propKey, selfId))
                        }
                    }
                }
                if (propValue instanceof Map) {
                    ids.addAll(collectIds(propValue, propKey, selfId))
                }

                if (map.containsKey("@type") && !map.containsKey("@value"))  { //&& !map.containsKey("@id") ?
                    //Try to to find matching authority entities using es-search
                    labelKey = searchLabel.get(map["@type"])
                    if (labelKey && map.containsKey(labelKey)) {
                        def searchTerm = URLEncoder.encode(map.get(labelKey), "UTF-8")
                        searchStr = "$labelKey:$searchTerm"
                        esQuery = new ElasticQuery(searchStr)
                        esQuery.indexType = map["@type"].toLowerCase() //or search auth with @type?

                        log.trace("Performing search on: $searchStr ...")
                        result = whelk.search(esQuery)

                        log.trace("Number of hits: ${result.numberOfHits}")
                        resultJson = mapper.readValue(result.toJson(), Map)

                        if (resultJson.hits > 0) {
                            for (r in resultJson.list) {
                                ids << new Link(new URI(r["identifier"]), "auth")
                            }
                            log.trace("Result data: " + mapper.writeValueAsString(resultJson))
                        }
                    }

                }

            }
        }

        return ids
    }
}

