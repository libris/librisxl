package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.component.ElasticQuery
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Link

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

@Log
class IndexLinkFinder extends BasicPlugin implements LinkFinder {
    String requiredContentType = "application/ld+json"
    ObjectMapper mapper
    def whelk

    def searchLabel = ["Person" : "controlledLabel", "Concept" : "prefLabel"]

    public IndexLinkFinder(Whelk whelk) {
        this.whelk = whelk
    }

    @Override
    Set<Link> findLinks(Document doc) {
        log.info("Trying to find links for ${doc.identifier}")
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
                    ids.addAll(collect(propValue, propKey, selfId))
                }

                if (map.containsKey("@type") && !map.containsKey("@value"))  {
                    //Try to to find matching authority entities using es-search
                    labelKey = searchLabel.get(map["@type"])
                    if (labelKey) {
                        searchStr = "$labelKey:${map.get(labelKey)}"
                        esQuery = new ElasticQuery(searchStr)
                        esQuery.indexType = map["@type"].toLowerCase() //or search auth with @type?

                        log.info("Performing search on: $searchStr ...")
                        result = whelk.search(esQuery)

                        log.info("Number of hits: ${result.numberOfHits}")
                        resultJson = mapper.readValue(results.toJson(), Map)

                        if (resultJson.hits > 0) {
                            for (r in resultJson.list) {
                                ids << new Link(new URI(r["identifier"]), "auth")
                            }
                            log.debug("Result data: " + mapper.writeValueAsString(json))
                        }
                    }

                }

            }
        }

        return ids
    }
}

