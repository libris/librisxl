package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.component.ElasticQuery
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Link

import static se.kb.libris.conch.Tools.*

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

@Log
class IndexLinkFinder extends BasicPlugin implements LinkFinder, WhelkAware {
    String requiredContentType = "application/ld+json"
    ObjectMapper mapper
    def whelk

    def searchLabel = ["Person" : "controlledLabel", "Concept" : "prefLabel", "Organization" : ["name", "label"]]

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
            links = collectIds(getDataAsMap(doc), "", doc.identifier)
        }

        return links as Set
    }

    def performSearch(labelKey, prop, propValue) {
        def searchStr, esQuery, searchTerm

        log.trace("Property entity type: $propValue")

        if (labelKey instanceof List) {
            labelKey.each { x ->
                searchTerm = prop.get(x)
            }
        } else searchTerm = prop.get(labelKey)

        if (searchTerm) {
            if (labelKey instanceof List) {
                labelKey.eachWithIndex() { it, i ->
                    if (i == 0) {
                        searchStr = "$it:" + URLEncoder.encode(searchTerm, "UTF-8")
                    } else {
                        searchStr = searchStr + " OR $it:" + URLEncoder.encode(searchTerm, "UTF-8")
                    }
                }
                log.trace("labelKey is List, resulting in searchStr: ${searchStr}")
            } else {
                searchStr = "$labelKey:" + URLEncoder.encode(searchTerm, "UTF-8")
            }
            esQuery = new ElasticQuery(searchStr)
            esQuery.indexType = propValue.toLowerCase() //or search auth with @type?

            log.trace("Performing search on: $searchStr ... using indextype ${esQuery.indexType}")
            return whelk.search(esQuery)
        }
    }

    def collectIds(prop, type, selfId) {
        def ids = []
        def labelKey, result, resultJson

        if (prop instanceof Map) {

            prop.each { propKey, propValue ->

                if ((propKey == "@type" || (propKey == "@id" && (type == "broader" || type == "narrower"))) && !prop.containsKey("@value")) {
                    if (type == "broader" || type == "narrower") {
                        type = propValue = "Concept"
                    }
                    labelKey = searchLabel.get(propValue)

                    result = performSearch(labelKey, prop, propValue)
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
