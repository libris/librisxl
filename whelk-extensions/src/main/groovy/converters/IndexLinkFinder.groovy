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

    def searchLabel = ["Person": "controlledLabel", "Concept": "prefLabel", "Organization": ["name", "label"], "Work": ["uniformTitle", "attributedTo.controlledLabel"]]

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

    def performSearch(labelKeyOrKeys, prop, propType) {
        def searchStr, esQuery, searchTerm
        def operand = "OR"

        log.trace("Property entity type: $propType")

        if (labelKeyOrKeys instanceof List) {

            if (propType == "Work") operand = "AND"

            labelKeyOrKeys.eachWithIndex() { label, i ->
                searchTerm = prop.get(label)
                if (searchTerm) {
                    if (i == 0) {
                        searchStr = "$label:" + URLEncoder.encode(searchTerm, "UTF-8")
                    } else {
                        searchStr = searchStr + " $operand $label:" + URLEncoder.encode(searchTerm, "UTF-8")
                    }
                }
            }
            log.trace("labelKey is List, resulting in searchStr: ${searchStr}")

        } else {

            searchTerm = prop.get(labelKeyOrKeys)
            if (searchTerm) {
                searchStr = "$labelKeyOrKeys:" + URLEncoder.encode(searchTerm, "UTF-8")
            }
        }

        if (searchStr) {
            esQuery = new ElasticQuery(searchStr)
            esQuery.indexType = propType.toLowerCase() //or search auth with @type?

            log.trace("Performing search on: $searchStr ... using indextype ${esQuery.indexType}")
            return whelk.search(esQuery)
        }
    }


    def collectIds(prop, relationType, selfId) {
        def ids = []
        def labelKey, result, resultJson, entityType

        if (prop instanceof Map) {

            prop.each { propKey, propValue ->

                if ((propKey == "@type" || (propKey == "@id" && (relationType == "broader" || relationType == "narrower"))) && !prop.containsKey("@value")) {

                    switch (relationType) {
                        case "broader":
                        case "narrower":
                            entityType = "Concept"
                            break
                        case "relation":
                            entityType = "Work"
                            break
                        default:
                            entityType = propValue
                    }

                    labelKey = searchLabel.get(entityType)

                    result = performSearch(labelKey, prop, entityType)
                }

                if (result) {
                    log.trace("Number of hits: ${result.numberOfHits}")
                    resultJson = mapper.readValue(result.toJson(), Map)
                    if (resultJson.hits > 0) {
                        for (r in resultJson.list) {
                            if (r["identifier"] != selfId && r["identifier"] != "/resource" + selfId)  {
                                ids << new Link(new URI(r["identifier"]), relationType)
                                log.debug("Added link of type: $relationType to ${r["identifier"]} to doc with id $selfId")
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