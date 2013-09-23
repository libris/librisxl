package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.component.ElasticQuery

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class IndexBasedFormatAugmenter extends BasicFormatConverter implements WhelkAware {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def whelk

    public void setWhelk(Whelk whelk) {
        this.whelk = whelk
    }

    Document doConvert(Document doc) {
        def json = mapper.readValue(doc.dataAsString, Map)
        def result = [:]

        HashMap<String, HashMap<String, String>> linksMap = ["creator": ["propertyPath": "about.instanceOf.creator", "searchStr": "controlledLabel", "indexType": "auth", "findIn": "about.controlledLabel"]]

        linksMap.each { key, props ->
            def propertyPath = props["propertyPath"]
            def searchStr = props["searchStr"]
            def findIn = props["findIn"]
            def type = props["indexType"]
            def jsonMap = json
            def propsInPath = propertyPath.tokenize(".")
            for (p in propsInPath) {
                try {
                    jsonMap = jsonMap[p]
                } catch (Exception e) {
                    jsonMap = null
                    break
                }
            }
            if (jsonMap && jsonMap instanceof List) {
                def searchTerm
                int i = 0
                for (c in jsonMap) {
                    searchTerm = c.get(searchStr, null)
                    if (searchTerm) {
                        log.debug("Trying to find link for $propertyPath... using string $searchStr. Searching in $type: $findIn.")
                        log.debug("Using $searchStr: $searchTerm to perform search...")

                        def queryMap = ["q": "$findIn:$searchTerm"]
                        def query = new ElasticQuery(queryMap).withType(type)
                        def results = this.whelk.search(query)

                        def resultJson = mapper.readValue(results.toJson(), Map)
                        log.debug("Results: " + results.toJson())
                        if (resultJson.hits > 0) {
                            String extractedLink
                            for (r in resultJson.list) {
                                extractedLink = r["identifier"]
                                json["about"]["instanceOf"]["creator"][i++]["@link"] = extractedLink
                            }
                            log.debug("Extracted link: $extractedLink")
                            log.debug("Result data: " + mapper.writeValueAsString(json))
                            doc = doc.withData(mapper.writeValueAsString(json)).withLink(extractedLink, "creator")
                        }
                    }
                }
            }
        }
        return doc
    }
}


