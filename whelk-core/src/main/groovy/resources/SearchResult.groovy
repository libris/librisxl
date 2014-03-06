package se.kb.libris.whelks.result

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.IndexDocument
import se.kb.libris.whelks.component.ElasticJsonMapper

import org.codehaus.jackson.map.ObjectMapper

@Log
class SearchResult {

    List hits
    Map facets
    ObjectMapper mapper

    long numberOfHits = 0

    SearchResult(long nrHits) {
        this.numberOfHits = nrHits
        this.hits = new ArrayList<IndexDocument>()
        this.mapper = new ElasticJsonMapper()
    }

    void setNumberOfHits(int nrHits) {
        this.numberOfHits = nrHits
    }

    void addHit(IndexDocument d) {
        this.hits.add(d)
    }

    void addHit(IndexDocument d, Map<String, String[]> highlightedFields) {
        def doc = new IndexDocument(d, highlightedFields)
        this.hits.add(doc)
    }

    String toJson() {
        log.debug("Manual toJson()")
        def jsonString = new StringBuilder()
        jsonString << "{"
        jsonString << "\"hits\": " << numberOfHits << ","
        jsonString << "\"list\": ["
        hits.eachWithIndex() { it, i ->
            if (i > 0) { jsonString << "," }
            jsonString << "{\"identifier\": \"" << it.identifier << "\","
            jsonString << "\"data\":" << it.dataAsString << "}"
        }
        jsonString << "]"
        if (facets) {
            jsonString << ",\"facets\":" << jsonifyFacets()
        }
        jsonString << "}"
        return jsonString.toString()
    }

    Map toMap(List keys) {
        def result = [:]
        result['hits'] = numberOfHits
        result['list'] = []
        hits.each {
            def item = [:]
            def source = it.dataAsMap.asImmutable()
            for (key in keys) {
                log.debug("key: $key")
                item = extractStructure(key, item, source)
            }
            if (!item) {
                item = source
            }
            result['list'] << ['data':item, 'identifier':it.identifier]
        }
        if (facets) {
            result['facets'] = facets
        }
        return result
    }

    String toJson(List keys) {
        def result = toMap(keys)
        log.debug("toJson by $keys")
        return mapper.writeValueAsString(result)
    }

    private Map extractStructure(String keystring, Map result, Map source) {
        def keylist = keystring.split(/\./)
        int numkeys = keylist.size() - 1
        def keyResult = result
        keylist.eachWithIndex() { key, i ->
            if (source.containsKey(key)) {
                def item = source.get(key)
                if (item instanceof List || item instanceof Map) {
                    if (i == numkeys) {
                        keyResult.put(key, item)
                    } else {
                        if (!keyResult.containsKey(key)) {
                            keyResult.put(key, [:])
                        }
                        keyResult = keyResult.get(key)
                        source = item
                    }
                } else {
                    keyResult.put(key, item)
                }
            }
        }
        return result
    }

    private jsonifyFacets() {
        return mapper.writeValueAsString(facets)
    }
}

