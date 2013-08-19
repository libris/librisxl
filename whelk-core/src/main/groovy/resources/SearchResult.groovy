package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.IndexDocument
import se.kb.libris.whelks.SearchResult
import se.kb.libris.whelks.component.ElasticJsonMapper

import org.codehaus.jackson.map.ObjectMapper

@Log
class SearchResult {

    Iterable hits
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

    String toJson(Map pattern) {
        log.info("New toJson-method")
        def result = [:]
        result['hits'] = numberOfHits
        result['list'] = []
        hits.each {
            result['list'] << it.dataAsMap
        }
        if (facets) {
            result['facets'] = facets
        }
        return mapper.writeValueAsString(result)
    }

    private jsonifyFacets() {
        return mapper.writeValueAsString(facets)
    }
}

