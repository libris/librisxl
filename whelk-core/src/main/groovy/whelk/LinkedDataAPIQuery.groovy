package whelk

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

@Log
class LinkedDataAPIQuery extends ElasticQuery {

    def keyWords = ["_page", "_pageSize", "_sort", "_where", "_orderBy", "_select", "callback"]
    final static ObjectMapper mapper = new ObjectMapper()
    Map requestMap

    LinkedDataAPIQuery(Map requestMap) {
        this.requestMap = requestMap
        log.info("reqmap: $requestMap")
        if (requestMap.containsKey("_pageSize")) {
            n = requestMap["_pageSize"][0] as int
        }
        if (requestMap.containsKey("_page")) {
            start = (((requestMap["_page"][0] as int) - 1) * n)
            log.info("Set start: $start")
        }
        if (requestMap.containsKey("_sort")) {
            requestMap.get("_sort").each {
                def direction = "ASC"
                if (it && it.startsWith("-")) {
                    it = it.substring(1)
                    direction = "DESC"
                }
                log.trace("Set order $it ($direction)")
                addSort(it, direction)
            }
        }
    }

    String toJsonQuery() {
        def dslQuery = [:]
        dslQuery['from'] = this.start
        dslQuery['size'] = this.n
        if (sourceFilter != null) {
            dslQuery['_source'] = sourceFilter
        }
        def constructedQueries = []
        def ranges = [:]
        def boolMustGroup = []
        def boolShouldGroup = []
        requestMap.each { k, values ->
            log.info("$k = $values")
            if (!(k in keyWords)) {
                def v = values.first()
                if (k.startsWith("min-")) {
                    k = k.substring(4)
                    ranges.get(k, [:]).put("from", v)
                    ranges.get(k).put("include_lower", true)
                } else if (k.startsWith("minEx-")) {
                    k = k.substring(6)
                    ranges.get(k, [:]).put("from", v)
                    ranges.get(k).put("include_lower", false)
                } else if (k.startsWith("max-")) {
                    k = k.substring(4)
                    ranges.get(k, [:]).put("to", v)
                    ranges.get(k).put("include_upper", true)
                } else if (k.startsWith("maxEx-")) {
                    k = k.substring(6)
                    ranges.get(k, [:]).put("to", v)
                    ranges.get(k).put("include_upper", false)
                } else {
                    if (values.size() > 1) {
                        for (val in values) {
                            boolShouldGroup << ["match": [(k): val]]
                        }
                    } else {
                        boolMustGroup << ["match": [(k): v]]
                    }
                }
            }
        }
        if (boolShouldGroup) {
            boolMustGroup << ["bool" : ["should" : boolShouldGroup]]
        }
        if (boolMustGroup) {
            constructedQueries.addAll(boolMustGroup)
        }
        if (ranges) {
            constructedQueries << ["range":ranges]
        }
        log.trace("constructedQueries: $constructedQueries")
        if (constructedQueries.size() > 1) {
            dslQuery['query'] = ["bool" : ["must" : constructedQueries]]
        } else if (constructedQueries.size() == 1) {
            dslQuery['query'] = constructedQueries[0]
        }
        def sortList = []
        this.sorting.each {
            sortList << [(it.key): ["order":it.value.toLowerCase(), "missing": "_last", "ignore_unmapped":true]]
        }
        if (sortList) {
            dslQuery['sort'] = sortList
        }
        return mapper.writeValueAsString(dslQuery)
    }
}

