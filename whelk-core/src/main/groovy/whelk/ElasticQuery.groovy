package whelk

import groovy.util.logging.Log4j2 as Log

import org.codehaus.jackson.map.ObjectMapper

import whelk.exception.WhelkRuntimeException

@Log
class ElasticQuery extends Query {
    String phraseField, phraseValue
    List indexTypes
    boolean phraseQuery = false
    static final int MAX_NUMBER_OF_FACETS = 100

    Map<String,List> terms = null
    def sourceFilter = null

    final static ObjectMapper mapper = new ObjectMapper()

    ElasticQuery() {}

    ElasticQuery(String qs) {
        super(qs)
    }
    ElasticQuery(String field, String value) {
        this.phraseQuery = true
        this.phraseField = field
        this.phraseValue = value
    }
    ElasticQuery(Map qmap) {
        super(qmap)
        if (qmap.get("type")) {
            setIndexTypes(qmap.get("type"))
        }
        if (qmap.get("indexType")) {
            setIndexTypes(qmap.get("indexType"))
        }
        if (qmap.get("terms")) {
            terms = new HashMap<String,List>()
            for (t in qmap.get("terms")) {
                def (term, value) = t.split(":", 2)
                terms.get(term, []) << value
            }
        } else if (!this.query) {
            throw new WhelkRuntimeException("Trying to create empty query.")
        }
        if (qmap.containsKey("_source")) {
            sourceFilter = qmap.get("_source")
        } else {
            if (qmap.get("_source.include")) {
                if (!sourceFilter) {
                    sourceFilter = [:]
                }
                sourceFilter["include"] = qmap.get("_source.include")
            }
            if (qmap.get("_source.exclude")) {
                if (!sourceFilter) {
                    sourceFilter = [:]
                }
                sourceFilter["exclude"] = qmap.get("_source.exclude")
            }
        }
    }

    String[] getIndexTypes() {
        indexTypes as String[]
    }

    void setIndexTypes(String type) {
        setIndexTypes([type])
    }
    void setIndexTypes(String[] types) {
        this.indexTypes = types as List
    }

    void setIndexTypes(List<String> types) {
        this.indexTypes = new ArrayList(types)
    }

    ElasticQuery(Query q) {
        q.properties.each { name, value ->
            log.trace("[ElasticQuery] setting $name : $value")
            try {
                this."$name" = value
            } catch (groovy.lang.ReadOnlyPropertyException rope) {
                log.trace("[ElasticQuery] ${rope.message}")
            }
        }
    }

    ElasticQuery withType(String type) {
        setIndexTypes(type)
        return this
    }

    Map buildQueryStringQuery() {
        Map qsMap = [:]
        if (this.query) {
            qsMap = ['query_string': ['query': this.query, "default_operator": "and"]]
        }

        if (this.fields) {
            def fieldsList = []
            this.fields.each {
                if (this.boost && this.boost[it]) {
                    fieldsList << it + "^" + this.boost[it]
                } else {
                    fieldsList << it
                }
            }
            qsMap.query_string.put('fields', fieldsList)
        } else if (this.boost) {
            def fieldsList = ["_all"]
            this.boost.each { f, b ->
                fieldsList << f + "^" + b
            }
            qsMap.query_string.put('fields', fieldsList)
        }
        return qsMap
    }

    String toJsonQuery() {
        def dslQuery = [:]
        dslQuery['from'] = this.start
        dslQuery['size'] = this.n
        if (sourceFilter != null) {
            dslQuery['_source'] = sourceFilter
        }
        if (this.query == "*") {
            dslQuery['query'] = ['match_all': [:]]
        } else if (terms) {
            def termsList = []
            terms.each { t,v ->
                termsList << ["terms": [(t) : v]]
            }
            if (termsList.size() == 1) {
                if (this.query) {
                    dslQuery['query'] = [
                        'bool': [
                            'must': [
                                buildQueryStringQuery(),
                                termsList.first()
                            ]
                        ]
                    ]
                } else {
                    dslQuery['query'] = termsList.first()
                }
            } else {
                dslQuery['query'] = ['bool': ['must': [ termsList ] ] ]
                if (this.query) {
                    dslQuery.query.bool.must << buildQueryStringQuery()
                }
            }
        } else if (phraseQuery) {
            throw new UnsupportedOperationException("Phrasequery not yet implemented in DSL.")
            //srb.setQuery(textPhrase(q.phraseField, q.phraseValue))
        } else {
            dslQuery['query'] = buildQueryStringQuery()
        }
        if (this.sorting) {
            def sortList = []
            this.sorting.each {
                sortList << [(it.key): ["order":it.value.toLowerCase(), "missing": "_last", "ignore_unmapped":true]]
            }
            dslQuery['sort'] = sortList
        }
        if (this.facets) {
            def facetMap = [:]
            /* Necessary?
            this.facets.each {
                if (it instanceof TermFacet) {
                    log.trace("Building FIELD facet for ${it.field}")
                    facetMap[(it.name)] = ["terms":["field":it.field,"size": MAX_NUMBER_OF_FACETS]]
                } else if (it instanceof ScriptFieldFacet) {
                    if (it.field.contains("@")) {
                        log.warn("Forcing FIELD facet for ${it.field}")
                        facetMap[(it.name)] = ["terms":["field":it.field,"size": MAX_NUMBER_OF_FACETS]]
                    } else {
                        log.trace("Building SCRIPTFIELD facet for ${it.field}")
                        throw new UnsupportedOperationException("Scriptfield facet not yet implemented in DSL")
                    }
                } else if (it instanceof QueryFacet) {
                    throw new UnsupportedOperationException("QueryFacet not yet implemented in DSL")
                }

            }
                */
            dslQuery['facets'] = facetMap
        }
        def constructedFilters = []
        def filterOperator = "and"
        if (this.filters) {
            def previousKeys = []
            this.filters.each {
                it.each { k, v ->
                    log.debug("Build filter: $k = $v")
                    if (k in previousKeys) { filterOperator = "or" }
                    previousKeys << k
                    if (k.charAt(0) == '!') {
                        constructedFilters << ["not": ["filter": ["term" : [(k): v]]]]
                    } else {
                        constructedFilters << ["term": [(k): v]]
                    }
                }
            }
        }
        if (this.ranges) {
            this.ranges.each {k, v ->
                if (k.charAt(0) == '!') {
                    constructedFilters << ["not": ["filter": ["range":[(k): ["from": v[0], "to": v[1], "include_lower": true, "include_upper": true]]]]]
                } else {
                    constructedFilters << ["range":[(k): ["from": v[0], "to": v[1], "include_lower": true, "include_upper": true]]]
                }
            }
        }
        if (constructedFilters.size() > 1) {
            dslQuery['post_filter'] = [(filterOperator) : ["filters": constructedFilters]]
        } else if (constructedFilters.size() == 1) {
            dslQuery['post_filter'] = constructedFilters[0]
        }
        if (this.highlights) {
            def highlightFields = [:]
            this.highlights.each {
                highlightFields[(it)] = [:]
            }
            dslQuery['highlight'] = ["pre_tags": [ "" ], "post_tags": [ "" ], "fields": highlightFields]
        }
        log.debug("Elastic DSL Json: " + mapper.defaultPrettyPrintingWriter().writeValueAsString(dslQuery))
        return mapper.writeValueAsString(dslQuery)
    }
}
