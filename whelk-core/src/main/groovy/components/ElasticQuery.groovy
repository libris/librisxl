package se.kb.libris.whelks.component

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.ElasticSearch
import se.kb.libris.whelks.exception.WhelkRuntimeException

@Log
class ElasticQuery extends Query {
    String indexType, phraseField, phraseValue
    boolean phraseQuery = false

    Map<String,List> terms = null
    Map sourceFilter = null

    final static ObjectMapper mapper = new ObjectMapper()

    ElasticQuery() {super()}

    ElasticQuery(String qs) {
        super(qs)
    }
    ElasticQuery(String field, String value) {
        super()
        this.phraseQuery = true
        this.phraseField = field
        this.phraseValue = value
    }
    ElasticQuery(Map qmap) {
        super(qmap)
        if (qmap.get("type")) {
            this.indexType = qmap.get("type")
        }
        if (qmap.get("indexType")) {
            this.indexType = qmap.get("indexType")
        }
        if (qmap.get("terms")) {
            terms = new HashMap<String,List>()
            def (term, values) = qmap.get("terms").split(":", 2)
            terms.put(term, values.split(","))
        } else if (!this.query) {
            throw new WhelkRuntimeException("Trying to create empty query.")
        }
        if (qmap.get("_source.include")) {
            if (!sourceFilter) {
                sourceFilter = [:]
            }
            sourceFilter["include"] = qmap.get("_source.include").split(",")
        }
        if (qmap.get("_source.exclude")) {
            if (!sourceFilter) {
                sourceFilter = [:]
            }
            sourceFilter["exclude"] = qmap.get("_source.exclude").split(",")
        }
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
        this.indexType = type
        return this
    }

    String toJsonQuery() {
        def dslQuery = [:]
        dslQuery['from'] = this.start
        dslQuery['size'] = this.n
        if (sourceFilter) {
            dslQuery['_source'] = sourceFilter
        }
        if (this.query == "*") {
            dslQuery['query'] = ['match_all': [:]]
        } else if (terms) {
            terms.each { t,v ->
                dslQuery['query'] = ["terms": [(t) : v]]
            }
        } else if (phraseQuery) {
            throw new UnsupportedOperationException("Phrasequery not yet implemented in DSL.")
            //srb.setQuery(textPhrase(q.phraseField, q.phraseValue))
        } else {
            dslQuery['query'] = ['query_string': ['query': this.query, "default_operator": "and"]]

            if (this.fields) {
                def fieldsList = []
                this.fields.each {
                    if (this.boost && this.boost[it]) {
                        fieldsList << it + "^" + this.boost[it]
                    } else {
                        fieldsList << it
                    }
                }
                dslQuery.query.query_string.put('fields', fieldsList)
            } else if (this.boost) {
                def fieldsList = ["_all"]
                this.boost.each { f, b ->
                    fieldsList << f + "^" + b
                }
                dslQuery.query.query_string.put('fields', fieldsList)
            }
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
            this.facets.each {
                if (it instanceof TermFacet) {
                    log.trace("Building FIELD facet for ${it.field}")
                    facetMap[(it.name)] = ["terms":["field":it.field,"size": ElasticSearch.MAX_NUMBER_OF_FACETS]]
                } else if (it instanceof ScriptFieldFacet) {
                    if (it.field.contains("@")) {
                        log.warn("Forcing FIELD facet for ${it.field}")
                        facetMap[(it.name)] = ["terms":["field":it.field,"size": ElasticSearch.MAX_NUMBER_OF_FACETS]]
                    } else {
                        log.trace("Building SCRIPTFIELD facet for ${it.field}")
                        throw new UnsupportedOperationException("Scriptfield facet not yet implemented in DSL")
                        //srb = srb.addFacet(FacetBuilders.termsFacet(it.name).scriptField("_source.?"+it.field.replaceAll(/\./, ".?")).size(ElasticSearch.MAX_NUMBER_OF_FACETS))
                    }
                } else if (it instanceof QueryFacet) {
                    throw new UnsupportedOperationException("QueryFacet not yet implemented in DSL")
                    /*def qf = new QueryStringQueryBuilder(it.query).defaultOperator(QueryStringQueryBuilder.Operator.AND)
                    srb = srb.addFacet(FacetBuilders.queryFacet(it.name).query(qf))
                    */
                }

            }
            dslQuery['facets'] = facetMap
        }
        def constructedFilters = []
        if (this.filters) {
            this.filters.each { k, v ->
                if (k.charAt(0) == '!') {
                    constructedFilters = ["not": ["filter": ["term" : [(k): v]]]]
                } else {
                    constructedFilters << ["term": [(k): v]]
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
            dslQuery['post_filter'] = ["and" : ["filters": constructedFilters]]
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
