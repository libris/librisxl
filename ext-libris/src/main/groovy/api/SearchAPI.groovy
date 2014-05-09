package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.plugin.*
import se.kb.libris.whelks.exception.*

@Log
class SearchAPI extends BasicAPI implements API {
    String id = "Search"
    String description = "Generic search query API. User parameters \"q\" for querystring, and optionally \"facets\" and \"boost\"."

    def config
    Whelk whelk


    SearchAPI(indexTypeConfig) {
        this.config = indexTypeConfig
    }

    @Override
    protected ApiResult doHandle(Map queryMap, List pathVars) {
        Map result = [:]
        def indexType = pathVars.first()
        def indexConfig = config.indexTypes[indexType]
        def boost = queryMap.boost ?: indexConfig?.defaultBoost?.split(",")
        def facets = queryMap.facets ?: indexConfig?.queryFacets?.split(",")
        if (indexConfig['queryProperties']) {
            queryMap.putAll(indexConfig['queryProperties'])
        }
        def elasticQuery = new ElasticQuery(queryMap)
        if (queryMap.f) {
            elasticQuery.query += " " + queryMap.f
        }
        elasticQuery.indexTypes = indexConfig.types ?: [indexType]
        if (facets) {
            for (f in facets) {
                elasticQuery.addFacet(f)
            }
        }
        if (boost) {
            for (b in boost) {
                if (b.size() > 0) {
                    def (k, v) = b.split(":")
                    elasticQuery.addBoost(k, Long.parseLong(v))
                }
            }
        }
        def fields = indexConfig?.get("queryFields")
        if (fields && fields.size() > 0) {
            elasticQuery.fields = fields
        }

        elasticQuery.highlights = indexConfig?.get("queryFields")
        if (!queryMap['sort'] && !queryMap['order']) {
            elasticQuery.sorting = indexConfig?.get("sortby")
        }
        try {
            def callback = queryMap.get("callback")
            def jsonResult =
            (callback ? callback + "(" : "") +
            performQuery(elasticQuery, indexConfig) +
            (callback ? ");" : "")

            return new ApiResult(jsonResult, "application/json")

        } catch (WhelkRuntimeException wrte) {
            log.error("error", wrte)

            //response.setStatus(Status.CLIENT_ERROR_NOT_FOUND, wrte.message)
        }
    }

    String performQuery(elasticQuery, indexConfig) {
        long startTime = System.currentTimeMillis()
        //def elasticQuery = new ElasticQuery(queryMap)
        log.debug("elasticQuery: ${elasticQuery.query}")
        def results
        try {

            log.debug("Handling search request with indextype $elasticQuery.indexTypes")

            log.debug("Query $elasticQuery.query Fields: ${elasticQuery.fields} Facets: ${elasticQuery.facets}")
            results = this.whelk.search(elasticQuery)
            /*
             // Unused, now with _source
            def keyList = indexConfig?.get("resultFields")
            log.info("keyList: $keyList")
            */
            def resultKey = indexConfig?.get("resultKey")
            log.debug("resultKey: $resultKey")
            def extractedResults = results.toJson(resultKey)

            return extractedResults
        } finally {
            log.info("Query [" + elasticQuery?.query + "] completed resulting in " + results?.numberOfHits + " hits")
            //this.logMessage = "Query [" + elasticQuery?.query + "] completed resulting in " + results?.numberOfHits + " hits"
        }
    }

    Map createProperFormMap(form) {
        def formMap = [:].withDefault{ [] }
        for (name in form.names) {
            def va = form.getValuesArray(name) as List
            if (va.size() == 1) {
                va = va.first().split(",")
            }
            formMap.get(name).addAll(va)
        }
        return formMap
    }
}
