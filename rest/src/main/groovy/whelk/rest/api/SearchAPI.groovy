package whelk.rest.api

import groovy.util.logging.Log4j2 as Log

import javax.servlet.http.*

import whelk.*
import whelk.component.*
import whelk.plugin.*
import whelk.exception.*

import java.util.regex.Pattern

@Log
class SearchAPI implements RestAPI {
    String description = "Generic search query API. User parameters \"q\" for querystring, and optionally \"facets\" and \"boost\"."

    def config
    Whelk whelk

    Pattern pathPattern = Pattern.compile("/([\\w/]+)/_search")


    SearchAPI(indexTypeConfig) {
        this.config = indexTypeConfig
    }

    @Override
    void handle(HttpServletRequest request, HttpServletResponse response, List pathVars) {
        def queryMap = new HashMap(request.parameterMap)
        Map result = [:]
        def indexType = pathVars.first()
        def indexConfig = config.indexTypes.get(indexType, [:])
        def boost = queryMap.boost ?: indexConfig?.defaultBoost?.split(",")
        def facets = queryMap.facets ?: indexConfig?.queryFacets?.split(",")
        if (indexConfig['queryProperties']) {
            queryMap.putAll(indexConfig['queryProperties'])
        }
        if (indexConfig['terms']) {
            queryMap.put("terms", indexConfig['terms'])
        }
        def elasticQuery = new ElasticQuery(queryMap)
        if (queryMap.f) {
            elasticQuery.query += " " + queryMap.f.first()
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
            def callback = queryMap.get("callback")?.first()
            def jsonResult =
            (callback ? callback + "(" : "") +
            performQuery(elasticQuery, indexConfig) +
            (callback ? ");" : "")

            HttpTools.sendResponse(response, jsonResult, "application/json")

        } catch (WhelkRuntimeException wrte) {
            response.sendError(response.SC_NOT_FOUND)
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
