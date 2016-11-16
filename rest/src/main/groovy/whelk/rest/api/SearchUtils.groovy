package whelk.rest.api

import groovy.util.logging.Slf4j as Log

import whelk.Document
import whelk.JsonLd
import whelk.Location
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.exception.WhelkRuntimeException

@Log
class SearchUtils {

    final static URI VOCAB_BASE_URI = new URI("https://id.kb.se/vocab/")

    final static int DEFAULT_LIMIT = 200
    final static int MAX_LIMIT = 4000
    final static int DEFAULT_OFFSET = 0

    static Map doSearch(Whelk whelk, Map queryParameters, String dataset,
                        String siteBaseUri) {
        String relation = getReservedQueryParameter('p', queryParameters)
        String reference = getReservedQueryParameter('o', queryParameters)
        String value = getReservedQueryParameter('value', queryParameters)
        String query = getReservedQueryParameter('q', queryParameters)

        Tuple2 limitAndOffset = getLimitAndOffset(queryParameters)
        int limit = limitAndOffset.first
        int offset = limitAndOffset.second

        Map pageParams = ['p': relation,
                          'o': reference,
                          'value': value,
                          'q': query,
                          '_limit': limit]

        Map results = null

        if (relation && reference) {
            results = findByRelation(whelk, relation, reference, limit, offset)
        } else if (relation && value) {
            results = findByValue(whelk, relation, value, limit, offset)
        } else if (reference) {
            results = findByQuotation(whelk, reference, limit, offset)
        } else if (query) {
            // If general q-parameter chosen, use elastic for query
            if (whelk.elastic) {
                results = queryElasticSearch(whelk, queryParameters,
                                             pageParams,
                                             dataset, siteBaseUri,
                                             limit, offset)
            } else {
                throw new WhelkRuntimeException("ElasticSearch not configured.")
            }
        } else {
            // If none of the special query parameters were specified,
            // we query PostgreSQL
            results = queryPostgreSQL(whelk, queryParameters, dataset,
                                      limit, offset)
        }

        return results
    }

    private static Map findByRelation(Whelk whelk, String relation,
                                      String reference,
                                      int limit, int offset) {
        log.debug("Calling findByRelation with p: ${relation} and " +
                  "o: ${reference}")

        List<Document> docs = whelk.storage.findByRelation(relation,
                                                           reference,
                                                           limit, offset)

        return assembleSearchResults(docs)
    }

    private static Map findByValue(Whelk whelk, String relation, String value,
                                   int limit, int offset) {
        log.debug("Calling findByValue with p: ${relation} and value: ${value}")

        List<Document> docs = whelk.storage.findByValue(relation, value,
                                                        limit, offset)

        return assembleSearchResults(docs)
    }

    private static Map findByQuotation(Whelk whelk, String identifier,
                                       int limit, int offset) {
        log.debug("Calling findByQuotation with o: ${identifier}")

        List<Document> docs = whelk.storage.findByQuotation(identifier,
                                                            limit, offset)

        return assembleSearchResults(docs)
    }

    private static Map queryElasticSearch(Whelk whelk, Map queryParameters,
                                          Map pageParams,
                                          String dataset, String siteBaseUri,
                                          int limit, int offset) {
        String query = getReservedQueryParameter('q', queryParameters)
        log.debug("Querying ElasticSearch")

        Map stats = null
        List mappings = []
        mappings << ['variable': 'q',
                     'predicate': getTermChip('textQuery'),
                     'value': query]
        def dslQuery = ElasticSearch.createJsonDsl(queryParameters,
                                                   limit, offset)

        Tuple2 mappingsAndPageParams = mapParams(queryParameters)
        mappings.addAll(mappingsAndPageParams.first)
        pageParams << mappingsAndPageParams.second

        if (siteBaseUri) {
            dslQuery['query']['bool'] << makeSiteFilter(siteBaseUri)
        }

        // TODO: statsTree may depend on site ({id,libris}.kb.se)
        Map statsTree = ['@type': []]

        if (statsTree) {
            dslQuery['aggs'] = buildAggQuery(statsTree)
        }

        Map esResult = whelk.elastic.query(dslQuery, dataset)

        int total = 0
        if (esResult['totalHits']) {
            total = esResult['totalHits']
        }

        List items = []
        if (esResult['items']) {
            items = toCards(esResult['items'])
        }

        if (statsTree) {
            stats = buildStats(whelk, esResult['aggregations'].asMap,
                               makeFindUrl(pageParams))
        }

        Map result = ['@type': 'PartialCollectionView']
        result['@id'] = makeFindUrl(pageParams, offset)
        result['itemOffset'] = offset
        result['totalItems'] = total

        mappings.tail().each { mapping ->
            Map params = pageParams.clone()
            params.remove(mapping['variable'])
            mapping['up'] = ['@id': makeFindUrl(params, offset)]
        }
        result['search'] = ['mapping': mappings]

        String value = getReservedQueryParameter('value', queryParameters)
        if (value) {
            result['value'] = value
        }

        Offsets offsets = new Offsets(total, limit, offset)

        result['first'] = ['@id': makeFindUrl(pageParams)]
        result['last'] = ['@id': makeFindUrl(pageParams, offsets.last)]

        if (offsets.prev) {
            if (offsets.prev == 0) {
                result['previous'] = result['first']
            } else {
                result['previous'] = ['@id': makeFindUrl(pageParams,
                                                         offsets.prev)]
            }
        }

        if (offsets.next) {
            result['next'] = ['@id': makeFindUrl(pageParams, offsets.next)]
        }

        result['items'] = items

        if (stats) {
            result['stats'] = stats
        }

        return result
    }

    private static Map queryPostgreSQL(Whelk whelk, Map queryParameters,
                                       String dataset,
                                       int limit, int offset) {
        log.debug("Querying PostgreSQL")

        return whelk.storage.query(queryParameters, dataset,
                                   autoDetectQueryMode(queryParameters))
    }

    /**
     * Create ES filter for specified siteBaseUri.
     *
     */
    static Map makeSiteFilter(String siteBaseUri) {
        return ['should': [
                   ['prefix': ['@id': siteBaseUri]],
                   ['prefix': ['sameAs.@id': siteBaseUri]]
                ],
                'minimum_should_match': 1]
    }

    /**
     * Build the term aggregation part of an ES query.
     *
     */
    static Map buildAggQuery(def tree, int size=1000) {
        Map query = [:]
        List keys = []

        // In Python, the syntax for iterating over each item in a
        // list and for iterating over each key in a dict is the
        // same. That's not the case for Groovy, hence the following
        if (tree instanceof Map) {
            keys = tree.keySet() as List
        } else if (tree instanceof List) {
            keys = tree
        }

        keys.each { key ->
          query[key] = ['terms': ['field': key, 'size': size]]
          if (tree instanceof Map) {
              query[key]['aggs'] = buildAggQuery(tree[key], size)
          }
        }
        return query
    }

    /*
     * Build aggregation statistics for ES result.
     *
     */
    private static Map buildStats(Whelk whelk, Map aggregations,
                                  String baseUrl) {
        Map result = [:]
        result = addSlices(whelk, [:], aggregations, baseUrl)
        return result
    }

    private static Map addSlices(Whelk whelk, Map stats, Map aggregations,
                                 String baseUrl) {
        Map sliceMap = aggregations.inject([:]) { acc, key, aggregation ->
            List observations = []
            Map sliceNode = ['dimension': key.replace('.@id', '')]

            aggregation['buckets'].each { bucket ->
                String itemId = bucket['key']
                String searchPageUrl = "${baseUrl}&${key}=${urlEncode(itemId)}"

                Map observation = ['totalItems': bucket.getAt('docCount'),
                                   'view': ['@id': searchPageUrl],
                                   'object': toChip(lookup(whelk, itemId))]

                Map bucketAggs = bucket.getAggregations().asMap

                observation = addSlices(whelk, observation, bucketAggs,
                                        searchPageUrl)
                observations << observation
            }

            if (observations) {
                sliceNode['observation'] = observations
                acc[key] = sliceNode
            }
        }

        if (sliceMap) {
          stats['sliceByDimension'] = sliceMap
        }

        return stats
    }

    /*
     * Read vocab item from db.
     *
     * Default to dummy value if not found.
     *
     */
    private static Map lookup(Whelk whelk, String itemId) {
        String fullId = VOCAB_BASE_URI.resolve(itemId).toString()
        Location loc = whelk.storage.locate(fullId, true)
        Document doc = loc?.document
        if (doc) {
            return getEntry(doc.data)
        } else {
            return ['@id': itemId, 'label': itemId]
        }
    }

    // FIXME move to Document or JsonLd
    private static Map getEntry(Map jsonLd) {
        // FIXME make this less brittle
        return jsonLd['@graph'][0]
    }

    /**
     * Create a URL for '/find' with the specified query parameters.
     *
     */
    static String makeFindUrl(Map queryParameters0, int offset=0) {
        Map queryParameters = queryParameters0.clone()
        if (!('q' in queryParameters)) {
            queryParameters['q'] = '*'
        }
        // q was added as a single value manually
        String query = queryParameters.remove('q')
        List params = ["q=${query}"]
        List keys = (queryParameters.keySet() as List).sort()
        keys.each { k ->
            def v = queryParameters[k]
            if (!v) {
                return
            }

            if (v instanceof List) {
                v.each { value ->
                    params << "${k}=${value}"
                }
            } else {
                params << "${k}=${v}"
            }
        }
        if (offset > 0) {
            params << "_offset=${offset}"
        }
        return "/find?${params.join('&')}"
    }

    /**
     * Get limit and offset from query parameters.
     *
     * Use default values if not in query.
     *
     */
    static Tuple2 getLimitAndOffset(Map queryParams) {
        int limit = parseIntFromQueryParams("_limit", queryParams,
                                            DEFAULT_LIMIT)
        // don't let users get carried away.
        if (limit > MAX_LIMIT) {
            limit = DEFAULT_LIMIT
        }
        int offset = parseIntFromQueryParams("_offset", queryParams,
                                             DEFAULT_OFFSET)

        return new Tuple2(limit, offset)
    }

    /*
     * Return specified query parameter parsed as int.
     *
     * Use default value if key not found.
     *
     */
    private static int parseIntFromQueryParams(String key, Map queryParams,
                                               int defaultValue) {
        if (queryParams.containsKey(key)) {
            def value = queryParams.get(key)

            // if someone supplies multiple values, we just pick the
            // first one and discard the rest.
            if (value instanceof List) {
                value = value[0]
            }

            if (value.isInteger()) {
                return value as int
            } else {
                return defaultValue
            }
        } else {
            return defaultValue
        }
    }

    private static Map getTermChip(termKey) {
        // FIXME get definition from vocab
        Map termDefinition = [:]
        String id = termDefinition.get('@id')
        if (!id) {
            id = termKey
        }
        // FIXME get label from termDefinition
        return ['@id': id, 'label': '']
    }

    // FIXME move to separate module?
    private static List toCards(List things) {
        // FIXME implement
        return things
    }

    private static List toChips(List things) {
        // FIXME implement
        return things
    }

    private static Map toChip(Map thing) {
        // FIXME implement
        return thing
    }

    /*
     * Get mappings and page params for specified query.
     *
     * Reserved parameters or parameters beginning with '_' are
     * filtered out.
     *
     */
    private static Tuple2 mapParams(Map params) {
        List result = []
        Map pageParams = [:]
        List reservedParams = getReservedParameters()
        params.each { param, paramValue ->
            if (param.startsWith("_") || param in reservedParams) {
                return
            }

            paramValue.each { val ->
                String valueProp
                String termKey
                def value
                if (param == JsonLd.TYPE_KEY ||
                    param.endsWith(JsonLd.ID_KEY)) {
                    valueProp = 'object'
                    termKey = param[0..-5]
                    value = [ID: val]
                } else {
                    valueProp = 'value'
                    termKey = param
                    value = val
                }

                Map termChip = getTermChip(termKey)

                result << ['variable': param, 'predicate': termChip,
                           (valueProp): value]

                if (!(param in pageParams)) {
                    pageParams[param] = []
                }
                pageParams[param] << val
            }
        }
        return new Tuple2(result, pageParams)
    }

    private static List getReservedParameters() {
        return ['q', 'p', 'o', 'value', '_limit', '_offset']
    }

    // TODO implement
    /*
     * Assemble PostgreSQL search results.
     *
     */
    private static Map assembleSearchResults(List<Document> docs) {
        Map result = [:]
        List items = []

        docs.each { doc ->
            items << toCard(doc.data)
        }

        result["items"] = items
        result["hits"] = items.size()

        return result
    }

    /*
     * Get value for reserved parameter from query.
     *
     * Query is a Map<String, String[]>, but for reserved parameters,
     * we only allow a single value, so we return the first element of
     * the String[] if found, null otherwise.
     *
     */
    private static String getReservedQueryParameter(String name,
                                                    Map queryParameters) {
        if (name in queryParameters) {
            // For reserved parameters, we assume only one value
            return queryParameters.get(name)[0]
        } else {
            return null
        }
    }

    private static String urlEncode(String input) {
        return java.net.URLEncoder.encode(input, "UTF-8")
    }
}


class Offsets {
    Integer prev
    Integer next
    Integer last

    Offsets(Integer total, Integer limit, Integer offset) {
        this.prev = offset - limit
        if (this.prev < 0) {
            this.prev = null
        }

        this.next = offset + limit
        if (this.next >= total) {
            this.next = null
        } else if (!offset) {
            this.next = limit
        }

        if ((offset + limit) >= total) {
            this.last =  offset
        } else {
            this.last = total - (total % limit)
        }
    }
}

