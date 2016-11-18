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

    Whelk whelk
    Map displayData

    SearchUtils(Whelk whelk) {
        this.whelk = whelk
    }

    void readDisplayData() {
        //Read the display file - should propably not do this here.
        String vocabDisplayUri = "https://id.kb.se/vocab/display"
        Document displayDoc = whelk.storage.locate(vocabDisplayUri,
                                                   true).document
        displayData = displayDoc.data
    }

    Map doSearch(Map queryParameters, String dataset, String siteBaseUri) {
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
            results = findByRelation(relation, reference, limit, offset)
        } else if (relation && value) {
            results = findByValue(relation, value, limit, offset)
        } else if (reference) {
            results = findByQuotation(reference, limit, offset)
        } else if (query) {
            // If general q-parameter chosen, use elastic for query
            if (whelk.elastic) {
                results = queryElasticSearch(queryParameters,
                                             pageParams,
                                             dataset, siteBaseUri,
                                             limit, offset)
            } else {
                throw new WhelkRuntimeException("ElasticSearch not configured.")
            }
        } else {
            // If none of the special query parameters were specified,
            // we query PostgreSQL
            results = queryPostgreSQL(queryParameters, dataset,
                                      limit, offset)
        }

        return results
    }

    private Map findByRelation(String relation,
                               String reference,
                               int limit, int offset) {
        log.debug("Calling findByRelation with p: ${relation} and " +
                  "o: ${reference}")

        List<Document> docs = whelk.storage.findByRelation(relation,
                                                           reference,
                                                           limit, offset)

        return assembleSearchResults(docs)
    }

    private Map findByValue(String relation, String value,
                            int limit, int offset) {
        log.debug("Calling findByValue with p: ${relation} and value: ${value}")

        List<Document> docs = whelk.storage.findByValue(relation, value,
                                                        limit, offset)

        return assembleSearchResults(docs)
    }

    private Map findByQuotation(String identifier, int limit, int offset) {
        log.debug("Calling findByQuotation with o: ${identifier}")

        List<Document> docs = whelk.storage.findByQuotation(identifier,
                                                            limit, offset)

        return assembleSearchResults(docs)
    }

    private Map queryElasticSearch(Map queryParameters,
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
            stats = buildStats(esResult['aggregations'].asMap,
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

    private Map queryPostgreSQL(Map queryParameters, String dataset,
                                int limit, int offset) {
        log.debug("Querying PostgreSQL")

        return whelk.storage.query(queryParameters, dataset,
                                   autoDetectQueryMode(queryParameters))
    }

    /**
     * Create ES filter for specified siteBaseUri.
     *
     */
    Map makeSiteFilter(String siteBaseUri) {
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
    Map buildAggQuery(def tree, int size=1000) {
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
    private Map buildStats(Map aggregations, String baseUrl) {
        Map result = [:]
        result = addSlices([:], aggregations, baseUrl)
        return result
    }

    private Map addSlices(Map stats, Map aggregations, String baseUrl) {
        Map sliceMap = aggregations.inject([:]) { acc, key, aggregation ->
            List observations = []
            Map sliceNode = ['dimension': key.replace('.@id', '')]

            aggregation['buckets'].each { bucket ->
                String itemId = bucket['key']
                String searchPageUrl = "${baseUrl}&${key}=${urlEncode(itemId)}"

                Map observation = ['totalItems': bucket.getAt('docCount'),
                                   'view': ['@id': searchPageUrl],
                                   'object': toChip(lookup(itemId))]

                Map bucketAggs = bucket.getAggregations().asMap

                observation = addSlices(observation, bucketAggs, searchPageUrl)
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
    private Map lookup(String itemId) {
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
    private Map getEntry(Map jsonLd) {
        // FIXME make this less brittle
        return jsonLd['@graph'][0]
    }

    /**
     * Create a URL for '/find' with the specified query parameters.
     *
     */
    String makeFindUrl(Map queryParameters0, int offset=0) {
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
    Tuple2 getLimitAndOffset(Map queryParams) {
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
    private int parseIntFromQueryParams(String key, Map queryParams,
                                        int defaultValue) {
        if (queryParams.containsKey(key)) {
            def value = queryParams.get(key)

            // if someone supplies multiple values, we just pick the
            // first one and discard the rest.
            if (value instanceof List || value instanceof String[]) {
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

    private Map getTermChip(termKey) {
        // FIXME get definition from vocab
        Map termDefinition = [:]
        String id = termDefinition.get('@id')
        if (!id) {
            id = termKey
        }
        // FIXME get label from termDefinition
        return ['@id': id, 'label': '']
    }

    /**
     * Convert a list of posts to cards.
     *
     */
    List toCards(List things){
        return things.collect { toCard(it) }
    }

    /**
     * Convert a post to card.
     *
     */
    Map toCard(Map thing) {
        Map lensGroups = displayData.get("lensGroups")
        Map cardLensGroup = lensGroups.get("cards")

        Map json = removeProperties(thing, cardLensGroup)
        return toChip(json)
    }

    /**
     * Convert a list of posts to chips.
     *
     */
    List toChips(List things) {
        return things.collect { toChip(it) }
    }

    /**
     * Convert a post to chip.
     *
     */
    Map toChip(Map json) {
        Map lensGroups = displayData.get("lensGroups")
        Map chipLensGroup = lensGroups.get("chips")
        Map itemsToKeep = [:]

        json.each { key, value ->
            itemsToKeep[key] = walkThroughData(value, chipLensGroup, true)
        }
        return itemsToKeep
    }

    private Map removeProperties(Map jsonMap, Map lensGroups,
                                 boolean goRecursive=false) {
        Map itemsToKeep = [:]
        Map types = lensGroups.get("lenses")
        Map showPropertiesField = types.get(jsonMap.get("@type"))
        if (jsonMap.get("@type") && types.get(jsonMap.get("@type").toString())) {
            def propertiesToKeep = showPropertiesField.get("showProperties")
            jsonMap.each {key, value ->
                if (key.toString() in propertiesToKeep ||
                    key.toString().startsWith("@")) {
                    if (goRecursive) {
                        itemsToKeep[key] = walkThroughData(value, lensGroups,
                                                           goRecursive)
                    } else {
                        itemsToKeep[key] = value
                    }
                }
            }
            return itemsToKeep
        } else {
            return jsonMap
        }

    }

    private Object walkThroughData(Object o, Map displayData, boolean goRecursive) {
        if(o instanceof Map) {
            return removeProperties(o, displayData, goRecursive)
        } else if (o instanceof List){
            return walkThroughDataFromList(o, displayData, goRecursive)
        } else {
            return o
        }
    }


    private List walkThroughDataFromList(List items, Map displayData,
                                         boolean goRecursive) {
        List result = []
        items.each { item ->
            result << walkThroughData(item, displayData, goRecursive)
        }
        return result
    }

    /*
     * Get mappings and page params for specified query.
     *
     * Reserved parameters or parameters beginning with '_' are
     * filtered out.
     *
     */
    private Tuple2 mapParams(Map params) {
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

    private List getReservedParameters() {
        return ['q', 'p', 'o', 'value', '_limit', '_offset']
    }

    // TODO implement
    /*
     * Assemble PostgreSQL search results.
     *
     */
    private Map assembleSearchResults(List<Document> docs) {
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
    private String getReservedQueryParameter(String name, Map queryParameters) {
        if (name in queryParameters) {
            // For reserved parameters, we assume only one value
            return queryParameters.get(name)[0]
        } else {
            return null
        }
    }

    private String urlEncode(String input) {
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

