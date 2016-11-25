package whelk.rest.api

import groovy.util.logging.Slf4j as Log

import whelk.Document
import whelk.JsonLd
import whelk.Location
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.StorageType
import whelk.exception.WhelkRuntimeException

@Log
class SearchUtils {

    final static URI VOCAB_BASE_URI = new URI("https://id.kb.se/vocab/")

    final static int DEFAULT_LIMIT = 200
    final static int MAX_LIMIT = 4000
    final static int DEFAULT_OFFSET = 0

    enum SearchType {
        FIND_BY_RELATION,
        FIND_BY_VALUE,
        FIND_BY_QUOTATION,
        ELASTIC,
        POSTGRES
    }

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
        List mappings = []
        mappings << ['variable': 'p',
                     'predicate': toChip(getVocabEntry('predicate')),
                     'value': relation]
        mappings << ['variable': 'o',
                     'predicate': toChip(getVocabEntry('object')),
                     'value': reference]

        Map pageParams = ['p': relation, 'o': reference,
                          '_limit': limit]

        int total = whelk.storage.countByRelation(relation, reference)

        List items = toCards(docs.collect { it.data })

        return assembleSearchResults(SearchType.FIND_BY_RELATION,
                                     items, mappings, pageParams,
                                     limit, offset, total)
    }

    private Map findByValue(String relation, String value,
                            int limit, int offset) {
        log.debug("Calling findByValue with p: ${relation} and value: ${value}")

        List<Document> docs = whelk.storage.findByValue(relation, value,
                                                        limit, offset)

        List mappings = []
        mappings << ['variable': 'p',
                     'predicate': toChip(getVocabEntry('predicate')),
                     'value': relation]
        mappings << ['variable': 'value',
                     'predicate': toChip(getVocabEntry('object')),
                     'value': value]

        Map pageParams = ['p': relation, 'value': value,
                          '_limit': limit]

        int total = whelk.storage.countByValue(relation, value)

        List items = toCards(docs.collect { it.data })

        return assembleSearchResults(SearchType.FIND_BY_VALUE,
                                     items, mappings, pageParams,
                                     limit, offset, total)
    }

    private Map findByQuotation(String identifier, int limit, int offset) {
        log.debug("Calling findByQuotation with o: ${identifier}")

        List<Document> docs = whelk.storage.findByQuotation(identifier,
                                                            limit, offset)

        List mappings = []
        mappings << ['variable': 'o',
                     'predicate': toChip(getVocabEntry('object')),
                     'value': identifier]

        Map pageParams = ['o': identifier, '_limit': limit]

        int total = whelk.storage.countByQuotation(identifier)

        List items = toCards(docs.collect { it.data })

        return assembleSearchResults(SearchType.FIND_BY_QUOTATION,
                                     items, mappings, pageParams,
                                     limit, offset, total)
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
                     'predicate': toChip(getVocabEntry('textQuery')),
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
                               makeFindUrl(SearchType.ELASTIC, pageParams))
        }

        mappings.tail().each { mapping ->
            Map params = pageParams.clone()
            params.remove(mapping['variable'])
            mapping['up'] = ['@id': makeFindUrl(SearchType.ELASTIC, params,
                                                offset)]
        }

        Map result = assembleSearchResults(SearchType.ELASTIC,
                                           items, mappings, pageParams,
                                           limit, offset, total)

        if (stats) {
            result['stats'] = stats
        }

        return result
    }

    private Map assembleSearchResults(SearchType st, List items,
                                      List mappings, Map pageParams,
                                      int limit, int offset, int total) {
        Map result = ['@type': 'PartialCollectionView']
        result['@id'] = makeFindUrl(st, pageParams, offset)
        result['itemOffset'] = offset
        result['totalItems'] = total

        result['search'] = ['mapping': mappings]

        Map paginationLinks = makePaginationLinks(st, pageParams, limit,
                                                  offset, total)
        result << paginationLinks

        result['items'] = items

        return result
    }

    private Map queryPostgreSQL(Map queryParameters, String dataset,
                                int limit, int offset) {
        log.debug("Querying PostgreSQL")

        return whelk.storage.query(queryParameters, dataset,
                                   autoDetectQueryMode(queryParameters))
    }

    StorageType autoDetectQueryMode(Map queries) {
        boolean probablyMarcQuery = false
        for (entry in queries) {
            if (entry.key ==~ /\d{3}\.{0,1}\w{0,1}/) {
                probablyMarcQuery = true
            } else if (!entry.key.startsWith("_")) {
                probablyMarcQuery = false
            }
        }

        if (probablyMarcQuery) {
            return StorageType.MARC21_JSON
        } else {
            return StorageType.JSONLD_FLAT_WITH_DESCRIPTIONS
        }
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
        Map entry = getVocabEntry(itemId)

        if (entry) {
            return entry
        } else {
            return ['@id': itemId, 'label': itemId]
        }
    }

    /*
     * Read vocab term data from storage.
     *
     * Returns null if not found.
     *
     */
    private Map getVocabEntry(String id) {
        String fullId = VOCAB_BASE_URI.resolve(id).toString()
        Location loc = whelk.storage.locate(fullId, true)
        Document doc = loc?.document

        if (doc) {
            return getEntry(doc.data)
        } else {
            return null
        }
    }

    // FIXME move to Document or JsonLd
    private Map getEntry(Map jsonLd) {
        // we rely on this convention for the time being.
        return jsonLd['@graph'][0]
    }

    /**
     * Create a URL for '/find' with the specified query parameters.
     *
     */
    String makeFindUrl(SearchType st, Map queryParameters, int offset=0) {
        Tuple2 initial = getInitialParamsAndKeys(st, queryParameters)
        List params = initial.first
        List keys = initial.second
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

    private Tuple2 getInitialParamsAndKeys(SearchType st,
                                           Map queryParameters0) {
        Map queryParameters = queryParameters0.clone()
        Tuple2 result
        switch (st) {
            case SearchType.FIND_BY_RELATION:
                result = getRelationParams(queryParameters)
                break
            case SearchType.FIND_BY_VALUE:
                result = getValueParams(queryParameters)
                break
            case SearchType.FIND_BY_QUOTATION:
                result = getQuotationParams(queryParameters)
                break
            case SearchType.ELASTIC:
                result = getElasticParams(queryParameters)
                break
        }
        return result
    }

    private Tuple2 getRelationParams(Map queryParameters) {
        String relation = queryParameters.remove('p')
        String reference = queryParameters.remove('o')
        List initialParams = ["p=${relation}", "o=${reference}"]
        List keys = (queryParameters.keySet() as List).sort()

        return new Tuple2(initialParams, keys)
    }

    private Tuple2 getValueParams(Map queryParameters) {
        String relation = queryParameters.remove('p')
        String value = queryParameters.remove('value')
        List initialParams = ["p=${relation}", "value=${value}"]
        List keys = (queryParameters.keySet() as List).sort()

        return new Tuple2(initialParams, keys)
    }

    private Tuple2 getQuotationParams(Map queryParameters) {
        String reference = queryParameters.remove('o')
        List initialParams = ["o=${reference}"]
        List keys = (queryParameters.keySet() as List).sort()

        return new Tuple2(initialParams, keys)
    }

    private Tuple2 getElasticParams(Map queryParameters) {
        if (!('q' in queryParameters)) {
            queryParameters['q'] = '*'
        }

        String query = queryParameters.remove('q')
        List initialParams = ["q=${query}"]
        List keys = (queryParameters.keySet() as List).sort()

        return new Tuple2(initialParams, keys)
    }

    Map makePaginationLinks(SearchType st, Map pageParams,
                            int limit, int offset, int total) {
        Map result = [:]
        Offsets offsets = new Offsets(total, limit, offset)

        result['first'] = ['@id': makeFindUrl(st, pageParams)]
        result['last'] = ['@id': makeFindUrl(st, pageParams, offsets.last)]

        if (offsets.prev) {
            if (offsets.prev == 0) {
                result['previous'] = result['first']
            } else {
                result['previous'] = ['@id': makeFindUrl(st, pageParams,
                                                         offsets.prev)]
            }
        }

        if (offsets.next) {
            result['next'] = ['@id': makeFindUrl(st, pageParams, offsets.next)]
        }

        return result
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
        Map result = [:]

        Map card = removeProperties(thing, cardLensGroup)
        card.each {key, value ->
            result[key] = toChip(value)
        }
        return result
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
    Object toChip(Object object) {
        Map lensGroups = displayData.get("lensGroups")
        Map chipLensGroup = lensGroups.get("chips")
        Map itemsToKeep = [:]
        Map result = [:]

        if (object instanceof List){
            return toChips(object)
        } else if ((object instanceof Map)) {
            itemsToKeep = removeProperties(object, chipLensGroup)
            itemsToKeep.each {key, value ->
                result[key] = toChip(value)
            }
            return result
        } else {
            return object
        }
    }

    private Map removeProperties(Map jsonMap, Map lensGroups) {
        Map itemsToKeep = [:]
        Map types = lensGroups.get("lenses")
        String type = jsonMap.get("@type")

        if (!type) {
            return jsonMap
        }

        Map showPropertiesField = types.get(type)

        if (showPropertiesField) {
            List propertiesToKeep = showPropertiesField.get("showProperties")

            jsonMap.each {key, value ->
                if (shouldKeep(key, propertiesToKeep)) {
                    itemsToKeep[key] = value
                }
            }
            return itemsToKeep
        } else {
            return jsonMap
        }
    }

    private boolean shouldKeep(String key, List propertiesToKeep) {
        return (key in propertiesToKeep || key.startsWith("@"))
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

                Map termChip
                Map termDef = getVocabEntry(termKey)
                if (termDef) {
                  termChip = toChip(termDef)
                }

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

