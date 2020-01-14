package whelk.rest.api

import com.google.common.escape.Escaper
import com.google.common.net.UrlEscapers
import groovy.transform.PackageScope
import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.exception.InvalidQueryException
import whelk.exception.WhelkRuntimeException
import whelk.search.ESQuery

@Log
class SearchUtils {

    final static int DEFAULT_LIMIT = 200
    final static int MAX_LIMIT = 4000
    final static int DEFAULT_OFFSET = 0

    private static final Escaper QUERY_ESCAPER = UrlEscapers.urlFormParameterEscaper()

    enum SearchType {
        FIND_BY_VALUE,
        FIND_REVERSE,
        ELASTIC,
        POSTGRES
    }

    Whelk whelk
    JsonLd ld
    ESQuery esQuery
    URI vocabUri

    SearchUtils(Whelk whelk) {
        this(whelk.jsonld)
        this.whelk = whelk
        this.esQuery = new ESQuery(whelk)
    }

    SearchUtils(JsonLd jsonld) {
        this.ld = jsonld
        if (ld.vocabId) {
            vocabUri = new URI(ld.vocabId)
        }
    }

    Map doSearch(Map queryParameters, String dataset, JsonLd jsonld) {
        String relation = getReservedQueryParameter('p', queryParameters)
        String object = getReservedQueryParameter('o', queryParameters)
        String value = getReservedQueryParameter('value', queryParameters)
        String query = getReservedQueryParameter('q', queryParameters)
        String sortBy = getReservedQueryParameter('_sort', queryParameters)
        String siteBaseUri = getReservedQueryParameter('_site_base_uri', queryParameters)

        Tuple2 limitAndOffset = getLimitAndOffset(queryParameters)
        int limit = limitAndOffset.first
        int offset = limitAndOffset.second

        if (object && (relation || value || query || sortBy)) {
            throw new InvalidQueryException("Cannot use 'o' together with other search parameters")
        }

        Map results
        if (relation && value) {
            results = findByValue(relation, value, limit, offset)
        } else if (object) {
            results = findReverse(
                    object,
                    getReservedQueryParameter('_lens', queryParameters),
                    limit,
                    offset)
        } else { //assumes elastic query
            // If general q-parameter chosen, use elastic for query
            if (whelk.elastic) {
                Map pageParams = ['p': relation,
                                  'value': value,
                                  'q': query,
                                  '_sort': sortBy,
                                  '_limit': limit]

                results = queryElasticSearch(queryParameters,
                                             pageParams,
                                             dataset, siteBaseUri,
                                             limit, offset, jsonld)
            } else {
                throw new WhelkRuntimeException("ElasticSearch not configured.")
            }
        }

        return results
    }

    private Map findByValue(String relation, String value,
                            int limit, int offset) {
        log.debug("Calling findByValue with p: ${relation} and value: ${value}")

        List<Document> docs = whelk.storage.findByValue(relation, value,
                                                        limit, offset)

        List mappings = []
        mappings << ['variable': 'p',
                     'predicate': ld.toChip(getVocabEntry('predicate')),
                     'value': relation]
        mappings << ['variable': 'value',
                     'predicate': ld.toChip(getVocabEntry('object')),
                     'value': value]

        Map pageParams = ['p': relation, 'value': value,
                          '_limit': limit]

        int total = whelk.storage.countByValue(relation, value)

        List items = docs.collect { ld.toCard(it.data) }

        return assembleSearchResults(SearchType.FIND_BY_VALUE,
                                     items, mappings, pageParams,
                                     limit, offset, total)
    }

    private Map findReverse(String id, String lens, int limit, int offset) {
        lens = lens ? lens : 'cards'
        log.debug("findReverse. o: ${id}, _lens: ${lens}")

        def ids = whelk.findIdsLinkingTo(id)
        int total = ids.size()

        ids = slice(ids, offset, offset+limit)

        List items = whelk.bulkLoad(ids).values()
                .each(whelk.&embellish)
                .collect(SearchUtils.&formatReverseResult)
                .findAll{ !it.isEmpty() }
                .collect{applyLens(it, id, lens)}

        Map pageParams = ['o': id, '_lens': lens, '_limit': limit]

        return assembleSearchResults(SearchType.FIND_REVERSE,
                items, [], pageParams,
                limit, offset, total)
    }

    private static Map formatReverseResult(Document document) {
        document.setThingMeta(document.getCompleteId())
        List<String> thingIds = document.getThingIdentifiers()
        if (thingIds.isEmpty()) {
            log.warn("Missing mainEntity? In: " + document.getCompleteId())
            return [:]
        }
        return JsonLd.frame(thingIds.get(0), document.data)
    }

    private Map applyLens(Map framedThing, String preserveId, String lens) {
        def preservedPaths = JsonLd.findPaths(framedThing, '@id', preserveId)
        return lens == 'chips'
                ? ld.toChip(framedThing, preservedPaths)
                : ld.toCard(framedThing, preservedPaths)
    }

    @PackageScope
    static <T> List<T> slice(List<T> list, int fromIx, int toIx) {
        if (fromIx > list.size() || fromIx > toIx) {
            return []
        }
        return list[(Math.max(0,fromIx)..<Math.min(list.size(), toIx))]
    }

    private Map queryElasticSearch(Map queryParameters,
                                   Map pageParams,
                                   String dataset, String siteBaseUri,
                                   int limit, int offset, JsonLd jsonld) {
        String query = pageParams['q']
        log.debug("Querying ElasticSearch")

        // SearchUtils may overwrite the `_limit` query param, and since it's
        // used for the other searches we overwrite limit here, so we keep it
        // consistent across search paths
        //
        // TODO Only manipulate `_limit` in one place
        queryParameters['_limit'] = [limit.toString()]

        Map esResult = esQuery.doQuery(queryParameters, dataset)

        List<Map> mappings = []
        mappings << ['variable': 'q',
                     'predicate': ld.toChip(getVocabEntry('textQuery')),
                     'value': query]

        Tuple2 mappingsAndPageParams = mapParams(queryParameters)
        mappings.addAll(mappingsAndPageParams.first)
        pageParams << mappingsAndPageParams.second

        int total = 0
        if (esResult['totalHits']) {
            total = esResult['totalHits']
        }

        List items = []
        if (esResult['items']) {
            items = esResult['items'].collect {
                def item = ld.toCard(it)
                item['reverseLinks'] = [
                        (JsonLd.TYPE_KEY) : 'PartialCollectionView',
                        (JsonLd.ID_KEY) : URLEncoder.encode(Document.getBASE_URI().resolve('find?o=' + it['@id']).toString()),
                        'totalItems' : it['meta']['linksHereCount']
                ]
                return item
            }
        }

        Map stats = buildStats(esResult['aggregations'],
                           makeFindUrl(SearchType.ELASTIC, stripNonStatsParams(pageParams)))
        if (!stats) {
            log.debug("No stats found for query: ${queryParameters}, result: ${esResult}")
        }

        mappings.tail().each { Map mapping ->
            Map params = removeMappingFromParams(pageParams, mapping)
            String upUrl = makeFindUrl(SearchType.ELASTIC, params, offset)
            mapping['up'] = [ (JsonLd.ID_KEY): upUrl ]
        }

        Map result = assembleSearchResults(SearchType.ELASTIC,
                                           items, mappings, pageParams,
                                           limit, offset, total)

        if (stats) {
            result['stats'] = stats
        }

        if (esResult['_debug']) {
            result['_debug'] = esResult['_debug']
        }

        return result
    }

    Map removeMappingFromParams(Map pageParams, Map mapping) {
        Map params = pageParams.clone()
        String variable = mapping['variable']
        def param = params[variable]
        List values = param instanceof List ? param.clone() : param ? [param] : []
        if ('object' in mapping) {
            def value = mapping.object[JsonLd.ID_KEY]
            values.remove(value)
        } else if ('value' in mapping) {
            def value = mapping.value
            values.remove(value)
        }
        if (!values) {
            params.remove(variable)
        } else {
            params[variable] = values
        }
        return params
    }

    /*
     * Return a map without helper params, useful for facet links.
     */
    private Map stripNonStatsParams(Map incoming) {
        Map result = [:]
        List reserved = getReservedAuxParameters()
        incoming.each { k, v ->
            if (!reserved.contains(k)) {
                result[k] = v
            }
        }
        return result
    }

    private Map assembleSearchResults(SearchType st, List items,
                                      List mappings, Map pageParams,
                                      int limit, int offset, int total) {
        Map result = [(JsonLd.TYPE_KEY): 'PartialCollectionView']
        result[(JsonLd.ID_KEY)] = makeFindUrl(st, pageParams, offset)
        result['itemOffset'] = offset
        result['itemsPerPage'] = limit
        result['totalItems'] = total

        result['search'] = ['mapping': mappings]

        Map paginationLinks = makePaginationLinks(st, pageParams, limit,
                                                  offset, total)
        result << paginationLinks

        result['items'] = items

        return result
    }

    /**
     * Create ES filter for specified siteBaseUri.
     *
     */
    Map makeSiteFilter(String siteBaseUri) {
        return ['should': [
                   ['prefix': [(JsonLd.ID_KEY): siteBaseUri]],
                    // ideally, we'd use ID_KEY here too, but that
                    // breaks the test case :/
                   ['prefix': ['sameAs.@id': siteBaseUri]]
                ],
                'minimum_should_match': 1]
    }

    /**
     * Build the term aggregation part of an ES query.
     *
     */
    Map buildAggQuery(def tree, int size=10) {
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
            String sort = tree[key]?.sort =='key' ? '_term' : '_count'
            def sortOrder = tree[key]?.sortOrder =='asc' ? 'asc' : 'desc'
            query[key] = ['terms': [
                    'field': key,
                    'size': tree[key]?.size ?: size,
                    'order': [(sort):sortOrder]]]
            if (tree[key].subItems instanceof Map) {
                query[key]['aggs'] = buildAggQuery(tree[key].subItems, size)
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
            Map sliceNode = ['dimension': key.replace(".${JsonLd.ID_KEY}", '')]
            aggregation['buckets'].each { bucket ->
                String itemId = bucket['key']
                String searchPageUrl = "${baseUrl}&${makeParam(key, itemId)}"

                Map observation = ['totalItems': bucket.getAt('doc_count'),
                                   'view': [(JsonLd.ID_KEY): searchPageUrl],
                                   'object': ld.toChip(lookup(itemId))]

                /*Map bucketAggs = bucket.getAggregations().asMap

                observation = addSlices(observation, bucketAggs, searchPageUrl)*/
                observations << observation
            }

            if (observations) {
                sliceNode['observation'] = observations
                acc[key] = sliceNode
            }

            return acc
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
            return [(JsonLd.ID_KEY): itemId, 'label': itemId]
        }
    }

    /*
     * Read vocab term data from storage.
     *
     * Returns null if not found.
     *
     */
    private Map getVocabEntry(String id) {
        def termKey = ld.toTermKey(id)
        if (termKey in ld.vocabIndex) {
            return ld.vocabIndex[termKey]
        }
        String fullId
        try {
            if (vocabUri) {
                fullId = vocabUri.resolve(id).toString()
            }
        }
        catch (IllegalArgumentException e) {
            // Couldn't resolve, which means id isn't a valid IRI.
            // No need to check the db.
            return null
        }
        Document doc = whelk.storage.getDocumentByIri(fullId)

        if (doc) {
            return getEntry(doc.data, fullId)
        } else {
            return null
        }
    }

    // FIXME move to Document or JsonLd
    private Map getEntry(Map jsonLd, String entryId) {
        // we rely on this convention for the time being.
        return jsonLd[(JsonLd.GRAPH_KEY)].find { it[JsonLd.ID_KEY] == entryId }
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
                    params << makeParam(k, value)
                }
            } else {
                params << makeParam(k, v)
            }
        }
        if (offset > 0) {
            params << makeParam("_offset", offset)
        }
        return "/find?${params.join('&')}"
    }

    private String makeParam(key, value) {
        return "${escapeQueryParam(key)}=${escapeQueryParam(value)}"
    }

    private Tuple2 getInitialParamsAndKeys(SearchType st,
                                           Map queryParameters0) {
        Map queryParameters = queryParameters0.clone()
        Tuple2 result
        switch (st) {
            case SearchType.FIND_BY_VALUE:
                result = getValueParams(queryParameters)
                break
            case SearchType.FIND_REVERSE:
                result = getReverseParams(queryParameters)
                break
            case SearchType.ELASTIC:
                result = getElasticParams(queryParameters)
                break
        }
        return result
    }

    private Tuple2 getValueParams(Map queryParameters) {
        String relation = queryParameters.remove('p')
        String value = queryParameters.remove('value')
        List initialParams = [makeParam('p', relation), makeParam('value', value)]
        List keys = (queryParameters.keySet() as List).sort()

        return new Tuple2(initialParams, keys)
    }

    private Tuple2 getReverseParams(Map queryParameters) {
        String id = queryParameters.remove('o')
        String lens = queryParameters.remove('_lens')
        List initialParams = [makeParam('o', id), makeParam('_lens', lens)]
        List keys = (queryParameters.keySet() as List).sort()

        return new Tuple2(initialParams, keys)
    }

    private Tuple2 getElasticParams(Map queryParameters) {
        if (!('q' in queryParameters)) {
            queryParameters['q'] = '*'
        }

        String query = queryParameters.remove('q')
        List initialParams = [makeParam('q', query)]
        List keys = (queryParameters.keySet() as List).sort()

        return new Tuple2(initialParams, keys)
    }

    Map makePaginationLinks(SearchType st, Map pageParams,
                            int limit, int offset, int total) {
        if (limit == 0) {
            // we don't have anything to paginate over
            return [:]
        }

        Map result = [:]

        Offsets offsets = new Offsets(total, limit, offset)

        result['first'] = [(JsonLd.ID_KEY): makeFindUrl(st, pageParams)]
        result['last'] = [(JsonLd.ID_KEY): makeFindUrl(st, pageParams, offsets.last)]

        if (offsets.prev != null) {
            if (offsets.prev == 0) {
                result['previous'] = result['first']
            } else {
                result['previous'] = [(JsonLd.ID_KEY): makeFindUrl(st, pageParams,
                                                                   offsets.prev)]
            }
        }

        if (offsets.next) {
            result['next'] = [(JsonLd.ID_KEY): makeFindUrl(st, pageParams,
                                                           offsets.next)]
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

        if (limit < 0) {
            throw new InvalidQueryException(
                "\"_limit\" query parameter can't be negative."
            )
        }

        int offset = parseIntFromQueryParams("_offset", queryParams,
                                             DEFAULT_OFFSET)

        if (offset < 0) {
            throw new InvalidQueryException(
                "\"_offset\" query parameter can't be negative."
            )
        }

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
                if (param == JsonLd.TYPE_KEY || param == JsonLd.ID_KEY) {
                    valueProp = 'object'
                    termKey = param
                    value = [(JsonLd.ID_KEY): val]
                } else if (param.endsWith(".${JsonLd.ID_KEY}")) {
                    valueProp = 'object'
                    termKey = param[0..-5]
                    value = [(JsonLd.ID_KEY): val]
                } else {
                    valueProp = 'value'
                    termKey = param
                    value = val
                }

                Map termChip
                Map termDef = getVocabEntry(termKey)
                if (termDef) {
                    termChip = ld.toChip(termDef)
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

    /*
     * Return a list of reserved query params
     */
    private List getReservedParameters() {
        return ['q', 'p', 'o', 'value'] + getReservedAuxParameters()
    }

    /*
     * Return a list of reserved helper params
     */
    private List getReservedAuxParameters() {
        return ['_limit', '_offset']
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

    private Object escapeQueryParam(Object input) {
        return input instanceof String
                // We want pretty URIs, restore some characters which are inside query strings
                // https://tools.ietf.org/html/rfc3986#section-3.4
                ? QUERY_ESCAPER.escape(input).replace(['%3A': ':', '%2F': '/', '%40': '@'])
                : input
    }
}

class Offsets {
    Integer prev
    Integer next
    Integer last

    Offsets(Integer total, Integer limit, Integer offset) {
        if (limit <= 0) {
            throw new InvalidQueryException(
                "\"limit\" must be greater than 0."
            )
        }

        if (offset < 0) {
            throw new InvalidQueryException("\"offset\" can't be negative.")
        }

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
            this.last = offset
        } else {
            if (total % limit == 0) {
                this.last = total - limit
            } else {
                this.last = total - (total % limit)
            }
        }
    }
}

