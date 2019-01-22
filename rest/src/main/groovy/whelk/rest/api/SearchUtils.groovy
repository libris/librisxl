package whelk.rest.api

import groovy.util.logging.Log4j2 as Log

import whelk.Document
import whelk.JsonLd
import whelk.Location
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.component.StorageType
import whelk.exception.WhelkRuntimeException
import whelk.exception.InvalidQueryException

@Log
class SearchUtils {

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
    JsonLd ld
    URI vocabUri

    SearchUtils(Whelk whelk) {
        this(whelk.jsonld)
        this.whelk = whelk
    }

    SearchUtils(JsonLd jsonld) {
        this.ld = jsonld
        if (ld.vocabId) {
            vocabUri = new URI(ld.vocabId)
        }
    }

    Map doSearch(Map queryParameters, String dataset, JsonLd jsonld) {
        String relation = getReservedQueryParameter('p', queryParameters)
        String reference = getReservedQueryParameter('o', queryParameters)
        String value = getReservedQueryParameter('value', queryParameters)
        String query = getReservedQueryParameter('q', queryParameters)
        String sortBy = getReservedQueryParameter('_sort', queryParameters)
        String siteBaseUri = getReservedQueryParameter('_site_base_uri', queryParameters)

        Tuple2 limitAndOffset = getLimitAndOffset(queryParameters)
        int limit = limitAndOffset.first
        int offset = limitAndOffset.second

        Map pageParams = ['p': relation,
                          'o': reference,
                          'value': value,
                          'q': query,
                          '_sort': sortBy,
                          '_limit': limit]

        Map results = null

        if (relation && reference) {
            results = findByRelation(relation, reference, limit, offset)
        } else if (relation && value) {
            results = findByValue(relation, value, limit, offset)
        } else if (reference) {
            results = findByQuotation(reference, limit, offset)
        } else { //assumes elastic query
            // If general q-parameter chosen, use elastic for query
            if (whelk.elastic) {
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
                     'predicate': ld.toChip(getVocabEntry('predicate')),
                     'value': relation]
        mappings << ['variable': 'o',
                     'predicate': ld.toChip(getVocabEntry('object')),
                     'value': reference]

        Map pageParams = ['p': relation, 'o': reference,
                          '_limit': limit]

        int total = whelk.storage.countByRelation(relation, reference)

        List items = docs.collect { ld.toCard(it.data) }

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

    private Map findByQuotation(String identifier, int limit, int offset) {
        log.debug("Calling findByQuotation with o: ${identifier}")

        List<Document> docs = whelk.storage.findByQuotation(identifier,
                                                            limit, offset)

        List mappings = []
        mappings << ['variable': 'o',
                     'predicate': ld.toChip(getVocabEntry('object')),
                     'value': identifier]

        Map pageParams = ['o': identifier, '_limit': limit]

        int total = whelk.storage.countByQuotation(identifier)

        List items = docs.collect { ld.toCard(it.data) }

        return assembleSearchResults(SearchType.FIND_BY_QUOTATION,
                                     items, mappings, pageParams,
                                     limit, offset, total)
    }

    private Map queryElasticSearch(Map queryParameters,
                                   Map pageParams,
                                   String dataset, String siteBaseUri,
                                   int limit, int offset, JsonLd jsonld) {
        String query = pageParams['q']
        String sortBy = pageParams['_sort']
        log.debug("Querying ElasticSearch")

        // Filter out all @types that have (more specific) subclasses that are also in the list
        // So for example [Instance, Electronic] should be reduced to just [Electronic].
        // Afterwards, include all subclasses of the remaining @types
        String[] types = queryParameters.get('@type')
        if (types != null) {
            // Select types to prune
            Set<String> toBeRemoved = []
            for (String c1 : types) {
                ArrayList<String> c1SuperClasses = []
                jsonld.getSuperClasses(c1, c1SuperClasses)
                toBeRemoved.addAll(c1SuperClasses)
            }
            // Make a new pruned list without the undesired superclasses
            List<String> prunedTypes = []
            for (String type : types) {
                if (!toBeRemoved.contains(type))
                    prunedTypes.add(type)
            }
            // Add all subclasses of the remaining types
            ArrayList<String> subClasses = []
            for (String type : prunedTypes) {
                jsonld.getSubClasses(type, subClasses)
                subClasses.add(type)
            }

            queryParameters.put('@type', (String[]) subClasses.toArray())
        }

        Map stats = null
        List mappings = []
        mappings << ['variable': 'q',
                     'predicate': ld.toChip(getVocabEntry('textQuery')),
                     'value': query]

        def dslQuery = ElasticSearch.createJsonDsl(queryParameters,
                                                   limit, offset, sortBy)

        // If there was an @type parameter, all subclasses of that type were added as well,
        // let's clean that up and "hide" it from the caller.
        if (types != null) {
            queryParameters.put('@type', types)
        }

        Tuple2 mappingsAndPageParams = mapParams(queryParameters)
        mappings.addAll(mappingsAndPageParams.first)
        pageParams << mappingsAndPageParams.second

        if (siteBaseUri) {
            dslQuery['query']['bool'] << makeSiteFilter(siteBaseUri)
        }

        // TODO: statsTree may depend on site ({id,libris}.kb.se)
        String[] statsRepr = queryParameters.get('_statsrepr')
        Map statsTree = statsRepr ? JsonLd.mapper.readValue(statsRepr[0], Map)
                        : [(JsonLd.TYPE_KEY): []]

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
            items = esResult['items'].collect { ld.toCard(it) }
        }

        //items = embellishItems(items)
        if (statsTree) {
            stats = buildStats(esResult['aggregations'],
                               makeFindUrl(SearchType.ELASTIC, stripNonStatsParams(pageParams)))
            if (!stats) {
                log.debug("No stats found for query: ${dslQuery}, result: ${esResult}")
            }
        }

        mappings.tail().each { mapping ->
            Map params = pageParams.clone()
            params.remove(mapping['variable'])
            mapping['up'] = [(JsonLd.ID_KEY): makeFindUrl(SearchType.ELASTIC,
                                                          params, offset)]
        }

        Map result = assembleSearchResults(SearchType.ELASTIC,
                                           items, mappings, pageParams,
                                           limit, offset, total)

        if (stats) {
            result['stats'] = stats
        }

        return result
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

    private List embellishItems(List items) {
        List embellished = []

        for (item in items) {
            Map graph = JsonLd.flatten(item)
            List refs = JsonLd.getExternalReferences(graph)
            List ids = ld.expandLinks(refs)
            Map recordMap = whelk.bulkLoad(ids).collectEntries { id, doc -> [id, doc.data] }
            ld.embellish(graph, recordMap)

            embellished << JsonLd.frame(item[JsonLd.ID_KEY], null, graph, true)
        }

        return embellished
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
                String searchPageUrl = "${baseUrl}&${key}=${urlEncode(itemId)}"

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
        catch (java.lang.IllegalArgumentException e) {
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

    private String urlEncode(String input) {
        return java.net.URLEncoder.encode(input, "UTF-8")
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
            this.last =  offset
        } else {
            this.last = total - (total % limit)
        }
    }
}

