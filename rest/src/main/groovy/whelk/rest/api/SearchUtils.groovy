package whelk.rest.api

import com.google.common.collect.ArrayListMultimap
import com.google.common.collect.Multimap
import com.google.common.escape.Escaper
import com.google.common.net.UrlEscapers
import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.exception.InvalidQueryException
import whelk.exception.WhelkRuntimeException
import whelk.search.ESQuery
import whelk.search.ElasticFind
import whelk.search.RangeParameterPrefix
import whelk.util.DocumentUtil

import static whelk.search.ESQuery.Connective.AND
import static whelk.search.ESQuery.Connective.OR
import static whelk.util.Unicode.stripPrefix

@Log
class SearchUtils {
    final static int DEFAULT_LIMIT = 200
    final static int MAX_LIMIT = 4000
    final static int DEFAULT_OFFSET = 0

    final static String MATCHES_PROP = 'matchesTransitive'

    private static final Escaper QUERY_ESCAPER = UrlEscapers.urlFormParameterEscaper()

    enum SearchType {
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

    Map doSearch(Map queryParameters) {
        if (!whelk.elastic) {
            throw new WhelkRuntimeException("ElasticSearch not configured.")
        }

        List predicates = queryParameters['p']
        String object = getReservedQueryParameter('o', queryParameters)
        String value = getReservedQueryParameter('value', queryParameters)
        String query = getReservedQueryParameter('q', queryParameters)
        String sortBy = getReservedQueryParameter('_sort', queryParameters)
        String lens = getReservedQueryParameter('_lens', queryParameters)
        String addStats = getReservedQueryParameter('_stats', queryParameters)
        String suggest = getReservedQueryParameter('_suggest', queryParameters)
        String spell = getReservedQueryParameter('_spell', queryParameters)
        String computedLabel = getReservedQueryParameter('_computedLabel', queryParameters)

        if (queryParameters['p'] && !object) {
            throw new InvalidQueryException("Parameter 'p' can only be used together with 'o'")
        }

        if (suggest && lens != 'chips') {
            throw new InvalidQueryException("Parameter '_suggest' can only be used when '_lens' is set to 'chips'")
        }

        Tuple2 limitAndOffset = getLimitAndOffset(queryParameters)
        int limit = limitAndOffset.v1
        int offset = limitAndOffset.v2

        Map pageParams = ['p'     : predicates,
                          'value' : value,
                          'q'     : query,
                          'o'     : object,
                          '_sort' : sortBy,
                          '_limit': limit,
                          '_lens' : lens,
                          '_stats' : addStats,
                          '_suggest' : suggest,
                          '_spell': spell,
                          '_computedLabel': computedLabel,

        ]

        Map results = queryElasticSearch(
                queryParameters,
                pageParams,
                limit,
                offset,
                lens)

        return results
    }

    private Map applyLens(Map framedThing, String lens, String preserveId = null) {
        Set<String> preserveLinks = preserveId ? [preserveId] : []

        switch (lens) {
            case 'chips':
                return ld.toChip(framedThing, preserveLinks)
            case 'full':
                return removeSystemInternalProperties(framedThing)
            default:
                return ld.toCard(framedThing, false, false, false, preserveLinks, true)
        }
    }

    private Map queryElasticSearch(Map queryParameters,
                                   Map pageParams,
                                   int limit,
                                   int offset,
                                   String lens) {
        String query = pageParams['q']
        String reverseObject = pageParams['o']
        List<String> predicates = pageParams['p']
        String addStats = pageParams['_stats']
        String suggest = pageParams['_suggest']
        String spell = pageParams['_spell']
        lens = lens ?: 'cards'

        log.debug("Querying ElasticSearch")

        // SearchUtils may overwrite the `_limit` query param, and since it's
        // used for the other searches we overwrite limit here, so we keep it
        // consistent across search paths
        //
        // TODO Only manipulate `_limit` in one place
        queryParameters['_limit'] = [limit.toString()]

        Map esResult = esQuery.doQuery(queryParameters, suggest, spell)
        Lookup lookup = new Lookup()
        
        List<Map> mappings = []
        if (query) {
            mappings << ['variable' : 'q',
                         'predicate': lookup.chip('textQuery'),
                         'value'    : query]
        }

        def multiSelectable = ESQuery.multiSelectFacets(queryParameters)
        Tuple2 mappingsAndPageParams = mapParams(lookup, queryParameters, multiSelectable)
        mappings.addAll(mappingsAndPageParams.v1)
        pageParams << mappingsAndPageParams.v2

        int total = 0
        if (esResult['totalHits']) {
            total = esResult['totalHits']
        }

        List items = []
        if (esResult['items']) {
            items = esResult['items'].collect {
                def item = applyLens(it, lens, reverseObject)
                
                // ISNIs and ORCIDs are indexed with and without spaces, remove the one with spaces.
                item.identifiedBy?.with { List ids -> ids.removeAll { (Document.isIsni(it) || Document.isOrcid(it) ) && it.value?.size() == 16+3 } }
                
                // This object must be re-added because it might get filtered out in applyLens().
                item['reverseLinks'] = it['reverseLinks']
                if (item['reverseLinks'] != null)
                    item['reverseLinks'][JsonLd.ID_KEY] = Document.getBASE_URI().resolve('find?o=' + URLEncoder.encode(it['@id'], 'UTF-8').toString()).toString()
                return item
            }
        }

        def aggregations = ((Map) esResult['aggregations'])
        def selectedFacets = ((Map<String,?>) mappingsAndPageParams.v2)

        // Filter out already selected facets (if not in multi-select group)
        selectedFacets.each { k, v ->
            if (k in multiSelectable) {
                return
            }
            k = stripPrefix((String) k, ESQuery.AND_PREFIX)
            k = stripPrefix((String) k, ESQuery.OR_PREFIX)
            ((List) aggregations[k]?['buckets'])?.removeIf { it['key'] in v }
        } 

        Map stats = null
        if ((addStats == null || (addStats == 'true' || addStats == 'on')) && spell != "only") {
            stats = buildStats(lookup, aggregations,
                               makeFindUrl(SearchType.ELASTIC, stripNonStatsParams(pageParams)),
                               (total > 0 && !predicates) ? reverseObject : null,
                                multiSelectable.collectEntries{ [(it) : selectedFacets[it] ?: []] } as Map<String, List>)
        }
        if (!stats) {
            log.debug("No stats found for query: ${queryParameters}")
        }

        (query ? mappings.tail() : mappings).each { Map mapping ->
            Map params = removeMappingFromParams(pageParams, mapping)
            String upUrl = makeFindUrl(SearchType.ELASTIC, params, offset)
            mapping['up'] = [ (JsonLd.ID_KEY): upUrl ]
        }

        if (reverseObject) {
            String upUrl = makeFindUrl(SearchType.ELASTIC, pageParams - ['o' : pageParams['o'], 'p' : pageParams['p']], offset)
            mappings << [
                    'variable' : 'o',
                    'object'   : lookup.chip(reverseObject),  // TODO: object/predicate/???
                    'up'       : [(JsonLd.ID_KEY): upUrl],
            ]
        }

        if (reverseObject && predicates) {
            String upUrl = makeFindUrl(SearchType.ELASTIC, pageParams - ['p' : pageParams['p']], offset)
            mappings << [
                    'variable' : 'p',
                    'object'   : reverseObject,
                    'predicate': lookup.chip(predicates.first()),
                    'up'       : [(JsonLd.ID_KEY): upUrl],
            ]
        }
        
        Map result = assembleSearchResults(SearchType.ELASTIC,
                                           items, mappings, pageParams,
                                           limit, offset, total)

        if (stats && (!suggest || spell != "only")) {
            stats[JsonLd.ID_KEY] = '#stats'
            result['stats'] = stats
        }

        if (spell) {
            result['_spell'] = []
            esResult["spell"].each { Map suggestion ->
                result['_spell'] << [
                        'label': suggestion['text'],
                        'labelHTML': suggestion['highlighted'],
                        'view': [
                                '@id': makeFindUrl(
                                        SearchType.ELASTIC,
                                        pageParams - ['q': pageParams['q']] + ['q': suggestion['text']],
                                        offset)
                        ]
                ]
            }
        }

        if (esResult['_debug']) {
            result['_debug'] = esResult['_debug']
        }

        result['maxItems'] = esQuery.getMaxItems().toString()

        lookup.run()
        
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

    Map removeSystemInternalProperties(Map framedThing) {
        DocumentUtil.traverse(framedThing) { value, path ->
            if (path && ((String) path.last()).startsWith('_')) {
                return new DocumentUtil.Remove()
            }
        }
        return framedThing
    }

    /*
     * Return a map without helper params, useful for facet links.
     */
    private Map stripNonStatsParams(Map incoming) {
        Map result = [:]
        List reserved = ['_offset']
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
                                                  offset, Math.min(total, esQuery.getMaxItems()))
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
            String sort = tree[key]?.sort =='key' ? '_key' : '_count'
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
    private Map buildStats(Lookup lookup, Map aggregations, String baseUrl, String reverseObject, Map<String, List> multiSelected) {
        return addSlices(lookup, [:], aggregations, baseUrl, reverseObject, multiSelected)
    }

    private Map addSlices(Lookup lookup, Map stats, Map aggregations, String baseUrl, String reverseObject, Map<String, List> multiSelected) {
        Map sliceMap = aggregations.inject([:]) { acc, key, aggregation ->
            String baseUrlForKey = removeWildcardForKey(baseUrl, key)
            List observations = []
            Map sliceNode = ['dimension': key]
            sliceNode['dimensionChain'] = makeDimensionChain(key)

            aggregation['buckets'].each { bucket ->
                String itemId = bucket['key']

                if (key in multiSelected.keySet()) {
                    String param = makeParam(key, itemId)
                    boolean isSelected = escapeQueryParam(itemId) in multiSelected[key]
                    String searchPageUrl = isSelected
                        ? baseUrlForKey.replace("&${param}", '') // FIXME: generate up-link in a cleaner way
                        : "${baseUrlForKey}&${param}"

                    Map observation = ['totalItems': bucket.getAt('doc_count'),
                                       'view': [(JsonLd.ID_KEY): searchPageUrl],
                                       '_selected': isSelected,
                                       'object': lookup.chip(itemId)]

                    observations << observation
                }
                else {
                    String searchPageUrl = "${baseUrlForKey}&${ESQuery.AND_PREFIX}${makeParam(key, itemId)}"

                    Map observation = ['totalItems': bucket.getAt('doc_count'),
                                       'view': [(JsonLd.ID_KEY): searchPageUrl],
                                       'object': lookup.chip(itemId)]

                    observations << observation
                }
            }

            if (observations) {
                sliceNode['observation'] = observations
                acc[key] = sliceNode
            }

            return acc
        }
        
        if (reverseObject && !hasHugeNumberOfIncomingLinks(reverseObject)) {
            def counts = groupRelations(whelk.relations.getReverseCountByRelation(reverseObject)) // TODO precompute and store in ES indexed doc?)
            Map sliceNode = [
                    'dimension'  : JsonLd.REVERSE_KEY,
                    'observation': counts.collect { List<String> relations, long count ->
                        def viewUrl = baseUrl + '&' + 
                                relations.collect{ makeParam('p', it + '.' + JsonLd.ID_KEY) }.join('&')
                        [
                                'totalItems': count,
                                'view'      : ['@id': viewUrl],
                                'object'    : lookup.chip(relations.first())
                        ]
                    }
            ]

            sliceMap[JsonLd.REVERSE_KEY] = sliceNode
        }

        if (sliceMap) {
            stats['sliceByDimension'] = sliceMap
        }

        return stats
    }

    List<String> makeDimensionChain(String key) {
        List<String> dimensionChain = key.split(/\./).findResults {
            it == JsonLd.TYPE_KEY ? 'rdf:type' : it == JsonLd.ID_KEY ? null : it
        }
        if (dimensionChain[0] == JsonLd.REVERSE_KEY) {
            dimensionChain.remove(0)
            String inv = dimensionChain.remove(0)
            dimensionChain.add(0, ['inverseOfTerm': inv])
        }
        return dimensionChain
    }

    /**
     * Group together instanceOf.x and x
     * 
     * Meaning there will be one predicate/facet for e.g. 'subject' and 'instanceOf.subject' called 'subject'.
     * That is, it will match both works and instances with local works.
     * Calling the relation 'subject' is of course not completely correct (it hides instanceOf) but the idea is that 
     * it is more practical for now.
     */
    static List<Tuple2<List<String>, Long>> groupRelations(Map<String, Long> counts) {
        Map<String, Long> blankWork = [:]
        Map<String, Long> other = [:]
        counts.each {relation, count -> 
            (relation.startsWith("$JsonLd.WORK_KEY.") ? blankWork : other)[relation] = count
        }

        List result = []
        other.each { relation, count ->
            String r = "$JsonLd.WORK_KEY.$relation"
            if (blankWork.containsKey(r)) {
                result.add(new Tuple2([relation, r], count + blankWork.remove(r)))
            }
            else {
                result.add(new Tuple2([relation], count))
            }
        }

        blankWork.each {relation, count ->
            result.add(new Tuple2([stripPrefix(relation, "$JsonLd.WORK_KEY."), relation], count))
        }

        return result
    }
    
    private String removeWildcardForKey(String url, String key) {
        url.replace("&${makeParam(key, '*')}", "")
    }

    private boolean hasHugeNumberOfIncomingLinks(String iri) {
        def num = numberOfIncomingLinks(iri)
        return num < 0 || num > 500_000
    }
    
    private int numberOfIncomingLinks(String iri) {
        try {
            def doc = new ElasticFind(esQuery).find([(JsonLd.ID_KEY): [iri]]).first()
            return doc['reverseLinks']['totalItems']
        }
        catch (Exception e) {
            log.warn("Error getting numberOfIncomingLinks for $iri: $e", e)
            return -1
        }
    }

    private class Lookup {
        private Multimap<String, Map> iriPos = ArrayListMultimap.create()
        
        Map chip(String itemRepr) {
            boolean matchesTerm = false
            def itemId = itemRepr
            if (itemRepr.startsWith(RangeParameterPrefix.MATCHES.prefix)) {
                matchesTerm = true
                itemId = itemId[RangeParameterPrefix.MATCHES.prefix.size()..-1]
            }

            def termKey = ld.toTermKey(itemId)
            if (termKey in ld.vocabIndex) {
                return ld.vocabIndex[termKey]
            }

            if (!itemId.startsWith('http') && itemId.contains('.')) {
                String[] parts = itemId.split('\\.')
                List chain = parts
                    .findAll { it != JsonLd.ID_KEY }
                    .collect { Lookup.this.chip(it) }
                String label = parts.join(' ')

                if (matchesTerm) {
                    def proptype = chain[-1][JsonLd.TYPE_KEY]
                    List<String> proptypes = proptype instanceof String ?
                        [(String) proptype] :
                        (List<String>) proptype
                    if (proptypes.any { ld.isSubClassOf(it, 'ObjectProperty') }) {
                        chain << Lookup.this.chip(MATCHES_PROP)
                        label = "matches $label"
                    }
                }

                return [
                        'propertyChainAxiom': chain,
                        'label': label,
                        '_key': itemRepr,  // lxlviewer has some propertyChains of its own defined, this is used to match them 
                ]
            }

            String iri = getFullUri(itemId)

            if(!iri) {
                return dummyChip(itemId)
            }

            Map m = [:]
            iriPos.put(iri, m)
            return m
        }
        
        void run() {
            Map<String, Map> cards = whelk.getCards(iriPos.keySet())
            iriPos.entries().each {
                def thing = cards.get(it.key)?.with { card -> getEntry(card, it.key) } ?: dummyChip(it.key)
                def chip = (Map) ld.toChip(thing)
                it.value.putAll(chip)
            }
        }
    }
    
    private Map dummyChip(String itemId) {
        [(JsonLd.ID_KEY): itemId, 'label': itemId]
    }

    /*
     * Read vocab term data from storage.
     *
     * Returns null if not found.
     *
     */
    private String getFullUri(String id) {
        try {
            if (vocabUri) {
                return vocabUri.resolve(id).toString()
            }
        }
        catch (IllegalArgumentException e) {
            // Couldn't resolve, which means id isn't a valid IRI.
            // No need to check the db.
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
        List params = initial.v1
        List keys = initial.v2
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
            case SearchType.ELASTIC:
                result = getElasticParams(queryParameters)
                break
        }
        return result
    }

    private Tuple2 getElasticParams(Map queryParameters) {
        if (!('q' in queryParameters) && !('o' in queryParameters)) {
            queryParameters['q'] = '*'
        }

        String query = queryParameters.remove('q')
        List initialParams = query ? [makeParam('q', query)] : []
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
    private Tuple2 mapParams(Lookup lookup, Map params, Set<String> multiSelectable) {
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
                    value = lookup.chip(val).with { it[JsonLd.ID_KEY] = val; return it }
                } else if (param.endsWith(".${JsonLd.ID_KEY}")) {
                    valueProp = 'object'
                    termKey = param[0..-5]
                    value = lookup.chip(val).with { it[JsonLd.ID_KEY] = val; return it }
                } else {
                    valueProp = 'value'
                    termKey = param
                    value = val
                }
                
                termKey = stripPrefix(termKey, ESQuery.AND_PREFIX)
                termKey = stripPrefix(termKey, ESQuery.OR_PREFIX)

                result << [
                    'variable': param,
                    'predicate': lookup.chip(termKey),
                    (valueProp): value
                ] + (param as String in multiSelectable ? ['_selected': true] : [:])

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
        return ['q', 'p', 'o', 'value', '_limit', '_offset', '_suggest', '_spell', '_computedLabel']
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

    Map buildStatsReprFromSliceSpec(List sliceList) {
        def statsfind = [:]
        for (Map slice : sliceList) {
            List<String> path = slice.dimensionChain.collect {
                it instanceof Map ? "@reverse.${it['inverseOfTerm']}" :
                it == 'rdf:type' ? JsonLd.TYPE_KEY : it
            }
            String leaf = path[-1]
            if (!ld.isVocabTerm(leaf) && ld.isInstanceOf(ld.vocabIndex[leaf], 'ObjectProperty')) {
                path << JsonLd.ID_KEY
            }
            String key = path.join('.')
            int limit = slice['itemLimit']
            def connective = slice['connective']?.equals(OR.toString()) ? OR : AND
            statsfind[key] = ['sort': 'value', 'sortOrder': 'desc', 'size': limit, 'connective': connective]
            if (slice['_matchMissing']) { // FIXME: what should it be called?
                statsfind[key]['_matchMissing'] = slice['_matchMissing']
            }
        }
        return statsfind
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

