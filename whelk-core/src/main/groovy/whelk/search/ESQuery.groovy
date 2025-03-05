package whelk.search

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.transform.TypeCheckingMode
import groovy.util.logging.Log4j2 as Log
import whelk.JsonLd
import whelk.Whelk
import whelk.exception.InvalidQueryException
import whelk.util.DocumentUtil
import whelk.util.Unicode

import static whelk.component.ElasticSearch.flattenedLangMapKey
import static whelk.util.Jackson.mapper
import static whelk.util.Unicode.stripPrefix

@CompileStatic
@Log
class ESQuery {
    enum Connective {
        AND,
        OR
    }

    private Whelk whelk
    private JsonLd jsonld
    private Set keywordFields
    private Set dateFields
    private Set<String> nestedFields
    private Set<String> nestedNotInParentFields
    private Set<String> numericExtractorFields

    private static final int DEFAULT_PAGE_SIZE = 50
    private static final List RESERVED_PARAMS = [
            'q', 'o', '_limit', '_offset', '_sort', '_statsrepr', '_site_base_uri', '_debug', '_boost', '_lens', '_stats', '_suggest', '_site', '_spell', '_searchMainOnly'
    ]
    public static final String AND_PREFIX = 'and-'
    public static final String AND_MATCHES_PREFIX = 'and-matches-'
    public static final String OR_PREFIX = 'or-'
    private static final String NOT_PREFIX = 'not-'
    private static final String EXISTS_PREFIX = 'exists-'

    private static final List<String> QUERY_RANGE_PREFIXES = [AND_MATCHES_PREFIX] + RangeParameterPrefix.values().collect { it.prefix }
    // Prefixes are matched in this order so AND_MATCHES_PREFIX must be before AND_PREFIX.
    private static final List<String> QUERY_PREFIXES = QUERY_RANGE_PREFIXES + [AND_PREFIX, OR_PREFIX, NOT_PREFIX,
                                                                               EXISTS_PREFIX]

    private static final String FILTERED_AGG_NAME = 'a'
    private static final String NESTED_AGG_NAME = 'n'

    private static final String SPELL_CHECK_FIELD = '_sortKeyByLang.sv.trigram'
    private static final String SPELL_CHECK_FIELD_REVERSE = '_sortKeyByLang.sv.reverse'

    private static final Map recordsOverCacheRecordsBoost = [
            'bool': ['should': [
                    ['constant_score': [
                            'filter': ['term': [(JsonLd.RECORD_KEY + '.' + JsonLd.TYPE_KEY): JsonLd.RECORD_TYPE]],
                            'boost' : 1000.0
                    ]],
                    ['constant_score': [
                            'filter': ['term': [(JsonLd.RECORD_KEY + '.' + JsonLd.TYPE_KEY): JsonLd.CACHE_RECORD_TYPE]],
                            'boost' : 1.0
                    ]]
            ]]
    ]

    private Map<String, List<String>> boostFieldsByType = [:]
    private ESQueryLensBoost lensBoost

    // TODO: temporary feature flag, to be removed
    // this feature only works after a full reindex has been done, so we have to detect that
    private boolean ENABLE_SPELL_CHECK = false

    ESQuery() {
        // NOTE: For unit tests only!
    }

    ESQuery(Whelk whelk) {
        this.whelk = whelk
        this.jsonld = whelk.jsonld
        initFieldMappings(this.whelk)

        this.lensBoost = new ESQueryLensBoost(jsonld)
    }

    void initFieldMappings(Whelk whelk) {
        if (whelk.elastic) {
            Map mappings = whelk.elastic.getMappings()
            Map mappingsSecondary = whelk.elastic.getMappings(whelk.elastic.secondaryIndexName)
            this.keywordFields = getKeywordFields(mappings) + getKeywordFields(mappingsSecondary)
            this.dateFields = getFieldsOfType('date', mappings) + getFieldsOfType('date', mappingsSecondary)
            this.nestedFields = getFieldsOfType('nested', mappings) + getFieldsOfType('nested', mappingsSecondary)
            this.nestedNotInParentFields = nestedFields - (getFieldsWithSetting('include_in_parent', true, mappings) + getFieldsWithSetting('include_in_parent', true, mappingsSecondary))
            this.numericExtractorFields = getFieldsWithAnalyzer('numeric_extractor', mappings) + getFieldsWithAnalyzer('numeric_extractor', mappingsSecondary)

            if (DocumentUtil.getAtPath(mappings, ['properties', '_sortKeyByLang', 'properties', 'sv', 'fields', 'trigram'], null)) {
                ENABLE_SPELL_CHECK = true
            }
            log.info("ENABLE_SPELL_CHECK = ${ENABLE_SPELL_CHECK}")
        } else {
            this.keywordFields = Collections.emptySet()
            this.dateFields = Collections.emptySet()
            this.nestedFields = Collections.emptySet()
        }
    }

    void setKeywords(Set keywordFields) {
        // NOTE: For unit tests only!
        this.keywordFields = keywordFields
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    Map doQuery(Map<String, String[]> queryParameters, String suggest = null, String spell = null, String searchMainOnly = null) {
        Map esQuery = getESQuery(queryParameters, suggest, spell)
        Map esResponse
        if (searchMainOnly == 'true') {
            esResponse = whelk.elastic.query(esQuery, true)
        } else {
            esResponse = whelk.elastic.query(esQuery)
        }
        return collectQueryResults(esResponse, esQuery, queryParameters, { def d = it."_source"; d."_id" = it."_id"; return d })
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    Map doQueryIds(Map<String, String[]> queryParameters) {
        Map esQuery = getESQuery(queryParameters)
        Map esResponse = whelk.elastic.query(esQuery)
        return collectQueryResults(esResponse, esQuery, queryParameters, { it."_id" })
    }

    private Map collectQueryResults(Map esResponse,
                                    Map esQuery,
                                    Map<String, String[]> queryParameters,
                                    Closure<Map> hitCollector) {
        def results = [:]

        results['startIndex'] = esQuery['from']
        results['totalHits'] = esResponse['hits']['total']['value']
        results['items'] = esResponse['hits']['hits'].collect(hitCollector)
        results['aggregations'] = esResponse['aggregations']
        // Spell checking
        if (esResponse['suggest']?['simple_phrase']) {
            results['spell'] = ((List) esResponse['suggest']['simple_phrase'])[0]['options']
        }

        if ('esQuery' in queryParameters.get('_debug')) {
            results['_debug'] = [esQuery: esQuery]
        }
        if ('esScore' in queryParameters.get('_debug')) {
            results['_debug'] = results['_debug'] ?: [:]
            results['_debug']['esScore'] = esResponse['hits']['hits'].collect { ((Map) it).subMap(['_id', '_score', '_explanation']) }
        }

        return hideKeywordFields(moveAggregationsToTopLevel(results))
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    Map getESQuery(Map<String, String[]> ogQueryParameters, String suggest = null, String spell = null) {
        Map<String, String[]> queryParameters = new HashMap<>(ogQueryParameters)
        // Legit params and their uses:
        //   q - query string, will be used as query_string or simple_query_string
        String q
        //   _limit, _offset - pagination
        int limit
        int offset
        //   _sort - keyword sort
        List sortBy
        //   _statsrepr - aggregates
        Map aggQuery
        //   _site_base_uri - Filter id.kb.se resources
        List siteFilter
        //   any k=v param - FILTER query (same key => OR, different key => AND)
        List filters
        Map multiSelectFilters
        //  _spell - check spelling
        Map spellQuery

        if (suggest && !whelk.jsonld.locales.contains(suggest)) {
            throw new InvalidQueryException("Parameter '_suggest' value '${suggest}' invalid, must be one of ${whelk.jsonld.locales}")
        }

        String[] originalTypeParam = queryParameters.get('@type')
        if (originalTypeParam != null) {
            queryParameters.put('@type',
                    expandTypeParam(originalTypeParam, whelk.jsonld))
        }

        if (queryParameters.containsKey('o')) {
            queryParameters.put('_links', queryParameters.get('o'))
        }

        q = Unicode.normalizeForSearch(getQueryString(queryParameters))
        (limit, offset) = getPaginationParams(queryParameters)
        sortBy = getSortClauses(queryParameters)
        siteFilter = getSiteFilter(queryParameters)
        (filters, multiSelectFilters) = getFilters(queryParameters)
        aggQuery = getAggQuery(queryParameters, multiSelectFilters)

        if (spell && q) {
            spellQuery = getSpellQuery(q)
        }
        // If the `_spell` query param is "only", return a query containing *only*
        // the spell checking part
        if (ENABLE_SPELL_CHECK && spell == "only" && q) {
            return ['suggest': spellQuery]
        }

        def isSimple = isSimple(q)
        String queryMode = isSimple ? 'simple_query_string' : 'query_string'
        if (!isSimple) {
            q = escapeNonSimpleQueryString(q)
        }

        Map simpleQuery = [
                (queryMode): [
                        'query'           : q,
                        'default_operator': 'AND',
                        'analyze_wildcard': true
                ]
        ]

        // In case of suggest/autocomplete search, target a specific field with a specific query type
        Map queryClauses = simpleQuery

        String[] boostParam = queryParameters.get('_boost')
        String boostMode = boostParam ? boostParam[0] : null
        List boostedFields = getBoostFields(originalTypeParam, boostMode)

        if (boostedFields && !suggest) {
            def softFields = boostedFields.findAll {
                it.contains(JsonLd.SEARCH_KEY)
            }
            def exactFields = boostedFields.findResults {
                it.replace(JsonLd.SEARCH_KEY, "${JsonLd.SEARCH_KEY}.exact")
            }

            Map boostedExact = [
                    (queryMode): [
                            'query'           : q,
                            'default_operator': 'AND',
                            'fields'          : exactFields,
                            'analyze_wildcard': true
                    ]
            ]

            Map boostedSoft = [
                    (queryMode): [
                            'query'             : q,
                            'default_operator'  : 'AND',
                            'fields'            : softFields,
                            'quote_field_suffix': ".exact",
                            'analyze_wildcard'  : true
                    ]
            ]

            queryClauses = ['bool':
                                    ['must': [
                                            ['bool': ['should': [
                                                    boostedExact,
                                                    boostedSoft,
                                                    simpleQuery]]],
                                            recordsOverCacheRecordsBoost
                                    ]
                                    ]]
        }

        Map query
        if (suggest) {
            query = [
                    'query': [
                            'bool': [
                                    'must'  : [
                                            'multi_match': [
                                                    'query' : q,
                                                    'type'  : 'bool_prefix',
                                                    'fields': [
                                                            "_sortKeyByLang.${suggest}.suggest".toString(),
                                                            "_sortKeyByLang.${suggest}.suggest._2gram".toString(),
                                                            "_sortKeyByLang.${suggest}.suggest._3gram".toString()
                                                    ]
                                            ]
                                    ],
                                    'should': [
                                            'prefix': [
                                                    ("_sortKeyByLang.${suggest}.keyword".toString()): [
                                                            'value': q,
                                                            'boost': 100
                                                    ]
                                            ]
                                    ]
                            ]
                    ],
                    'sort' : [
                            '_score'                                        : 'desc',
                            ("_sortKeyByLang.${suggest}.keyword".toString()): 'asc'
                    ]
            ]
        } else {
            query = [
                    'query': [
                            'bool': [
                                    'must': [
                                            queryClauses
                                    ]
                            ]
                    ]
            ]
        }

        if (limit >= 0) {
            query['size'] = limit
        }

        if (offset) {
            query['from'] = offset
        }

        // FIXME: How should these be filtered?
        // Never index them at all?
        // not-meta.@type=AdminRecord ?
        def recordFilter = [['bool': ['must_not': createBoolFilter(['@type': ['HandleAction']])]]]

        if (filters && siteFilter) {
            query['query']['bool']['filter'] = recordFilter + filters + siteFilter
        } else if (filters) {
            query['query']['bool']['filter'] = recordFilter + filters
        } else if (siteFilter) {
            query['query']['bool']['filter'] = recordFilter + siteFilter
        } else {
            query['query']['bool']['filter'] = recordFilter
        }

        if (sortBy) {
            query['sort'] = sortBy
        }

        if (aggQuery) {
            query['aggs'] = aggQuery
        }

        if (multiSelectFilters) {
            query['post_filter'] = ['bool': ['must': multiSelectFilters.values()]]
        }

        if (ENABLE_SPELL_CHECK && spell && q) {
            query['suggest'] = spellQuery
        }

        query['track_total_hits'] = true

        if (queryParameters['_debug']?.contains('esScore')) {
            if (sortBy) {
                // Scores won't be calculated when also using sort unless explicitly asked for
                query['track_scores'] = true
            }
            query['explain'] = true
        }

        return query
    }

    List<String> getBoostFields(String[] types, String boostMode) {
        if (boostMode?.indexOf('^') > -1) {
            return boostMode.tokenize(',')
        }
        if (boostMode == 'id.kb.se') {
            return CONCEPT_BOOST
        }

        String typeKey = types != null ? types.toUnique().sort().join(',') : ''
        typeKey += boostMode

        List<String> boostFields = boostFieldsByType[typeKey]
        if (boostFields == null) {
            if (boostMode == 'hardcoded') {
                boostFields = [
                        'prefLabel^100',
                        'code^100',
                        'name^100',
                        'familyName^100', 'givenName^100',
                        'lifeSpan^100', 'birthYear^100', 'deathYear^100',
                        'hasTitle.mainTitle^100', 'title^100',
                        'heldBy.sigel^100',
                ]
            } else {
                boostFields = computeBoostFields(types)
            }
            boostFieldsByType[typeKey] = boostFields
        }

        return boostFields
    }

    List<String> computeBoostFields(String[] types) {
        /* FIXME:
           lensBoost.computeBoostFieldsFromLenses does not give a good result for Concept. 
           Use hand-tuned boosting instead until we improve boosting/ranking in general. See LXL-3399 for details. 
        */
        def l = ((types ?: []) as List<String>).split { jsonld.isSubClassOf(it, 'Concept') }
        def (conceptTypes, otherTypes) = [l[0], l[1]]

        if (conceptTypes) {
            if (otherTypes) {
                def fromLens = lensBoost.computeBoostFieldsFromLenses(otherTypes as String[])
                def conceptFields = CONCEPT_BOOST.collect { it.split('\\^')[0] }
                def otherFieldsBoost = fromLens.findAll { !conceptFields.contains(it.split('\\^')[0]) }
                return CONCEPT_BOOST + otherFieldsBoost
            } else {
                return CONCEPT_BOOST
            }
        } else {
            return lensBoost.computeBoostFieldsFromLenses(types)
        }
    }

    private static final List<String> CONCEPT_BOOST = [
            'prefLabel^1500',
            'prefLabelByLang.sv^1500',
            'label^500',
            'labelByLang.sv^500',
            'code^200',
            'termComponentList._str.exact^125',
            'termComponentList._str^75',
            'altLabel^150',
            'altLabelByLang.sv^150',
            'hasVariant.prefLabel.exact^150',
            '_str.exact^100',
            'inScheme._str.exact^100',
            'inScheme._str^100',
            'inCollection._str.exact^10',
            'broader._str.exact^10',
            'exactMatch._str.exact^10',
            'closeMatch._str.exact^10',
            'broadMatch._str.exact^10',
            'related._str.exact^10',
            'scopeNote^10',
            'keyword._str.exact^10',
    ]

    private static final Set subjectRange = ["Person", "Family", "Meeting", "Organization", "Jurisdiction", "Subject", "Work"] as Set


    /**
     * Expand `@type` query parameter with subclasses.
     *
     * This also removes superclasses, since we only care about the most
     * specific class.
     */
    static String[] expandTypeParam(String[] types, JsonLd jsonld) {
        // Filter out all types that have (more specific) subclasses that are
        // also in the list.
        // So for example [Instance, Electronic] should be reduced to just
        // [Electronic].
        // Afterwards, include all subclasses of the remaining types.
        Set<String> subClasses = []

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
        for (String type : prunedTypes) {
            subClasses += jsonld.getSubClasses(type)
            subClasses.add(type)
        }

        return subClasses.toArray()
    }

    /**
     * Get query string from query params, or default if not found.
     *
     * Public for test only - don't call outside this class!
     *
     */
    @PackageScope
    String getQueryString(Map<String, String[]> queryParameters) {
        if ('q' in queryParameters) {
            // 'q' should only occur once, ingore any others
            return queryParameters.get('q')[0]
        } else {
            return '*'
        }
    }

    /**
     * Get limit and offset from query params, or default if not found.
     *
     * Public for test only - don't call outside this class!
     *
     */
    @PackageScope
    Tuple2<Integer, Integer> getPaginationParams(Map<String, String[]> queryParameters) {
        int limit = DEFAULT_PAGE_SIZE
        int offset = 0
        if ('_limit' in queryParameters) {
            String lim = queryParameters.get('_limit')[0]
            if (lim.isInteger()) {
                limit = lim as Integer
            }
        }
        if ('_offset' in queryParameters) {
            String off = queryParameters.get('_offset')[0]
            if (off.isInteger()) {
                offset = off as Integer
            }
        }
        return new Tuple2(limit, offset)
    }

    /**
     * Get sort clauses from query params, or null if not found.
     *
     * Public for test only - don't call outside this class!
     *
     */
    @PackageScope
    List getSortClauses(Map<String, String[]> queryParameters) {
        if (!('_sort' in queryParameters)) return null
        if (!(queryParameters.get('_sort').size() > 0) ||
                queryParameters.get('_sort')[0] == '') {
            return null
        }

        List result = []
        String sortParams = queryParameters.get('_sort')[0]
        sortParams.split(',').each { String sortParam ->
            result << getSortClause(sortParam)
        }
        return result
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    private Map getSortClause(String sortParam) {
        def (String field, String sortOrder) = getFieldAndSortOrder(sortParam)
        String termPath = getInferredSortTermPath(field)
        Map clause = [(termPath): ['order': sortOrder]]
        // FIXME: this should be based on if the path is inside nested, not hardcoded to hasTitle.mainTitle
        // what about the filter condition then?
        if (field == 'hasTitle.mainTitle' || field == 'hasTitle.mainTitle.keyword') {
            clause[termPath]['nested'] = [
                    'path'  : 'hasTitle',
                    'filter': ['term': ['hasTitle.@type': 'Title']]
            ]
        }
        return clause
    }

    private Tuple2<String, String> getFieldAndSortOrder(String field) {
        if (field.startsWith('-')) {
            return new Tuple2(field.substring(1), 'desc')
        } else {
            return new Tuple2(field, 'asc')
        }
    }

    private String getInferredTermPath(String termPath) {
        termPath = expandLangMapKeys(termPath)
        if (termPath in keywordFields) {
            return "${termPath}.keyword"
        } else {
            return termPath
        }
    }

    private String getInferredSortTermPath(String termPath) {
        termPath = expandLangMapKeys(termPath)
        if (termPath in keywordFields && termPath !in numericExtractorFields) {
            return "${termPath}.keyword"
        } else {
            return termPath
        }
    }

    /**
     * Get site filter clause from query params, or null if not found.
     *
     * Public for test only - don't call outside this class!
     *
     */
    List getSiteFilter(Map<String, String[]> queryParameters) {
        if (!('_site_base_uri' in queryParameters)) return null
        if (!(queryParameters.get('_site_base_uri').size() > 0)) return null

        String siteBaseUri = queryParameters.get('_site_base_uri')[0]
        List prefixFilters = [
                ['prefix': ['@id': siteBaseUri]],
                ['prefix': ['sameAs.@id': siteBaseUri]]
        ]

        // We want either of the prefix filters to match, so we put them
        // inside a bool query
        return [['bool': ['should': prefixFilters]]]
    }

    /**
     * Return a bool filter of any non-reserved query params, or null if no
     * params found.
     *
     * Public for test only - don't call outside this class!
     *
     */
    @CompileStatic(TypeCheckingMode.SKIP)
    Tuple2<List, Map> getFilters(Map<String, String[]> queryParameters) {
        def queryParametersCopy = new HashMap<>(queryParameters)
        List filters = []
        Map multiSelectFilters = [:]

        var groups = queryParametersCopy.groupBy { p -> getPrefixGroup(p.key, nestedFields) }
        var nested = getNestedParams(groups)
        Map notNested = (groups - nested).collect { it.value }.sum([:])

        // If both nested and notNested contains explicit OR they should be moved to notNested
        // If two different nested contains explicit OR they should be moved to not notNested
        boolean explicitOrInDifferentNested = nested.values()
                .findAll { it.keySet().any { k -> k.startsWith(OR_PREFIX) } }
                .size() > 1

        if (notNested.keySet().any { it.startsWith(OR_PREFIX) } || explicitOrInDifferentNested) {
            nested.values().each { n ->
                n.keySet().findAll { it.startsWith(OR_PREFIX) }.each {
                    notNested.put(it, n.remove(it))
                }
            }
            nested.removeAll { it.value.isEmpty() }
        }

        nested.each { key, vals ->
            filters.addAll(createNestedBoolFilters(key, vals))
        }

        makeRangeFilters(notNested).with { handledParameters, List rangeFilters ->
            handledParameters.each { notNested.remove(it) }
            filters.addAll(rangeFilters)
        }

        List notNestedGroupsForNot = []
        notNested.removeAll {
            if (it.key in RESERVED_PARAMS) {
                return true
            }
            // Any not-<field> is removed from notNested and added to a separate list,
            // since we need to deal with them separately
            if (it.key.startsWith(NOT_PREFIX)) {
                it.value.each { inner_value ->
                    notNestedGroupsForNot << [(it.key.substring(NOT_PREFIX.size())): [inner_value]]
                }
                return true
            }
        }

        Set multiSelectable = multiSelectFacets(queryParameters)
        Map<String, String> matchMissing = matchMissing(queryParameters)
        getOrGroups(notNested).each { Map<String, ?> m ->
            if (m.size() == 1 && m.keySet().first() in multiSelectable) {
                multiSelectFilters[m.keySet().first()] = createBoolFilter(addMissingMatch(m, matchMissing))
            } else {
                filters << wrapNestedNotInParent(createBoolFilter(addMissingMatch(m, matchMissing)))
            }
        }

        var mustNots = notNestedGroupsForNot.collect { createBoolFilter(it) }
        var filter = Q.bool(filters, mustNots).with { it.bool ? [it] : null }

        return new Tuple2(filter, multiSelectFilters)
    }

    private static Map addMissingMatch(Map m, Map matchMissing) {
        if (m.size() == 1 && m.keySet().first() in matchMissing) {
            String field = matchMissing[m.keySet().first()]
            m.put(EXISTS_PREFIX + field, ['false'])
        }
        return m
    }

    private static getPrefixGroup(String key, Set<String> nestedFields) {
        if (key.contains('.')) {
            (QUERY_PREFIXES.find { key.startsWith(it) } ?: "").with { String prefix ->
                String nested = nestedFields.find { key.startsWith(prefix + it + '.') }
                if (nested) {
                    return key.substring(prefix.length(), prefix.length() + nested.length())
                } else {
                    return key.substring(0, key.indexOf('.'))
                }
            }
        } else {
            return key
        }
    }

    private Map wrapNestedNotInParent(Map boolFilter) {
        var nested = { String f -> nestedNotInParentFields.find { (f ?: '').startsWith(it + '.') } }

        var fields = DocumentUtil.getAtPath(boolFilter, ['bool', 'should', '*', 'simple_query_string', 'fields', 0], [])

        if (fields.any(nested)) {
            boolFilter.bool['should'] = boolFilter.bool['should'].collect { Map q ->
                String n = nested((String) DocumentUtil.getAtPath(q, ['simple_query_string', 'fields', 0]))
                n ? Q.nested(n, q) : q
            }
        }

        return boolFilter
    }

    /**
     * @return A list of parameter maps where the entries in each map
     * should be combined with OR and the maps combined with AND
     */
    @CompileStatic(TypeCheckingMode.SKIP)
    private List<Map> getOrGroups(Map<String, String[]> parameters) {
        def or = [:]
        def and = []
        def other = [:]
        def p = [:]

        // explicit OR
        // p is always implicitly OR
        parameters.each { String key, value ->
            if (key == 'p') {
                value.each {
                    p.put(it, parameters['_links'])
                }
            } else if (key.startsWith(OR_PREFIX)) {
                or.put(key.substring(OR_PREFIX.size()), value)
            } else if (key.startsWith(AND_PREFIX)) {
                // For AND on the same field to work, we need a separate
                // map for each value
                value.each {
                    and << [(key.substring(AND_PREFIX.size())): [it]]
                }
            } else {
                other.put(key, value)
            }
        }

        // implicit OR - the same parameter exists with explicit OR
        or.each { key, value ->
            if (other.containsKey(key)) {
                or[key] = or[key] + other.remove(key)
            }
        }

        List result = other.collect { [(it.getKey()): it.getValue()] }
        if (or.size() > 0) {
            result.add(or)
        }
        if (and.size() > 0) {
            result.addAll(and)
        }
        if (p.size() > 0) {
            result.add(p)
        }

        return result
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    private Map<String, Map<String, String[]>> getNestedParams(Map<String, Map<String, String[]>> groups) {
        Map nested = groups.findAll { g ->
            // If included in parent: More than one property or more than one value for some property
            g.key in nestedNotInParentFields
                    || (g.key in nestedFields && (g.value.size() > 1 || g.value.values().any { it.length > 1 }))
        }
        return nested
    }

    @PackageScope
    @CompileStatic(TypeCheckingMode.SKIP)
    List<Map> createNestedBoolFilters(String prefix, Map<String, String[]> nestedQuery) {
        /*
            Example
            prefix: "identifiedBy"
            nestedQuery: ["and-identifiedBy.@type":["ISBN","LCCN"], "and-identifiedBy.value":["1234","5678"]]

            Example
            prefix: "@reverse.itemOf"
            nestedQuery: ["and-@reverse.itemOf.heldBy.@id":[".../library/A", ".../library/A]]

            Example
            prefix: "@reverse.itemOf"
            nestedQuery: ["@reverse.itemOf.heldBy.@id":[".../library/A"], "not-@reverse.itemOf.availability.@id": ["X"] ]
        */
        var ands = nestedQuery.findAll { it.key.startsWith(AND_PREFIX) && !it.key.startsWith(AND_MATCHES_PREFIX) }
        var nots = nestedQuery.findAll { it.key.startsWith(NOT_PREFIX) }
        var ranges = nestedQuery.findAll { e -> QUERY_RANGE_PREFIXES.any { p -> e.key.startsWith(p) } }
        var rest = nestedQuery.findAll { !(it.key in nots.keySet() || it.key in ands.keySet() || it.key in ranges.keySet()) }

        if (nots && !ranges && !ands && !rest) {
            return nots.collect {
                String prop = stripPrefix(it.key, NOT_PREFIX)
                Q.not([Q.nested(prefix, createBoolFilter([(prop): it.value]))])
            }
        }

        int numberOfReferencedDocs = ands.collect { it.value }.collect { it.length }?.max() ?: 1

        List<Map> result = []
        for (int i = 0; i < numberOfReferencedDocs; i++) {
            List<Map> musts = []
            List<Map> mustNots = null

            if (i == 0) {
                getOrGroups(rest).each {
                    musts.add(createBoolFilter(it))
                }

                makeRangeFilters(ranges).with { _, List rangeFilters ->
                    musts.addAll(rangeFilters)
                }

                mustNots = nots.collect {
                    String prop = stripPrefix(it.key, NOT_PREFIX)
                    createBoolFilter([(prop): it.value])
                }
            }

            musts.addAll(ands.findResults {
                it.value.length > i
                        ? createBoolFilter([(stripPrefix(it.key, AND_PREFIX)): [it.value[i]]])
                        : null
            })

            result << Q.nested(prefix, Q.bool(musts, mustNots))
        }

        return result
    }

    private static class Q {
        static Map not(List mustNots) {
            return bool(null, mustNots)
        }

        static Map bool(List must, List mustNot) {
            Map b = [:]
            if (must) {
                b['must'] = must
            }
            if (mustNot) {
                b['must_not'] = mustNot
            }
            return ['bool': b]
        }

        static Map nested(String prefix, Map query) {
            ['nested': ['path': prefix, 'query': query]]
        }
    }

    /**
     * Can this query string be handled by ES simple_query_string?
     */
    static boolean isSimple(String queryString) {
        // leading wildcards e.g. "*foo" are removed by simple_query_string
        def containsLeadingWildcard = queryString =~ /\*\S+/
        return !containsLeadingWildcard
    }

    static String escapeNonSimpleQueryString(String queryString) {
        if (queryString.findAll('\\"').size() % 2 != 0) {
            throw new whelk.exception.InvalidQueryException("Unbalanced quotation marks")
        }

        // The following chars are reserved in ES and need to be escaped to be used as literals: \+-=|&><!(){}[]^"~*?:/
        // Escape the ones that are not part of our query language.
        for (char c : '=&!{}[]^:/'.chars) {
            queryString = queryString.replace('' + c, '\\' + c)
        }

        // Inside words, treat '-' as regular hyphen instead of "NOT" and escape it
        queryString = queryString.replaceAll(/(^|\s+)-(\S+)/, '$1#ACTUAL_NOT#$2')
        queryString = queryString.replace('-', '\\-')
        queryString = queryString.replace('#ACTUAL_NOT#', '-')

        // Strip un-escapable characters
        for (char c : '<>'.chars) {
            queryString = queryString.replace('' + c, '')
        }

        return queryString
    }

    /**
     * Create a query to filter docs where `field` matches any of the
     * supplied `vals`
     *
     * Public for test only - don't call outside this class!
     *
     */
    @CompileStatic(TypeCheckingMode.SKIP)
    Map createBoolFilter(Map<String, String[]> fieldsAndVals) {
        List clauses = []
        fieldsAndVals.each { field, values ->
            if (field.startsWith(EXISTS_PREFIX)) {
                def f = field.substring(EXISTS_PREFIX.length())
                for (val in values) {
                    clauses.add(parseBoolean(field, val)
                            ? ['exists': ['field': f]]
                            : ['bool': ['must_not': ['exists': ['field': f]]]])
                }
            } else {
                for (val in values) {
                    boolean isSimple = isSimple(val)
                    clauses.add([(isSimple ? 'simple_query_string' : 'query_string'): [
                            'query'           : isSimple ? val : escapeNonSimpleQueryString(val),
                            'fields'          : [expandLangMapKeys(field)],
                            'default_operator': 'AND'
                    ]])
                }
            }
        }

        // FIXME? "should" wrapper is not needed if values/clauses.size == 1
        return ['bool': ['should': clauses]]
    }

    private String expandLangMapKeys(String field) {
        var parts = field.split('\\.')
        if (parts && parts[-1] in jsonld.langContainerAlias.keySet()) {
            parts[-1] = flattenedLangMapKey(parts[-1])
            return parts.join('.')
        }
        return field
    }

    private static boolean parseBoolean(String parameterName, String value) {
        if (value.toLowerCase() == 'true') {
            true
        } else if (value.toLowerCase() == 'false') {
            false
        } else {
            throw new InvalidQueryException("$parameterName must be 'true' or 'false', got '$value'")
        }
    }

    /**
     * Create an agg query based on the `_statsrepr` param, or default if not
     * supplied.
     *
     * Public for test only - don't call outside this class!
     *
     */
    @CompileStatic(TypeCheckingMode.SKIP)
    Map getAggQuery(Map queryParameters, Map multiSelectFilters) {
        Map statsrepr = getStatsRepr(queryParameters)
        if (statsrepr.isEmpty()) {
            Map defaultQuery = [(JsonLd.TYPE_KEY): ['terms': ['field': JsonLd.TYPE_KEY]]]
            return defaultQuery
        }
        return buildAggQuery(statsrepr, multiSelectFilters)
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    private Map buildAggQuery(def tree, Map multiSelectFilters, int size = 10) {
        Map query = [:]
        List keys = []

        if (tree instanceof Map) {
            keys = tree.keySet() as List
        } else if (tree instanceof List) {
            keys = tree
        }

        keys.each { key ->
            String sort = tree[key]?.sort == 'key' ? '_key' : '_count'
            def sortOrder = tree[key]?.sortOrder == 'asc' ? 'asc' : 'desc'
            String termPath = getInferredTermPath(key)

            // Core agg query
            query[termPath] = ['terms': [
                    'field': termPath,
                    'size' : tree[key]?.size ?: size,
                    'order': [(sort): sortOrder]]
            ]

            // If field is nested, wrap agg query with nested
            nestedFields.find { key.startsWith(it) }?.with { nestedField ->
                query[termPath] = [
                        'nested': ['path': nestedField],
                        'aggs'  : [(NESTED_AGG_NAME): query[termPath]]
                ]
            }

            // Wrap agg query with a filter so that we can get counts for multi select filters
            def filters = multiSelectFilters.findAll { it.key != key }.values()
            query[termPath] = [
                    'aggs'  : [(FILTERED_AGG_NAME): query[termPath]],
                    'filter': ['bool': ['must': filters]]
            ]

            if (tree[key].subItems instanceof Map) {
                query[termPath]['aggs'] = buildAggQuery(tree[key].subItems, multiSelectFilters, size)
            }
        }
        return query
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    static Set<String> multiSelectFacets(Map queryParameters) {
        getStatsRepr(queryParameters).findResults { key, value ->
            value['connective'] == Connective.OR.toString() ? key : null
        } as Set<String>
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    static Map<String, String> matchMissing(Map queryParameters) {
        getStatsRepr(queryParameters)
                .findAll { key, value -> value['_matchMissing'] }
                .collectEntries { key, value -> [key, value['_matchMissing'] as String] }
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    private static Map getStatsRepr(Map queryParameters) {
        mapper.readValue(queryParameters.get('_statsrepr')?[0] ?: '{}', Map)
    }

    /**
     * Construct "range query" filters for parameters prefixed with any {@link RangeParameterPrefix}
     * Ranges for different parameters are ANDed together
     * Multiple ranges for the same parameter are ORed together, the only exception being when `matches-`
     * is itself prefixed with `and-`.
     *
     * Range endpoints are matched in the order they appear
     * e.g. minEx-x=1984&maxEx-x=1988&minEx-x=1993&min-x=2000&maxEx-x=1995
     * means 1984 < x < 1988 OR 1993 < x < 1995 OR x >= 2000
     */
    Tuple2<Set<String>, List> makeRangeFilters(Map<String, String[]> queryParameters) {
        Map<String, Ranges> parameterToRanges = [:]
        Set<String> andParameters = new HashSet<>()
        Set<String> handledParameters = new HashSet<>()
        List<Tuple2<String, List>> queryParamsList = []

        queryParameters.each { parameter, values ->
            if (parameter.startsWith(AND_MATCHES_PREFIX)) {
                values.each { queryParamsList.add(new Tuple2(parameter.substring(AND_PREFIX.size()), [it])) }
                // `and-matches-` needs some special handling, so keep track of parameters that should be ANDed
                andParameters.add(parameter.substring(AND_PREFIX.size()))
                handledParameters.add(parameter)
            } else {
                queryParamsList.add(new Tuple2(parameter, values))
            }
        }

        queryParamsList.eachWithIndex { it, idx ->
            def parameter = (String) it[0]
            def values = (List<String>) it[1]

            parseRangeParameter(parameter) { String parameterNoPrefix, RangeParameterPrefix prefix ->
                // If a parameter has multiple values that should be ANDed we need to make sure that
                // each value is under its own unique key in parameterToRanges (hence the index)
                String parameterMapKey = parameter in andParameters ? "${parameterNoPrefix}-${idx}" : parameterNoPrefix
                Ranges r = parameterToRanges.computeIfAbsent(parameterMapKey, { p ->
                    parameterNoPrefix in dateFields
                            ? Ranges.date(parameterNoPrefix, whelk.getTimezone(), whelk)
                            : Ranges.nonDate(parameterNoPrefix, whelk)
                })

                values.each { it.tokenize(',').each { r.add(prefix, it.trim()) } }
                handledParameters.add(parameter)
            }
        }

        def filters = parameterToRanges.values().collect { it.toQuery() }

        return new Tuple2(handledParameters, filters)
    }

    private static void parseRangeParameter(String parameter, Closure handler) {
        for (RangeParameterPrefix p : RangeParameterPrefix.values()) {
            if (parameter.startsWith(p.prefix())) {
                handler(parameter.substring(p.prefix().size()), p)
                return
            }
        }
    }

    static Set<String> getFieldsOfType(String type, Map mappings) {
        getFieldsWithSetting('type', type, mappings)
    }

    static Set<String> getFieldsWithAnalyzer(String analyzer, Map mappings) {
        getFieldsWithSetting('analyzer', analyzer, mappings)
    }

    static Set getFieldsWithSetting(String setting, value, Map mappings) {
        Set fields = [] as Set
        DocumentUtil.findKey(mappings['properties'], setting) { v, path ->
            if (v == value) {
                fields.add(path.dropRight(1).findAll { it != 'properties' }.join('.'))
            }
            DocumentUtil.NOP
        }
        return fields
    }

    /**
     * Return a set of all fields with a `keyword` subfield.
     *
     * NOTE: This isn't the same thing as all fields of type keyword!
     * The purpose of this is to allow callers to skip the `.keyword` suffix
     * (on the fields in the returned set) in e.g. the sort clause of the query
     * to keep the API from leaking internal implementation details.
     *
     * Public for test only - don't call outside this class!
     *
     */
    Set getKeywordFields(Map mappings) {
        Set keywordFields = [] as Set
        if (mappings) {
            keywordFields = getKeywordFieldsFromProperties(mappings['properties'] as Map)
        }

        return keywordFields
    }

    private Set getKeywordFieldsFromProperties(Map properties, String parentName = '') {
        Set result = [] as Set
        properties.each { fieldName, fieldSettings ->
            result += getKeywordFieldsFromProperty(fieldName as String,
                    fieldSettings as Map, parentName)
        }

        return result
    }

    private Set getKeywordFieldsFromProperty(String fieldName, Map fieldSettings, String parentName) {
        Set result = [] as Set
        String currentField
        if (parentName == '') {
            currentField = fieldName
        } else {
            currentField = "${parentName}.${fieldName}"
        }
        Map fields = (Map) fieldSettings.get('fields')
        if (fields && fields.get('keyword')) {
            result.add(currentField)
        }
        Map properties = (Map) fieldSettings.get('properties')
        if (properties) {
            result += getKeywordFieldsFromProperties(properties, currentField)
        }
        return result
    }

    static Map moveAggregationsToTopLevel(Map esResponse) {
        if (!esResponse['aggregations']) {
            return esResponse
        }

        Map aggregations = (Map) esResponse['aggregations']
        aggregations.keySet().each { k ->
            if (aggregations[k][FILTERED_AGG_NAME]) {
                aggregations[k] = aggregations[k][FILTERED_AGG_NAME]
            }
            if (aggregations[k][NESTED_AGG_NAME]) {
                aggregations[k] = aggregations[k][NESTED_AGG_NAME]
            }
        }

        return esResponse
    }

    /**
     * Hide any `.keyword` field in ES response.
     *
     * Since this is an implementation detail, we don't want to leak it to
     * consumers. We drop the suffix for those cases where the ES mapping
     * allows.
     *
     * Public for test only - don't call outside this class!
     *
     */
    @CompileStatic(TypeCheckingMode.SKIP)
    Map hideKeywordFields(Map esResponse) {
        // no aggs? nothing to do.
        if (!esResponse.containsKey('aggregations')) {
            return esResponse
        }

        Map aggs = esResponse['aggregations']
        Map newAggs = [:]
        aggs.each { k, v ->
            if (k.endsWith('.keyword')) {
                String strippedKey = k.replaceFirst(/\.keyword$/, '')

                // we only care about actual keyword fields
                if (strippedKey in keywordFields) {
                    newAggs[(strippedKey)] = v
                } else {
                    newAggs[(k)] = v
                }
            } else {
                newAggs[(k)] = v
            }
        }

        esResponse['aggregations'] = newAggs
        return esResponse
    }

    int getMaxItems() {
        return whelk.elastic.maxResultWindow
    }

    static Map getSpellQuery(String q) {
        return [
                'text'         : q,
                'simple_phrase': [
                        'phrase': [
                                'field'           : SPELL_CHECK_FIELD,
                                'size'            : 1,
                                'max_errors'      : 2,
                                'direct_generator': [
                                        [
                                                'field'       : SPELL_CHECK_FIELD,
                                                'suggest_mode': 'always',
                                        ],
                                        [
                                                'field'       : SPELL_CHECK_FIELD_REVERSE,
                                                'suggest_mode': 'always',
                                                "pre_filter"  : "reverse",
                                                "post_filter" : "reverse"
                                        ]
                                ],
                                'highlight'       : [
                                        'pre_tag' : '<em>',
                                        'post_tag': '</em>'
                                ]
                        ]
                ]
        ]
    }
}
