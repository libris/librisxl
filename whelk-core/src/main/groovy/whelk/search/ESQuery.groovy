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

@CompileStatic
@Log
class ESQuery {
    private Whelk whelk
    private JsonLd jsonld
    private Set keywordFields
    private Set dateFields
    private Set nestedFields
    
    private static final int DEFAULT_PAGE_SIZE = 50
    private static final List RESERVED_PARAMS = [
        'q', 'o', '_limit', '_offset', '_sort', '_statsrepr', '_site_base_uri', '_debug', '_boost', '_lens', '_stats', '_suggest', '_site'
    ]
    private static final String OR_PREFIX = 'or-'
    private static final String EXISTS_PREFIX = 'exists-'

    private static final Map recordsOverCacheRecordsBoost = [
            'constant_score': [
                    'filter': [ 'term': [ (JsonLd.RECORD_KEY + '.' + JsonLd.TYPE_KEY) : JsonLd.BASE_RECORD_TYPE ]],
                    'boost': 1000.0
            ]
    ]

    private Map<String, List<String>> boostFieldsByType = [:]
    private ESQueryLensBoost lensBoost

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
            this.keywordFields =  getKeywordFields(mappings)
            this.dateFields = getFieldsOfType('date', mappings)
            this.nestedFields = getFieldsOfType('nested', mappings)
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
    Map doQuery(Map<String, String[]> queryParameters, suggest = null) {
        Map esQuery = getESQuery(queryParameters, suggest)
        Map esResponse = hideKeywordFields(whelk.elastic.query(esQuery))
        if ('esQuery' in queryParameters.get('_debug')) {
            esResponse._debug = [esQuery: esQuery]
        }
        return esResponse
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    Map doQueryIds(Map<String, String[]> queryParameters) {
        Map esQuery = getESQuery(queryParameters)
        Map esResponse = hideKeywordFields(whelk.elastic.queryIds(esQuery))
        if ('esQuery' in queryParameters.get('_debug')) {
            esResponse._debug = [esQuery: esQuery]
        }
        return esResponse
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    Map getESQuery(Map<String, String[]> ogQueryParameters, suggest = null) {
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
        aggQuery = getAggQuery(queryParameters)
        filters = getFilters(queryParameters)

        def isSimple = isSimple(q)
        String queryMode = isSimple ? 'simple_query_string' : 'query_string'
        if (!isSimple) {
            q = escapeNonSimpleQueryString(q)
        }

        Map simpleQuery = [
            (queryMode) : [
                'query': q,
                'default_operator':  'AND',
                'analyze_wildcard' : true
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
                    'query': q,
                    'default_operator':  'AND',
                    'fields': exactFields,
                    'analyze_wildcard' : true
                ]
            ]

            Map boostedSoft = [
                (queryMode) : [
                    'query': q,
                    'default_operator':  'AND',
                    'fields': softFields,
                    'quote_field_suffix': ".exact",
                    'analyze_wildcard' : true
                ]
            ]

            queryClauses = [
                'bool': ['should': [
                    boostedExact,
                    boostedSoft,
                    recordsOverCacheRecordsBoost,
                    simpleQuery
                ]]
            ]
        }

        Map query
        if (suggest) {
            query = [
                'query': [
                    'bool': [
                        'must': [
                            'multi_match': [
                                'query': q,
                                'type': 'bool_prefix',
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
                'sort': [
                    '_score': 'desc',
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

        if (filters && siteFilter) {
            query['query']['bool']['filter'] = filters + siteFilter
        } else if (filters) {
            query['query']['bool']['filter'] = filters
        } else if (siteFilter) {
            query['query']['bool']['filter'] = siteFilter
        }

        if (sortBy) {
            query['sort'] = sortBy
        }

        if (aggQuery) {
            query['aggs'] = aggQuery
        }

        query['track_total_hits'] = true

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
                def conceptFields = CONCEPT_BOOST.collect{ it.split('\\^')[0]}
                def otherFieldsBoost = fromLens.findAll{!conceptFields.contains(it.split('\\^')[0]) }
                return CONCEPT_BOOST + otherFieldsBoost
            }
            else {
                return CONCEPT_BOOST
            }
        }
        else {
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
        String termPath = getInferredTermPath(field)
        Map clause = [(termPath): ['order': sortOrder]]
        if (field == 'hasTitle.mainTitle' || field == 'hasTitle.mainTitle.keyword') {
            clause[termPath]['nested_path'] = 'hasTitle'
            clause[termPath]['nested_filter'] = ['term': ['hasTitle.@type': 'Title']]
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
        if (termPath in keywordFields) {
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
    List getFilters(Map<String, String[]> queryParameters) {
        def queryParametersCopy = new HashMap<>(queryParameters)
        List filters = []

        def (handledParameters, rangeFilters) = makeRangeFilters(queryParametersCopy)
        handledParameters.each {queryParametersCopy.remove(it)}
        filters.addAll(rangeFilters)
        
        Map groups = queryParametersCopy.groupBy { p -> getPrefixIfExists(p.key) }
        Map nested = getNestedParams(groups)
        Map notNested = (groups - nested).collect { it.value }.sum([:])
        nested.each { key, vals ->
            filters.addAll(createNestedBoolFilters(key, vals))
        }

        notNested.removeAll {it.key in RESERVED_PARAMS}

        getOrGroups(notNested).each { m ->
            filters << createBoolFilter(m)
        }

        if (filters) {
            return [['bool': ['must': filters]]]
        } else {
            return null
        }
    }

    private getPrefixIfExists(String key) {
        if (key.contains('.')) {
            return key.substring(0, key.indexOf('.'))
        } else {
            return key
        }
    }

    /**
     * @return A list of parameter maps where the entries in each map
     * should be combined with OR and the maps combined with AND
     */
    @CompileStatic(TypeCheckingMode.SKIP)
    private List<Map> getOrGroups(Map<String, String[]> parameters) {
        def or = [:]
        def other = [:]
        def p = [:]

        // explicit OR
        // p is always implicitly OR
        parameters.each { String key, value ->
            if (key == 'p') {
                value.each {
                    p.put(it, parameters['_links'])    
                }
            }
            else if (key.startsWith(OR_PREFIX)) {
                or.put(key.substring(OR_PREFIX.size()), value)
            }
            else {
                other.put(key, value)
            }
        }

        // implicit OR - the same parameter exists with explicit OR
        or.each { key, value ->
            if (other.containsKey(key)) {
                or[key] = or[key] + other.remove(key)
            }
        }

        List result = other.collect {[(it.getKey()): it.getValue()]}
        if (or.size() > 0) {
            result.add(or)
        }
        if (p.size() > 0) {
            result.add(p)
        }
        return result
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    private Map getNestedParams(Map<String, Map<String, String[]>> groups) {
        Map nested = groups.findAll { g ->
            // More than one property or more than one value for some property
            g.key in nestedFields && (g.value.size() > 1 || g.value.values().any{ it.length > 1})
        }
        return nested
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    private List<Map> createNestedBoolFilters(String prefix, Map<String, String[]> nestedQuery) {
        /*
            Example
            prefix: "identifiedBy"
            nestedQuery: ["identifiedBy.@type":["ISBN","LCCN"], "identifiedBy.value":["1234","5678"]]
         */
        
        int maxLen = nestedQuery.collect { it.value }.collect { it.length }.max()
        
        List result = []
        for (int i = 0 ; i < maxLen ; i++) {
            List<Map> musts = nestedQuery.findResults {
                it.value.length > i 
                    ? ['match': [(expandLangMapKeys(it.key)): it.value[i]]]
                    : null
            }
            
            result << [ 'nested': [
                            'path': prefix, 
                            'query': ['bool': ['must': musts]]]]
        }

        return result
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
            queryString = queryString.replace(''+c, '\\'+c)
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
        fieldsAndVals.each {field, values ->
            if (field.startsWith(EXISTS_PREFIX)) {
                def f = field.substring(EXISTS_PREFIX.length())
                for (val in values) {
                    clauses.add(parseBoolean(field, val)
                            ? ['exists': ['field': f]]
                            : ['bool': ['must_not': ['exists': ['field': f]]]])
                }
            }
            else {
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
        }
        else if (value.toLowerCase() == 'false') {
            false
        }
        else {
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
    Map getAggQuery(Map queryParameters) {
        if (!('_statsrepr' in queryParameters)) {
            Map defaultQuery = [(JsonLd.TYPE_KEY): ['terms': ['field': JsonLd.TYPE_KEY]]]
            return defaultQuery
        }

        Map statsrepr = mapper.readValue(queryParameters.get('_statsrepr')[0], Map)

        return buildAggQuery(statsrepr)
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    private Map buildAggQuery(def tree, int size=10) {
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
            String termPath = getInferredTermPath(key)
            query[termPath] = ['terms': [
                    'field': termPath,
                    'size': tree[key]?.size ?: size,
                    'order': [(sort):sortOrder]]]
            if (tree[key].subItems instanceof Map) {
                query[termPath]['aggs'] = buildAggQuery(tree[key].subItems, size)
            }
        }
        return query
    }

    /**
     * Construct "range query" filters for parameters prefixed with any {@link RangeParameterPrefix}
     * Ranges for different parameters are ANDed together
     * Multiple ranges for the same parameter are ORed together
     *
     * Range endpoints are matched in the order they appear
     * e.g. minEx-x=1984&maxEx-x=1988&minEx-x=1993&min-x=2000&maxEx-x=1995
     * means 1984 < x < 1988 OR 1993 < x < 1995 OR x >= 2000
     */
    Tuple2<Set<String>, List> makeRangeFilters(Map<String, String[]> queryParameters) {
        Map<String, Ranges> parameterToRanges = [:]
        Set<String> handledParameters = new HashSet<>()

        queryParameters.each { parameter, values ->
            parseRangeParameter(parameter) { String parameterNoPrefix, RangeParameterPrefix prefix ->
                Ranges r = parameterToRanges.computeIfAbsent(parameterNoPrefix, { p ->
                    p in dateFields 
                            ? Ranges.date(p, whelk.getTimezone(), whelk) 
                            : Ranges.nonDate(p, whelk) 
                })
                
                values.each { it.tokenize(',').each { r.add(prefix, it.trim()) } }
                handledParameters.add(parameter)
            }
        }

        def filters = parameterToRanges.values().collect { it.toQuery() }

        return new Tuple2(handledParameters, filters)
    }

    private static void parseRangeParameter (String parameter, Closure handler) {
        for (RangeParameterPrefix p : RangeParameterPrefix.values()) {
            if (parameter.startsWith(p.prefix())) {
                handler(parameter.substring(p.prefix().size()), p)
                return
            }
        }
    }

    static Set getFieldsOfType(String type, Map mappings) {
        Set fields = [] as Set
        DocumentUtil.findKey(mappings['properties'], 'type') { value, path ->
            if (value == type) {
                fields.add(path.dropRight(1).findAll{ it != 'properties'}.join('.'))
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
        Map fields = fieldSettings.get('fields')
        if (fields && fields.get('keyword')) {
            result.add(currentField)
        }
        Map properties = fieldSettings.get('properties')
        if (properties) {
            result += getKeywordFieldsFromProperties(properties, currentField)
        }
        return result
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
}
