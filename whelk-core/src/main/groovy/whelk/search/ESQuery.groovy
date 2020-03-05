package whelk.search

import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.transform.TypeCheckingMode
import groovy.util.logging.Log4j2 as Log
import org.codehaus.jackson.map.ObjectMapper
import whelk.JsonLd
import whelk.Whelk
import whelk.util.DocumentUtil
import whelk.util.Unicode

@CompileStatic
@Log
class ESQuery {
    private Whelk whelk
    private JsonLd jsonld
    private Set keywordFields
    private Set dateFields
    private static final ObjectMapper mapper = new ObjectMapper()
    private static final int DEFAULT_PAGE_SIZE = 50
    private static final List RESERVED_PARAMS = [
        'q', 'o', '_limit', '_offset', '_sort', '_statsrepr', '_site_base_uri', '_debug', '_boost', '_lens'
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
            this.dateFields = getDateFields(mappings)
        } else {
            this.keywordFields = Collections.emptySet()
            this.dateFields = Collections.emptySet()
        }
    }

    void setKeywords(Set keywordFields) {
        // NOTE: For unit tests only!
        this.keywordFields = keywordFields
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    Map doQuery(Map<String, String[]> queryParameters, String dataset) {
        Map esQuery = getESQuery(queryParameters)
        Map esResponse = hideKeywordFields(whelk.elastic.query(esQuery, dataset))
        if ('esQuery' in queryParameters.get('_debug')) {
            esResponse._debug = [esQuery: esQuery]
        }
        return esResponse
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    Map doQueryIds(Map<String, String[]> queryParameters, String dataset) {
        Map esQuery = getESQuery(queryParameters)
        Map esResponse = hideKeywordFields(whelk.elastic.queryIds(esQuery, dataset))
        if ('esQuery' in queryParameters.get('_debug')) {
            esResponse._debug = [esQuery: esQuery]
        }
        return esResponse
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    Map getESQuery(Map<String, String[]> queryParameters) {
        // Legit params and their uses:
        //   q - simple_query_string
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

        if (originalTypeParam != null) {
            queryParameters.put('@type', originalTypeParam)
        }

        Map simpleQuery = [
            'simple_query_string': [
                'query': q,
                'default_operator':  'AND'
            ]
        ]

        Map queryClauses = simpleQuery

        String[] boostParam = queryParameters.get('_boost')
        String boostMode = boostParam ? boostParam[0] : null
        List boostedFields = getBoostFields(originalTypeParam, boostMode)

        if (boostedFields) {
            def softFields = boostedFields.findAll {
                it.contains(JsonLd.SEARCH_KEY)
            }
            def exactFields = boostedFields.findResults {
                it.replace(JsonLd.SEARCH_KEY, "${JsonLd.SEARCH_KEY}.exact")
            }

            Map boostedExact = [
                'simple_query_string': [
                    'query': q,
                    'default_operator':  'AND',
                    'fields': exactFields
                ]
            ]

            Map boostedSoft = [
                'simple_query_string': [
                    'query': q,
                    'default_operator':  'AND',
                    'fields': softFields,
                    'quote_field_suffix': ".exact"
                ]
            ]

            queryClauses = [
                'bool': ['should': [
                    boostedExact,
                    boostedSoft,
                    simpleQuery
                ]]
            ]
        }

        Map query = [
            'query': [
                'bool': [
                    'must': [
                        queryClauses
                    ]
                ]
            ]
        ]

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

        return query
    }

    List<String> getBoostFields(String[] types, String boostMode) {
        if (boostMode?.indexOf('^') > -1) {
            return boostMode.tokenize(',')
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
                boostFields = lensBoost.computeBoostFieldsFromLenses(types)
            }
            boostFieldsByType[typeKey] = boostFields
        }

        return boostFields
    }

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
    public List getSiteFilter(Map<String, String[]> queryParameters) {
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

        makeRangeFilters(queryParametersCopy, filters)

        // TODO This is copied from the old code and should be rewritten.
        // We don't have that many nested mappings, so this is way to general.
        Map groups = queryParametersCopy.groupBy { p -> getPrefixIfExists(p.key) }
        Map nested = getNestedParams(groups)
        List notNested = (groups - nested).collect { it.value }

        nested.each { key, vals ->
            filters << createNestedBoolFilter(key, vals)
        }

        notNested.each { Map m ->
            m.each {k, v ->
                if (!(k in RESERVED_PARAMS)) {
                    filters << createBoolFilter(k, v as String[])
                }
            }
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

    @CompileStatic(TypeCheckingMode.SKIP)
    private Map getNestedParams(Map groups) {
        // TODO We hardcode `identifiedBy` here, since that's currently the only
        // type of search in the client where we need nesting.
        Map nested = groups.findAll { g ->
            g.key == 'identifiedBy' &&
            g.value.size() == 2
        }
        return nested
    }

    @CompileStatic(TypeCheckingMode.SKIP)
    private Map createNestedBoolFilter(String prefix, Map nestedQuery) {
        Map result = [:]

        Map musts = ['must': nestedQuery.collect { q -> ['match': [(q.key): q.value.first()]] }]

        result << [['nested': ['path': prefix,
                               'query': ['bool': musts]]]]

        return result
    }

    /**
     * Create a query to filter docs where `field` matches any of the
     * supplied `vals`
     *
     * Public for test only - don't call outside this class!
     *
     */
    @CompileStatic(TypeCheckingMode.SKIP)
    public Map createBoolFilter(String field, String[] vals) {
        List clauses = []
        for (val in vals) {
            clauses.add(['simple_query_string': [
                'query': val,
                'fields': [field],
                'default_operator': 'AND'
            ]])
        }
        return ['bool': ['should': clauses]]
    }

    /**
     * Create an agg query based on the `_statsrepr` param, or default if not
     * supplied.
     *
     * Public for test only - don't call outside this class!
     *
     */
    @CompileStatic(TypeCheckingMode.SKIP)
    public Map getAggQuery(Map queryParameters) {
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
            String sort = tree[key]?.sort =='key' ? '_term' : '_count'
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
     * Construct "range query" filters for parameters prefixed with any {@link ParameterPrefix}
     * Ranges for different parameters are combined with AND
     * Multiple ranges for the same parameter are combined with OR
     *
     * Range endpoints are matched in the order they appear
     * e.g. minEx-x=1984&maxEx-x=1988&minEx-x=1993&min-x=2000&maxEx-x=1995
     * means 1984 < x < 1988 OR 1993 < x < 1995 OR x >= 2000
     */
    @CompileDynamic // compiler doesn't get the collectMany construct below...
    void makeRangeFilters(Map<String, String[]> queryParameters, List filters) {
        Map<String, Ranges> ranges = [:]
        Set<String> handledParameters = new HashSet<>()

        queryParameters.each { parameter, values ->
            parseRangeParameter(parameter) { String nameNoPrefix, ParameterPrefix prefix ->
                Ranges r = ranges.computeIfAbsent(nameNoPrefix,
                        { p -> p in dateFields ? Ranges.date(p, whelk.getTimezone()) : Ranges.nonDate(p)})
                values.collectMany{ it.tokenize(',') }.each { r.add(prefix, it.trim()) }
                handledParameters.add(parameter)
            }
        }

        handledParameters.each {queryParameters.remove(it)}
        filters.addAll(ranges.values().collect {it.toQuery()})
    }

    private void parseRangeParameter (String parameter, Closure handler) {
        for (ParameterPrefix p : ParameterPrefix.values()) {
            if (parameter.startsWith(p.prefix())) {
                handler(parameter.substring(p.prefix().size()), p)
                return
            }
        }
    }

    Set getDateFields(Map mappings) {
        Set dateFields = [] as Set
        DocumentUtil.findKey(mappings['_default_']['properties'], 'type') { value, path ->
            if (value == 'date') {
                dateFields.add(path.dropRight(1).findAll{ it != 'properties'}.join('.'))
            }
            DocumentUtil.NOP
        }
        return dateFields
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
    public Set getKeywordFields(Map mappings) {
        Set keywordFields = [] as Set
        mappings.each { docType, docMappings ->
            keywordFields += getKeywordFieldsFromProperties(docMappings['properties'] as Map)
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
    public Map hideKeywordFields(Map esResponse) {
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
}
