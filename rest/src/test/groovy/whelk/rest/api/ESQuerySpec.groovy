package whelk.rest.api

import spock.lang.Ignore
import spock.lang.Specification

import whelk.rest.api.ESQuery
import whelk.JsonLd


class ESQuerySpec extends Specification {
    ESQuery es
    void setup() {
        es = new ESQuery()
        es.setKeywords(['bar'] as Set)
    }

    def "should get query string"() {
        expect:
        es.getQueryString(params) == query
        where:
        params                          | query
        ['foo': ['bar']]                | '*'
        ['q': ['abcd'], 'foo': ['bar']] | 'abcd'
    }

    def "should get pagination params"() {
        expect:
        es.getPaginationParams(params) == result
        where:
        params                                        | result
        // Default limit is 50
        ['foo': ['bar']]                              | new Tuple2(50, 0)
        ['_limit': ['100']]                           | new Tuple2(100, 0)
        ['_offset': ['10']]                           | new Tuple2(50, 10)
        ['_limit': ['100', '200'], '_offset': ['10']] | new Tuple2(100, 10)
    }

    def "should get sort clauses"() {
        expect:
        es.getSortClauses(params) == result
        where:
        params                  | result
        [:]                     | null
        ['_sort': []]           | null
        // `bar` has a keyword field in the mappings
        ['_sort': ['foo,-bar']] | [['foo': ['order': 'asc']], ['bar.keyword': ['order': 'desc']]]
        ['_sort': ['hasTitle.mainTitle']] | [['hasTitle.mainTitle': ['order': 'asc',
                                                                     'nested_path': 'hasTitle',
                                                                     'nested_filter': ['term': ['hasTitle.@type': 'Title']]]]]
    }

    def "should get site filter"() {
        expect:
        es.getSiteFilter(params) == result
        where:
        params                                          | result
        [:]                                             | null
        ['_site_base_uri': []]                          | null
        ['_site_base_uri': ['https://foo.example.com']] | [['bool':
                                                               ['should': [
                                                                   ['prefix': ['@id': 'https://foo.example.com']],
                                                                   ['prefix': ['sameAs.@id': 'https://foo.example.com']]
                                                               ]]
                                                          ]]

    }

    def "should get filters"(Map<String, String[]> params, List result) {
        expect:
        es.getFilters(params) == result
        where:
        params                  | result
        [:]                     | null
        ['foo': ['bar', 'baz']] | [['bool': ['must': [['terms': ['foo': ['bar', 'baz']]]]]]]
    }

    def "should create bool filter"(String key, String[] vals, Map result) {
        expect:
        es.createBoolFilter(key, vals) == result
        where:
        key   | vals           | result
        'foo' | ['bar', 'baz'] | ['terms': ['foo': ['bar', 'baz']]]
    }

    def "should get agg query"() {
        when:
        Map emptyAggs = [:]
        Map emptyAggsResult = [(JsonLd.TYPE_KEY): ['terms': ['field': JsonLd.TYPE_KEY]]]
        Map simpleAggs = ['_statsrepr': ['{"foo": {"sort": "key", "sortOrder": "desc", "size": 5}}']]
        Map simpleAggsResult = ['foo': ['terms': ['field': 'foo', 'size': 5, 'order': ['_term': 'desc']]]]
        Map subAggs = ['_statsrepr': ['{"bar": {"subItems": {"foo": {"sort": "key"}}}}']]
        // `bar` has a keyword field in the mappings
        Map subAggsResult = ['bar.keyword': ['terms': ['field': 'bar.keyword',
                                               'size': 10,
                                               'order': ['_count': 'desc']],
                                     'aggs': [
                                        'foo': ['terms': ['field': 'foo',
                                                          'size': 10,
                                                          'order': ['_term': 'desc']]]
                                     ]]]

        then:
        es.getAggQuery(emptyAggs) == emptyAggsResult
        es.getAggQuery(simpleAggs) == simpleAggsResult
        es.getAggQuery(subAggs) == subAggsResult
    }

    def "should get keyword fields"() {
        when:
        Map emptyMappings = [:]
        Set emptyResult = [] as Set
        Map simpleMappings = [
            'bib': [
                'properties': [
                    'foo': [
                        'type': 'text',
                        'fields': [
                            'keyword': [
                                'type': 'keyword'
                            ]
                        ]
                    ]
                ]
            ]
        ]
        Set simpleResult = ['foo'] as Set
        Map nestedMappings = [
            'bib': [
                'properties': [
                    'foo': [
                        'type': 'text',
                        'fields': [
                            'keyword': [
                                'type': 'keyword'
                            ]
                        ],
                        'properties': [
                            '@type': [
                                'type': 'keyword'
                            ],
                            'bar': [
                                'properties': [
                                    'baz': [
                                        'type': 'text',
                                        'fields': [
                                            'keyword': [
                                                'type': 'keyword'
                                            ]
                                        ]
                                    ],
                                    'quux': [
                                        'type': 'keyword'
                                    ]
                                ]
                            ]
                        ]
                    ],
                    'baz': [
                        'type': 'keyword'
                    ]
                ]
            ]
        ]
        Set nestedResult = ['foo', 'foo.bar.baz'] as Set
        then:
        es.getKeywordFields(emptyMappings) == emptyResult
        es.getKeywordFields(simpleMappings) == simpleResult
        es.getKeywordFields(nestedMappings) == nestedResult
    }

    def "should expand @type param"() {
        when:
        Map context = [:]
        Map display = [:]
        Map vocab = ['@graph': [['@id': 'foo', '@type': 'Class'],
                                ['@id': 'bar', '@type': 'Class', 'subClassOf': [['@id': 'foo']]],
                                ['@id': 'baz', '@type': 'Class', 'subClassOf': [['@id': 'bar']]]]]
        JsonLd jsonld = new JsonLd(context, display, vocab)
        Map emptyQueryParameters = [:]
        Tuple2 emptyQueryParametersResult = new Tuple2(null, [:])
        Map noTypeQueryParameters = ['foo': ['bar']]
        Tuple2 noTypeQueryParametersResult = new Tuple2(null, ['foo': ['bar']])
        Map simpleQueryParameters = ['@type': ['bar']]
        Tuple2 simpleQueryParametersResult = new Tuple2(['bar'], ['@type': ['baz', 'bar']])
        Map simpleQueryParameters2 = ['@type': ['foo', 'baz']]
        Tuple2 simpleQueryParametersResult2 = new Tuple2(['foo', 'baz'], ['@type': ['baz']])
        then:
        es.expandTypeParam(emptyQueryParameters, jsonld) == emptyQueryParametersResult
        es.expandTypeParam(noTypeQueryParameters, jsonld) == noTypeQueryParametersResult
        es.expandTypeParam(simpleQueryParameters, jsonld) == simpleQueryParametersResult
        es.expandTypeParam(simpleQueryParameters2, jsonld) == simpleQueryParametersResult2
    }
}