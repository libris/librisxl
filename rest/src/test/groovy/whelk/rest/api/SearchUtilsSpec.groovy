package whelk.rest.api

import spock.lang.Ignore
import spock.lang.Shared
import spock.lang.Specification
import whelk.JsonLd
import whelk.exception.InvalidQueryException
import whelk.rest.api.SearchUtils.SearchType

class SearchUtilsSpec extends Specification {

    @Shared SearchUtils search = new SearchUtils(new JsonLd([:], [:], [:]))

    def "aa"(){
        when:
        def uri =  "http://localhost/3ld03n3f023w1gd#it"
        def uris =  "https://localhost/3ld03n3f023w1gd#it"
        def urir =  "httpr://localhost/3ld03n3f023w1gd#it"
        def pattern = $/^https?:///$
        then:
        assert uri =~ pattern
        assert uris =~ pattern
        assert !(urir =~ pattern)
    }

    def "Should build aggregation query"() {
        when:
        Map tree = ['@type': []]
        Map expected = ['@type': ['terms': ['field': '@type',
                                            'size' : 10,
                                            'order': ['_count': 'desc']]]]
        then:
        assert search.buildAggQuery(tree) == expected
    }

    def "Should build aggregation query with supplied size parameter"() {
        when:
        Map tree = ['@type': ['size': 20]]
        Map expected = ['@type': ['terms': ['field': '@type',
                                            'size' : 20,
                                            'order': ['_count': 'desc']]]]
        then:
        assert search.buildAggQuery(tree) == expected
    }

    def "Should build aggregation query with supplied Sort-parmeter set to key(term) ascending"() {
        when:
        Map tree = ['@type': ['size': 2000, 'sort': 'key', 'sortOrder':'asc' ]]
        Map expected = ['@type': ['terms': [
                'field': '@type',
                'size' : 2000,
                'order': ['_key': 'asc']]]]

        then:
        assert search.buildAggQuery(tree) == expected
    }

    def "Should build aggregation query with supplied Sort-parmeter for value descending"() {
        when:
        Map tree = ['@type': ['sort': 'value', 'sortOrder':'desc' ]]
        Map expected = ['@type': ['terms': [
                'field': '@type',
                'size' : 10,
                'order': ['_count': 'desc']]]]

        then:
        assert search.buildAggQuery(tree) == expected
    }

    @Ignore
    def "Should build observations based on ES aggregate"() {
        when:
        Map aggregate = [
                "publication_date": [
                        "doc_count_error_upper_bound": 0,
                        "sum_other_doc_count"        : 381,
                        "buckets"                    : [
                                [
                                        "key"      : "1482",
                                        "doc_count": 2
                                ],
                                [
                                        "key"      : "1525",
                                        "doc_count": 1
                                ]
                        ]
                ]
        ]
        Map actual = search.addSlices([:], aggregate, 'localhost')
        then:
        assert actual

    }

    def "Should make find URL"() {
        expect:
        assert search.makeFindUrl(type, params) == result
        where:
        params                         | type                         | result
        [:]                            | SearchType.ELASTIC           | '/find?q=*'
        // 'q' is special in that it's never ever a list
        // (which is the caller's responsibility)
        ['q': 'Tove']                  | SearchType.ELASTIC           | '/find?q=Tove'
        ['q': 'Tove Jansson']          | SearchType.ELASTIC           | '/find?q=Tove+Jansson'
        ['q': 'functions & duties']    | SearchType.ELASTIC           | '/find?q=functions+%26+duties'
        ['q': 'ftp://test']            | SearchType.ELASTIC           | '/find?q=ftp://test'
        ['q': 'hej', '@type': 'Work']  | SearchType.ELASTIC           | '/find?q=hej&@type=Work'
        ['a': ['1']]                   | SearchType.ELASTIC           | '/find?q=*&a=1'
        ['a': '1']                     | SearchType.ELASTIC           | '/find?q=*&a=1'
        ['a': ['1', '2']]              | SearchType.ELASTIC           | '/find?q=*&a=1&a=2'
        ['a': ['1'], 'b': ['2']]       | SearchType.ELASTIC           | '/find?q=*&a=1&b=2'
        ['a': null, 'b': ['2']]        | SearchType.ELASTIC           | '/find?q=*&b=2'
    }

    def "Should make find URL with offset"() {
        expect:
        assert search.makeFindUrl(type, params, offset) == result
        where:
        params | offset | type               | result
        [:]    | 0      | SearchType.ELASTIC | '/find?q=*'
        [:]    | 10     | SearchType.ELASTIC | '/find?q=*&_offset=10'
    }

    def "Should get limit and offset"() {
        expect:
        assert search.getLimitAndOffset(params) == result
        where:
        params                                        | result
        [:]                                           | new Tuple2(SearchUtils.DEFAULT_LIMIT, SearchUtils.DEFAULT_OFFSET)
        ['_limit': '5']                               | new Tuple2(5, SearchUtils.DEFAULT_OFFSET)
        ['_limit': ['5']]                             | new Tuple2(5, SearchUtils.DEFAULT_OFFSET)
        ['_limit': ['5'] as String[]]                 | new Tuple2(5, SearchUtils.DEFAULT_OFFSET)
        ['_limit': '100000000']                       | new Tuple2(SearchUtils.DEFAULT_LIMIT, SearchUtils.DEFAULT_OFFSET)
        ['_offset': '5']                              | new Tuple2(SearchUtils.DEFAULT_LIMIT, 5)
        ['_limit': '5', 'foo': 'bar', '_offset': '5'] | new Tuple2(5, 5)
        ['_limit': 'foo']                             | new Tuple2(SearchUtils.DEFAULT_LIMIT, SearchUtils.DEFAULT_OFFSET)

    }

    def "Should throw on negative limit"() {
        given:

        when:
        search.getLimitAndOffset(['_limit': '-1'])

        then:
        thrown InvalidQueryException
    }

    def "Should throw on negative offset"() {
        given:

        when:
        search.getLimitAndOffset(['_offset': '-1'])

        then:
        thrown InvalidQueryException
    }

    def "Should compute offsets, I"() {
        when:
        int total = 10
        int limit = 1
        int offset = 2
        def offsets = new Offsets(total, limit, offset)
        then:
        assert offsets.prev == 1
        assert offsets.next == 3
        assert offsets.last == 9
    }

    def "Should compute offsets, Ib"() {
        expect:
        new Offsets(total, limit, offset).last == last
        where:
        total | limit | offset | last
        10    | 1     | 0      | 9
        10    | 1     | 2      | 9
        10    | 1     | 9      | 9
        10    | 1     | 10     | 10
        10    | 2     | 0      | 8
        10    | 2     | 8      | 8
        10    | 2     | 9      | 9
        10    | 2     | 10     | 10
        10    | 3     | 0      | 9
        10    | 3     | 6      | 9
        10    | 3     | 7      | 7
        10    | 3     | 8      | 8
        10    | 3     | 9      | 9
        10    | 3     | 10     | 10
        10    | 3     | 11     | 11
        10    | 5     | 0      | 5
        10    | 5     | 4      | 5
        10    | 5     | 6      | 6
    }


    def "Should compute offsets, II"() {
        when:
        int total = 1
        int limit = 1
        int offset = 0
        def offsets = new Offsets(total, limit, offset)
        then:
        assert offsets.prev == null
        assert offsets.next == null
        assert offsets.last == 0
    }

    def "Should compute offsets, III"() {
        when:
        int total = 52
        int limit = 20
        int offset = 0
        def offsets = new Offsets(total, limit, offset)
        then:
        assert offsets.prev == null
        assert offsets.next == 20
        assert offsets.last == 40
    }

    def "Should compute offsets, IV"() {
        when:
        int total = 52
        int limit = 20
        int offset = 20
        def offsets = new Offsets(total, limit, offset)
        then:
        assert offsets.prev == 0
        assert offsets.next == 40
        assert offsets.last == 40
    }

    def "Should compute offsets, V"() {
        when:
        int total = 52
        int limit = 20
        int offset = 40
        def offsets = new Offsets(total, limit, offset)
        then:
        assert offsets.prev == 20
        assert offsets.next == null
        assert offsets.last == 40
    }

    def "Should compute offsets, VI"() {
        when:
        int total = 50
        int limit = 10
        int offset = 40
        def offsets = new Offsets(total, limit, offset)
        then:
        assert offsets.prev == 30
        assert offsets.next == null
        assert offsets.last == 40
    }

    def "Should throw when computing offsets with negative limit"() {
        when:
        int total = 50
        int limit = -1
        int offset = 5
        def offsets = new Offsets(total, limit, offset)
        then:
        thrown InvalidQueryException
    }

    def "should throw when computing offsets with limit=0"() {
        when:
        int total = 50
        int limit = 0
        int offset = 5
        def offsets = new Offsets(total, limit, offset)
        then:
        thrown InvalidQueryException
    }

    def "should throw when computing offsets with negative offset"() {
        when:
        int total = 50
        int limit = 5
        int offset = -1
        def offsets = new Offsets(total, limit, offset)
        then:
        thrown InvalidQueryException
    }

    def "should remove a mapping from params"() {
        expect:
        search.removeMappingFromParams(params, mapping) == expected
        where:
        params                    | mapping                                         | expected
        [type: 'Thing']           | [variable: 'type', object: ['@id': 'Thing']]    | [:]
        [type: ['Thing']]         | [variable: 'type', object: ['@id': 'Thing']]    | [:]
        [type: ['Thing', 'Item']] | [variable: 'type', object: ['@id': 'Thing']]    | [type: ['Item']]
        [key: 'value']            | [variable: 'type', object: ['@id': 'Thing']]    | [key: 'value']
        [a: 'A', 'b': 'B']        | [variable: 'a', value: 'A']                     | [b: 'B']
        [a: ['A', 'a'], 'b': 'B']|  [variable: 'a', value: 'a']                     | [a: ['A'], 'b': 'B']
    }

}
