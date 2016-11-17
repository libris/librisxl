package whelk.rest.api

import spock.lang.Specification

import whelk.rest.api.SearchUtils


class SearchUtilsSpec extends Specification {

    void setup() {

    }

    def "Should make site filter"() {
        when:
        String url = "http://example.com"
        Map expected = ['should': [['prefix': ['@id': url]],
                                   ['prefix': ['sameAs.@id': url]]],
                        'minimum_should_match': 1]
        then:
        assert SearchUtils.makeSiteFilter(url) == expected
    }

    def "Should build aggregation query"() {
        when:
        Map tree = ['@type': []]
        Map expected = ['@type': ['terms': ['field': '@type',
                                            'size': 1000],
                                  'aggs': [:]]]
        then:
        assert SearchUtils.buildAggQuery(tree) == expected
    }

    def "Should make find URL"() {
        expect:
        assert SearchUtils.makeFindUrl(params) == result
        where:
        params                   | result
        [:]                      | '/find?q=*'
        // 'q' is special in that it's never ever a list
        // (which is the caller's responsibility)
        ['q': 'Tove']            | '/find?q=Tove'
        ['a': ['1']]             | '/find?q=*&a=1'
        ['a': '1']               | '/find?q=*&a=1'
        ['a': ['1', '2']]        | '/find?q=*&a=1&a=2'
        ['a': ['1'], 'b': ['2']] | '/find?q=*&a=1&b=2'
        ['a': null, 'b': ['2']]  | '/find?q=*&b=2'
    }

    def "Should make find URL with offset"() {
        expect:
        assert SearchUtils.makeFindUrl(params, offset) == result
        where:
        params | offset | result
        [:]    | 0      | '/find?q=*'
        [:]    | 10     | '/find?q=*&_offset=10'
    }

    def "Should get limit and offset"() {
        expect:
        assert SearchUtils.getLimitAndOffset(params) == result
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

    def "Should compute offsets, I"() {
        when:
        int total = 10
        int limit = 1
        int offset = 2
        def offsets = new Offsets(total, limit, offset)
        then:
        assert offsets.prev == 1
        assert offsets.next == 3
        assert offsets.last == 10
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
}