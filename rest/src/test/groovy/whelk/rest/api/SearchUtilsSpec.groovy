package whelk.rest.api

import spock.lang.Specification

import whelk.rest.api.SearchUtils
import whelk.rest.api.SearchUtils.SearchType
import whelk.exception.InvalidQueryException

class SearchUtilsSpec extends Specification {

    void setup() {

    }


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

    def "Should make site filter"() {
        when:
        String url = "http://example.com"
        Map expected = ['should': [['prefix': ['@id': url]],
                                   ['prefix': ['sameAs.@id': url]]],
                        'minimum_should_match': 1]
        SearchUtils search = new SearchUtils(null, null, null)
        then:
        assert search.makeSiteFilter(url) == expected
    }

    def "Should build aggregation query"() {
        when:
        Map tree = ['@type': []]
        Map expected = ['@type': ['terms': ['field': '@type',
                                            'size': 10],
                                  'aggs': [:]]]
        SearchUtils search = new SearchUtils(null, null, null)
        then:
        assert search.buildAggQuery(tree) == expected
    }

    def "Should make find URL"() {
        when:
        SearchUtils search = new SearchUtils(null, null, null)
        then:
        assert search.makeFindUrl(type, params) == result
        where:
        params                       | type                         | result
        [:]                          | SearchType.ELASTIC           | '/find?q=*'
        // 'q' is special in that it's never ever a list
        // (which is the caller's responsibility)
        ['q': 'Tove']                | SearchType.ELASTIC           | '/find?q=Tove'
        ['a': ['1']]                 | SearchType.ELASTIC           | '/find?q=*&a=1'
        ['a': '1']                   | SearchType.ELASTIC           | '/find?q=*&a=1'
        ['a': ['1', '2']]            | SearchType.ELASTIC           | '/find?q=*&a=1&a=2'
        ['a': ['1'], 'b': ['2']]     | SearchType.ELASTIC           | '/find?q=*&a=1&b=2'
        ['a': null, 'b': ['2']]      | SearchType.ELASTIC           | '/find?q=*&b=2'
        // as are 'p', 'o', and 'value'
        ['p': 'foo', 'o': 'bar']     | SearchType.FIND_BY_RELATION  | '/find?p=foo&o=bar'
        ['p': 'foo', 'value': 'bar'] | SearchType.FIND_BY_VALUE     | '/find?p=foo&value=bar'
        ['o': 'bar']                 | SearchType.FIND_BY_QUOTATION | '/find?o=bar'
    }

    def "Should make find URL with offset"() {
        when:
        SearchUtils search = new SearchUtils(null, null, null)
        then:
        assert search.makeFindUrl(type, params, offset) == result
        where:
        params | offset | type               | result
        [:]    | 0      | SearchType.ELASTIC | '/find?q=*'
        [:]    | 10     | SearchType.ELASTIC | '/find?q=*&_offset=10'
    }

    def "Should get limit and offset"() {
        when:
        SearchUtils search = new SearchUtils(null, null, null)
        then:
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
        SearchUtils search = new SearchUtils(null, null, null)

        when:
        search.getLimitAndOffset(['_limit': '-1'])

        then:
        thrown InvalidQueryException
    }

    def "Should throw on negative offset"() {
        given:
        SearchUtils search = new SearchUtils(null, null, null)

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
}
