package whelk.component

import spock.lang.Specification


class ElasticSearchSpec extends Specification {

    def "Should be configurable"() {
        when:
        def es1 = new ElasticSearch("localhost", null, "whelk_1")
        then:
        es1.elasticHostName == "localhost"
        es1.elasticHostPort == 9200
        es1.indexName == "whelk_1"
        when:
        def es2 = new ElasticSearch("otherhost:9999", null, "whelk_2")
        then:
        es2.elasticHostName == "otherhost"
        es2.elasticHostPort == 9999
        es2.indexName == "whelk_2"
    }

    def "Should make a nested request to ElasticSearch"() {
        when:
        Map queryParameters = ['identifiedBy.@type': ['ISBN'], 'identifiedBy.value': ['1234']]
        Map expected = [from:0,
                        size:0,
                        'query':
                           ['bool':
                               ['must':
                                   [['nested':['path': 'identifiedBy',
                                               'query':['bool':['must':[['match':['identifiedBy.@type': 'ISBN']],
                                                                        ['match':['identifiedBy.value': '1234']]]]]]]]]]
        ]

        then:
        assert ElasticSearch.createJsonDsl(queryParameters, 0, 0) == expected
    }

    def "Should make a nested and a simple query request to ElasticSearch"() {
        when:
        Map queryParameters = ['identifiedBy.@type': ['ISBN'], 'identifiedBy.value': ['9781412980319'], 'q':['tona ner reggaeprofilen']]
        Map expected = [from:0,
                        size:0,
                        'query':
                                ['bool':
                                         ['must':
                                                  [
                                                          [simple_query_string:[query:'tona ner reggaeprofilen', default_operator:'and']],
                                                          ['nested':['path': 'identifiedBy',
                                                              'query':['bool':['must':[['match':['identifiedBy.@type': 'ISBN']],
                                                                                       ['match':['identifiedBy.value': '9781412980319']]]]]]]]]]
        ]

        then:
        assert ElasticSearch.createJsonDsl(queryParameters, 0, 0) == expected
    }

    def "Should make both a nested and a Should request to ElasticSearch"() {
        when:
        Map queryParameters = ['identifiedBy.@type': ['ISBN'] as String[],
                               'identifiedBy.value': ['1234'] as String[],
                               'foo.type': ['Foo'] as String[],
                               'bar': ['Bar'] as String[]]
        Map expected = [from:0,
                        size:0,
                        'query':
                                ['bool':
                                         ['must':
                                                  [['nested':['path': 'identifiedBy',
                                                              'query':['bool':['must':[['match':['identifiedBy.@type': 'ISBN']],
                                                                                       ['match':['identifiedBy.value': '1234']]]]]]],
                                                   [bool:[should:[[match:['foo.type':'Foo']]], minimum_should_match:1]],
                                                   [bool:[should:[[match:['bar':'Bar']]], minimum_should_match:1]]]]]
        ]

        then:
        assert ElasticSearch.createJsonDsl(queryParameters, 0, 0) == expected
    }

    def "Should make a Should request to ElasticSearch"() {
        when:
        Map queryParameters = ['type': ['Foo Baz'] as String[], 'name': ['Bar Fizz'] as String[]]
        Map expected = [from:0,
                        size:0,
                        query:
                           [bool:
                              [must:[
                                     [bool:[should:[[match:[type:'Foo Baz']]], minimum_should_match:1]],
                                     [bool:[should:[[match:[name:'Bar Fizz']]], minimum_should_match:1]]]]]
        ]

        then:
        assert ElasticSearch.createJsonDsl(queryParameters, 0, 0) == expected
    }

}

