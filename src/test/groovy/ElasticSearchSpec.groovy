package whelk.component

import spock.lang.Specification

//import whelk.ElasticSearch

class ElasticSearchSpec extends Specification {


    def "Should make a nested request to ElasticSearch"() {
        when:
        Map queryParameters = ['identifiedBy.@type': ['ISBN'], 'identifiedBy.value': ['9781412980319']]
        Map expected = [from:0,
                        size:0,
                        'query':
                           ['bool':
                               ['must':
                                   [['nested':['path': 'identifiedBy',
                                               'query':['bool':['must':[['match':['identifiedBy.@type': 'ISBN']],
                                                                        ['match':['identifiedBy.value': '9781412980319']]]]]]]]]]
        ]

        then:
        assert ElasticSearch.createJsonDsl(queryParameters, 0, 0) == expected
    }

    def "Should make both a nested and a should request to ElasticSearch"() {
        when:
        Map queryParameters = ['identifiedBy.@type': ['ISBN'] as String[],
                               'identifiedBy.value': ['1234'] as String[], 'type': ['Foo'] as String[]]
        Map expected = [from:0,
                        size:0,
                        'query':
                                ['bool':
                                         ['must':
                                                  [['nested':['path': 'identifiedBy',
                                                              'query':['bool':['must':[['match':['identifiedBy.@type': 'ISBN']],
                                                                                       ['match':['identifiedBy.value': '1234']]]]]]],
                                                   [bool:[should:[[match:[type:['Foo']]]], minimum_should_match:1]]]]]
        ]

        then:
        assert ElasticSearch.createJsonDsl(queryParameters, 0, 0) == expected
    }

}

