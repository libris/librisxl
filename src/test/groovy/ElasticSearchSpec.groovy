package whelk.component

import groovy.json.JsonOutput
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.QueryShardContext
import org.elasticsearch.index.query.WrapperQueryBuilder
import spock.lang.Specification

//import whelk.ElasticSearch

class ElasticSearchSpec extends Specification {


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

    def "ES api should be able to parse our queries.."(){
        when:
            String dsl = """{
    "from": 0,
    "size": 200,
    "query": {
        "bool": {
            "must": [
                {
                    "match_all": {}
                }
            ]
        }
    },
    "aggs": {
        "instanceOf.language.@id": {
            "terms": {
                "field": "instanceOf.language.@id",
                "size": 10,
                "order": {
                    "_count": "asc"
                }
            }
        },
        "carrierType": {
            "terms": {
                "field": "carrierType",
                "size": 10,
                "order": {
                    "_count": "asc"
                }
            }
        },
        "instanceOf.@type": {
            "terms": {
                "field": "instanceOf.@type",
                "size": 10,
                "order": {
                    "_count": "asc"
                }
            }
        },
        "instanceOf.contentType.@id": {
            "terms": {
                "field": "instanceOf.contentType.@id",
                "size": 10,
                "order": {
                    "_count": "asc"
                }
            }
        },
        "publication.date.raw": {
            "terms": {
                "field": "publication.date.raw",
                "size": 10,
                "order": {
                    "_count": "asc"
                }
            }
        },
        "@type": {
            "terms": {
                "field": "@type",
                "size": 10,
                "order": {
                    "_count": "asc"
                }
            }
        }
    }
}"""
        WrapperQueryBuilder builder = QueryBuilders.wrapperQuery(JsonOutput.toJson(dsl))


        then:
        assert query
    }

}

