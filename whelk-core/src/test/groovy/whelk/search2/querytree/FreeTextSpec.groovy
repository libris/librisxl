package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.EsMappings

class FreeTextSpec extends Specification {
    EsMappings esMappings = TestData.getEsMappings()

    def "to ES query (basic boosting)"() {
        given:
        List<String> boostFields = ["field1^10", "field2^20"]
        Map esQuery = new FreeText("something").toEs(esMappings, boostFields)

        expect:
        esQuery == [
                "simple_query_string": [
                        "default_operator": "AND",
                        "query"           : "something",
                        "analyze_wildcard": true,
                        "fields"          : [
                                "field1^10.0",
                                "field2^20.0"
                        ]
                ]
        ]
    }

    def "to ES query (function boosting)"() {
        given:
        List<String> boostFields = ["field1^10(someFunc)", "field2^20(someFunc)", "field3^10(another(func))"]
        Map esQuery = new FreeText("something").toEs(esMappings, boostFields)

        expect:
        esQuery == [
                'bool': [
                        'should': [
                                [
                                        "script_score": [
                                                "script": [
                                                        "source": "someFunc"
                                                ],
                                                "query" : [
                                                        "simple_query_string": [
                                                                "default_operator": "AND",
                                                                "query"           : "something",
                                                                "analyze_wildcard": true,
                                                                "fields"          : [
                                                                        "field1^10.0",
                                                                        "field2^20.0",
                                                                        "field3^0.0"
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ],
                                [
                                        "script_score": [
                                                "script": [
                                                        "source": "another(func)"
                                                ],
                                                "query" : [
                                                        "simple_query_string": [
                                                                "default_operator": "AND",
                                                                "query"           : "something",
                                                                "analyze_wildcard": true,
                                                                "fields"          : [
                                                                        "field1^0.0",
                                                                        "field2^0.0",
                                                                        "field3^10.0"
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ]
    }

    def "to ES query (basic boosting + function boosting)"() {
        given:
        List<String> boostFields = ["field1^10", "field2^20", "field3^10(someFunc)"]
        Map esQuery = new FreeText("something").toEs(esMappings, boostFields)

        expect:
        esQuery == [
                'bool': [
                        'should': [
                                [
                                        "simple_query_string": [
                                                "default_operator": "AND",
                                                "query"           : "something",
                                                "analyze_wildcard": true,
                                                "fields"          : [
                                                        "field1^10.0",
                                                        "field2^20.0",
                                                        "field3^0.0"
                                                ]
                                        ]
                                ],
                                [
                                        "script_score": [
                                                "script": [
                                                        "source": "someFunc"
                                                ],
                                                "query" : [
                                                        "simple_query_string": [
                                                                "default_operator": "AND",
                                                                "query"           : "something",
                                                                "analyze_wildcard": true,
                                                                "fields"          : [
                                                                        "field1^0.0",
                                                                        "field2^0.0",
                                                                        "field3^10.0"
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ]
    }
}
