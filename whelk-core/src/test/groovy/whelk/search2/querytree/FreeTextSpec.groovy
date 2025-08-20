package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.ESSettings

class FreeTextSpec extends Specification {
    def "to ES query (basic boosting)"() {
        given:
        Map boostSettings = [
                "field_boost": [
                        "fields": [
                                [
                                        "name" : "field1",
                                        "boost": 10
                                ],
                                [
                                        "name" : "field2",
                                        "boost": 20
                                ]
                        ],
                        "analyze_wildcard": true
                ]
        ]
        Map esQuery = new FreeText("something").toEs(new ESSettings.Boost(boostSettings).fieldBoost())

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
        Map boostSettings = [
                "field_boost": [
                        "fields": [
                                [
                                        "name" : "field1",
                                        "boost": 10,
                                        "script_score": [
                                                "name"    : "a function",
                                                "function": "f1(_score)"
                                        ]
                                ],
                                [
                                        "name" : "field2",
                                        "boost": 20,
                                        "script_score": [
                                                "name"    : "a function",
                                                "function": "f1(_score)"
                                        ]
                                ],
                                [
                                        "name" : "field3",
                                        "boost": 10,
                                        "script_score": [
                                                "name"    : "another function",
                                                "function": "f2(_score)",
                                                "apply_if": "condition"
                                        ]
                                ]
                        ],
                        "analyze_wildcard": true
                ]
        ]
        Map esQuery = new FreeText("something").toEs(new ESSettings.Boost(boostSettings).fieldBoost())

        expect:
        esQuery == [
                'bool': [
                        'should': [
                                [
                                        "script_score": [
                                                "script": [
                                                        "source": "f1(_score)"
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
                                                        "source": "condition ? f2(_score) : _score"
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
        Map boostSettings = [
                "field_boost": [
                        "fields": [
                                [
                                        "name" : "field1",
                                        "boost": 10
                                ],
                                [
                                        "name" : "field2",
                                        "boost": 20
                                ],
                                [
                                        "name" : "field3",
                                        "boost": 10,
                                        "script_score": [
                                                "name"    : "a function",
                                                "function": "f(_score)"
                                        ]
                                ]
                        ],
                        "analyze_wildcard": true
                ]
        ]
        Map esQuery = new FreeText("something").toEs(new ESSettings.Boost(boostSettings).fieldBoost())

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
                                                        "source": "f(_score)"
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
