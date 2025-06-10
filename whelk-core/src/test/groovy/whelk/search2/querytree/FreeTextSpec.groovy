package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate
import whelk.search2.EsBoost
import whelk.search2.EsMappings

class FreeTextSpec extends Specification {
    EsMappings esMappings = TestData.getEsMappings()
    Disambiguate disambiguate = TestData.getDisambiguate()

    def "to ES query (basic boosting)"() {
        given:
        List<String> boostFields = ["field1^10", "field2^20"]
        Map esQuery = new FreeText("something").toEs(esMappings, EsBoost.Config.newBoostFieldsConfig(boostFields))

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
        Map esQuery = new FreeText("something").toEs(esMappings, EsBoost.Config.newBoostFieldsConfig(boostFields))

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
        Map esQuery = new FreeText("something").toEs(esMappings, EsBoost.Config.newBoostFieldsConfig(boostFields))

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

    def "to ES query (with suggest)"() {
        given:
        int cursor
        EsBoost.Config boostConfig
        FreeText freeText = (FreeText) QueryTreeBuilder.buildTree("abc xyz", disambiguate)

        when:
        // First word is being edited
        cursor = 3
        boostConfig = new EsBoost.Config(["field1^10"], [], null, null, true, cursor)

        then:
        freeText.toEs(esMappings, boostConfig) == [
                "bool": [
                        "should": [[
                                           "simple_query_string": [
                                                   "default_operator": "AND",
                                                   "query"           : "abc* xyz",
                                                   "analyze_wildcard": true,
                                                   "fields"          : ["field1^10.0"]
                                           ]
                                   ], [
                                           "bool": [
                                                   "should": [[
                                                                      "simple_query_string": [
                                                                              "default_operator": "AND",
                                                                              "query"           : "abc xyz",
                                                                              "analyze_wildcard": true,
                                                                              "fields"          : ["field1^10.0"]
                                                                      ]
                                                              ], [
                                                                      "query_string": [
                                                                              "default_operator": "AND",
                                                                              "query"           : "\"abc xyz\"",
                                                                              "analyze_wildcard": true,
                                                                              "fields"          : ["field1^10.0"],
                                                                              "type"            : "most_fields"
                                                                      ]
                                                              ]]
                                           ]
                                   ]]
                ]
        ]

        when:
        // No word is being edited
        cursor = 4
        boostConfig = new EsBoost.Config(["field1^10"], [], null, null, true, cursor)

        then:
        freeText.toEs(esMappings, boostConfig) == [
                "bool": [
                        "should": [[
                                           "simple_query_string": [
                                                   "default_operator": "AND",
                                                   "query"           : "abc xyz",
                                                   "analyze_wildcard": true,
                                                   "fields"          : ["field1^10.0"]
                                           ]
                                   ], [
                                           "query_string": [
                                                   "default_operator": "AND",
                                                   "query"           : "\"abc xyz\"",
                                                   "analyze_wildcard": true,
                                                   "fields"          : ["field1^10.0"],
                                                   "type"            : "most_fields"
                                           ]
                                   ]]
                ]
        ]

        when:
        // Second word is being edited
        cursor = 7
        boostConfig = new EsBoost.Config(["field1^10"], [], null, null, true, cursor)

        then:
        freeText.toEs(esMappings, boostConfig) == [
                "bool": [
                        "should": [[
                                           "simple_query_string": [
                                                   "default_operator": "AND",
                                                   "query"           : "abc xyz*",
                                                   "analyze_wildcard": true,
                                                   "fields"          : ["field1^10.0"]
                                           ]
                                   ], [
                                           "bool": [
                                                   "should": [[
                                                                      "simple_query_string": [
                                                                              "default_operator": "AND",
                                                                              "query"           : "abc xyz",
                                                                              "analyze_wildcard": true,
                                                                              "fields"          : ["field1^10.0"]
                                                                      ]
                                                              ], [
                                                                      "query_string": [
                                                                              "default_operator": "AND",
                                                                              "query"           : "\"abc xyz\"",
                                                                              "analyze_wildcard": true,
                                                                              "fields"          : ["field1^10.0"],
                                                                              "type"            : "most_fields"
                                                                      ]
                                                              ]]
                                           ]
                                   ]]
                ]
        ]
    }
}
