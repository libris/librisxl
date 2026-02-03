package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.AppParams
import whelk.search2.Disambiguate
import whelk.search2.ESSettings
import whelk.search2.SelectedFacets

class EsQueryTreeSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    Map boostSettings = [
            "field_boost": [
                    "fields"              : [
                            [
                                    "name"        : "fieldA",
                                    "boost"       : 10,
                                    "script_score": [
                                            "name"    : "a function",
                                            "function": "f(_score)",
                                            "apply_if": "condition"
                                    ]
                            ],
                            [
                                    "name" : "fieldB",
                                    "boost": 2
                            ],
                            [
                                    "name" : "fieldC",
                                    "boost": 1
                            ]
                    ],
                    "default_boost_factor": 5,
                    "analyze_wildcard"    : true
            ]
    ]
    ESSettings esSettings = new ESSettings(TestData.getEsMappings(), new ESSettings.Boost(boostSettings))

    def "convert to ES query"() {
        given:
        String q = '(NOT p1:v1 OR p4:v4) something'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
                "bool": [
                        "must": [
                                [
                                        "bool": [
                                                "should": [
                                                        [
                                                                "bool": [
                                                                        "must_not": [
                                                                                "simple_query_string": [
                                                                                        "default_operator"  : "AND",
                                                                                        "query"             : "v1",
                                                                                        "analyze_wildcard"  : true,
                                                                                        "quote_field_suffix": ".exact",
                                                                                        "fields"            : ["p1^5.0"]
                                                                                ]
                                                                        ]
                                                                ]
                                                        ],
                                                        [
                                                                "simple_query_string": [
                                                                        "default_operator"  : "AND",
                                                                        "query"             : "v4",
                                                                        "analyze_wildcard"  : true,
                                                                        "quote_field_suffix": ".exact",
                                                                        "fields"            : ["p4._str^5.0"]
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ],
                                [
                                        "bool": [
                                                "should": [
                                                        [
                                                                "simple_query_string": [
                                                                        "default_operator"  : "AND",
                                                                        "query"             : "something",
                                                                        "analyze_wildcard"  : true,
                                                                        "quote_field_suffix": ".exact",
                                                                        "fields"            : ["fieldA^0.0", "fieldB^2.0", "fieldC^1.0"]
                                                                ]
                                                        ],
                                                        [
                                                                "script_score": [
                                                                        "query" : [
                                                                                "simple_query_string": [
                                                                                        "default_operator"  : "AND",
                                                                                        "query"             : "something",
                                                                                        "analyze_wildcard"  : true,
                                                                                        "quote_field_suffix": ".exact",
                                                                                        "fields"            : ["fieldA^10.0", "fieldB^0.0", "fieldC^0.0"]
                                                                                ]
                                                                        ],
                                                                        "script": [
                                                                                "source": "condition ? f(_score) : _score"
                                                                        ]
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ]
    }

    def "match all if empty"() {
        given:
        EsQueryTree esQueryTree = new EsQueryTree(ExpandedQueryTree.newEmpty(), esSettings)

        expect:
        esQueryTree.getMainQuery() == Map.of("match_all", Map.of())
    }

    def "category ES query"() {
        given:
        def q = 'type:T1x category:"https://id.kb.se/term/ktg/Y" category:("https://id.kb.se/term/ktg/A" OR "https://id.kb.se/term/ktg/B")'
        QueryTree qt = new QueryTree(q, disambiguate)
        def appConfig = [
                "statistics": [
                        "sliceList": [
                                ["dimensionChain": ["findCategory"], "slice": ["dimensionChain": ["identifyCategory"]]],
                                ["dimensionChain": ["noneCategory"], "itemLimit": 100, "connective": "OR", "showIf": ["category"]]
                        ]
                ]
        ]
        AppParams appParams = new AppParams(appConfig, jsonLd)
        SelectedFacets selectedFacets = new SelectedFacets(qt, appParams.sliceList)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings, selectedFacets)

        expect:
        esQueryTree.getMainQuery() == [
                "bool": [
                        "filter": [
                                "term": [
                                        "@type": "T1x"
                                ]
                        ]
                ]
        ]
        esQueryTree.getPostFilter() == [
                "bool": [
                        "must": [[
                                         "bool": [
                                                 "should": [[
                                                                    "bool": [
                                                                            "filter": [
                                                                                    "term": [
                                                                                            "_categoryByCollection.@none.@id": "https://id.kb.se/term/ktg/A"
                                                                                    ]
                                                                            ]
                                                                    ]
                                                            ], [
                                                                    "bool": [
                                                                            "filter": [
                                                                                    "term": [
                                                                                            "_categoryByCollection.@none.@id": "https://id.kb.se/term/ktg/B"
                                                                                    ]
                                                                            ]
                                                                    ]
                                                            ]]
                                         ]
                                 ], [
                                         "bool": [
                                                 "filter": [
                                                         "term": [
                                                                 "_categoryByCollection.identify.@id": "https://id.kb.se/term/ktg/Y"
                                                         ]
                                                 ]
                                         ]
                                 ]]
                ]
        ]
    }

    def "To ES query: group nested"() {
        given:
        String q = 'p3.p1:x p3.p4:y'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "x",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p1^5.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "y",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p4._str^5.0"]
                                                         ]
                                                 ]]
                                ]
                        ]
                ]
        ]
    }

    def "To ES query: group nested (negated)"() {
        given:
        String q = 'p3.p1:x p3.p4:y'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "x",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p1^5.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "y",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p4._str^5.0"]
                                                         ]
                                                 ]]
                                ]
                        ]
                ]
        ]
    }

    def "To ES query: group nested 2"() {
        given:
        String q = 'p3.p1:x p3.p4:y p2:e1'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
                "bool": [
                        "must": [[
                                         "nested": [
                                                 "query": [
                                                         "bool": [
                                                                 "must": [[
                                                                                  "simple_query_string": [
                                                                                          "default_operator"  : "AND",
                                                                                          "query"             : "x",
                                                                                          "analyze_wildcard"  : true,
                                                                                          "quote_field_suffix": ".exact",
                                                                                          "fields"            : ["p3.p1^5.0"]
                                                                                  ]
                                                                          ], [
                                                                                  "simple_query_string": [
                                                                                          "default_operator"  : "AND",
                                                                                          "query"             : "y",
                                                                                          "analyze_wildcard"  : true,
                                                                                          "quote_field_suffix": ".exact",
                                                                                          "fields"            : ["p3.p4._str^5.0"]
                                                                                  ]
                                                                          ]]
                                                         ]
                                                 ],
                                                 "path" : "p3"
                                         ]
                                 ], [
                                         "bool": [
                                                 "filter": [
                                                         "term": [
                                                                 "p2": "E1"
                                                         ]
                                                 ]
                                         ]
                                 ]]
                ]
        ]
    }

    def "To ES query: group nested 3"() {
        given:
        String q = 'p3.p1:x (p3.p4:y OR p3.p4:z)'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "x",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p1^5.0"]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "should": [[
                                                                                    "simple_query_string": [
                                                                                            "default_operator"  : "AND",
                                                                                            "query"             : "y",
                                                                                            "analyze_wildcard"  : true,
                                                                                            "quote_field_suffix": ".exact",
                                                                                            "fields"            : ["p3.p4._str^5.0"]
                                                                                    ]
                                                                            ], [
                                                                                    "simple_query_string": [
                                                                                            "default_operator"  : "AND",
                                                                                            "query"             : "z",
                                                                                            "analyze_wildcard"  : true,
                                                                                            "quote_field_suffix": ".exact",
                                                                                            "fields"            : ["p3.p4._str^5.0"]
                                                                                    ]
                                                                            ]]
                                                         ]
                                                 ]]
                                ]
                        ]
                ]
        ]
    }

    def "To ES query: group nested 4"() {
        given:
        // TODO: Interpret "(p3.p1:x p3.p4:y) (p3.p1:a p3.p4:b)" differently, i.e. don't flatten and treat each group as a distinct nested clause?
        String q = 'p3.p1:x p3.p4:y p3.p1:a p3.p4:b'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
                "nested": [
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "x",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p1^5.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "y",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p4._str^5.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "a",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p1^5.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "b",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p4._str^5.0"]
                                                         ]
                                                 ]]
                                ]
                        ],
                        "path" : "p3"
                ]
        ]
    }

    def "To ES query: group nested + multi-selectable"() {
        given:
        String q = 'p2:E1 p3p1:y p3.p4:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        Map appConfig = [
                "statistics": [
                        "sliceList": [
                                ["dimensionChain": ["p3p1"], "connective": "OR"]
                        ]
                ]
        ]
        AppParams appParams = new AppParams(appConfig, jsonLd)
        SelectedFacets selectedFacets = new SelectedFacets(qt, appParams.sliceList)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings, selectedFacets)

        expect:
        esQueryTree.getMainQuery() == [
                "bool": [
                        "filter": [
                                "term": [
                                        "p2": "E1"
                                ]
                        ]
                ]
        ]
        esQueryTree.getPostFilter() == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "simple_query_string": [
                                                                 "default_operator"  : "AND",
                                                                 "query"             : "y",
                                                                 "analyze_wildcard"  : true,
                                                                 "quote_field_suffix": ".exact",
                                                                 "fields"            : ["p3.p1^5.0"]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p4.@id": "https://id.kb.se/x"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ]]
                                ]
                        ]
                ]
        ]
    }

    def "To ES query: group nested + multi-selected"() {
        given:
        String q = 'p2:E1 (p3p1:y OR p3p1:z) p3.p4:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        Map appConfig = [
                "statistics": [
                        "sliceList": [
                                ["dimensionChain": ["p3p1"], "connective": "OR"]
                        ]
                ]
        ]
        AppParams appParams = new AppParams(appConfig, jsonLd)
        SelectedFacets selectedFacets = new SelectedFacets(qt, appParams.sliceList)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings, selectedFacets)

        expect:
        esQueryTree.getMainQuery() == [
                "bool": [
                        "filter": [
                                "term": [
                                        "p2": "E1"
                                ]
                        ]
                ]
        ]
        esQueryTree.getPostFilter() == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "bool": [
                                                                 "should": [[
                                                                                    "simple_query_string": [
                                                                                            "default_operator"  : "AND",
                                                                                            "query"             : "y",
                                                                                            "analyze_wildcard"  : true,
                                                                                            "quote_field_suffix": ".exact",
                                                                                            "fields"            : ["p3.p1^5.0"]
                                                                                    ]
                                                                            ], [
                                                                                    "simple_query_string": [
                                                                                            "default_operator"  : "AND",
                                                                                            "query"             : "z",
                                                                                            "analyze_wildcard"  : true,
                                                                                            "quote_field_suffix": ".exact",
                                                                                            "fields"            : ["p3.p1^5.0"]
                                                                                    ]
                                                                            ]]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p4.@id": "https://id.kb.se/x"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ]]
                                ]
                        ]
                ]
        ]
    }

    def "Regroup alt selectors"() {
        given:
        String q = 'type:T2 p3p1:x p3p1:y'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings)

        expect:
        eqt.toString() == "(type:T2 OR type:T2x) (hasInstance.p3.p1:x OR p3.p1:x) (hasInstance.p3.p1:y OR p3.p1:y)"
        esQueryTree.toString() == "(type:T2 OR type:T2x) ((p3.p1:x p3.p1:y) OR (hasInstance.p3.p1:x hasInstance.p3.p1:y))"
    }
}
