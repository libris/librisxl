package whelk.search2.esquery

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.ESSettings
import whelk.search2.EsMappings
import whelk.search2.querytree.QueryTree
import whelk.search2.querytree.QueryTreeBuilder
import whelk.search2.querytree.TestData

class EsQueryTreeBuilderSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()
    EsMappings esMappings = TestData.getEsMappings()

    def "mixed query with boosted fields"() {
        given:
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
                        ]
                ]
        ]
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost(boostSettings))
        String q = '(NOT p1:v1 OR p4:v4) something'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
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
                                                                                        "fields"            : ["p1"]
                                                                                ]
                                                                        ]
                                                                ]
                                                        ],
                                                        [
                                                                "simple_query_string": [
                                                                        "default_operator"  : "AND",
                                                                        "query"             : "v4",
                                                                        "fields"            : ["p4._str"]
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ],
                                [
                                        "bool": [
                                                "should": [
                                                        [
                                                                "script_score": [
                                                                        "query" : [
                                                                                "simple_query_string": [
                                                                                        "default_operator"  : "AND",
                                                                                        "query"             : "something",
                                                                                        "fields"            : ["fieldA^10.0", "fieldB^0.0", "fieldC^0.0"]
                                                                                ]
                                                                        ],
                                                                        "script": [
                                                                                "source": "condition ? f(_score) : _score"
                                                                        ]
                                                                ]
                                                        ],
                                                        [
                                                                "simple_query_string": [
                                                                        "default_operator"  : "AND",
                                                                        "query"             : "something",
                                                                        "fields"            : ["fieldA^0.0", "fieldB^2.0", "fieldC"]
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ]
    }

    def "free-text query using configured boost params"() {
        given:
        Map boostSettings = [
                "field_boost": [
                        "fields"              : [
                                [
                                        "name"        : "fieldA",
                                        "boost"       : 10
                                ]
                        ],
                        "phrase_boost_divisor": 4,
                        "analyze_wildcard": true,
                        "quote_field_suffix": ".exact"
                ]
        ]
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost(boostSettings))
        String q = 'x y'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "should": [[
                                           "query_string": [
                                                   "default_operator"  : "AND",
                                                   "query"             : "\"x y\"",
                                                   "analyze_wildcard"  : true,
                                                   "quote_field_suffix": ".exact",
                                                   "fields"            : ["fieldA^2.5", "fieldA.exact^2.5"]
                                           ]
                                   ], [
                                           "simple_query_string": [
                                                   "default_operator"  : "AND",
                                                   "query"             : "x y",
                                                   "analyze_wildcard"  : true,
                                                   "quote_field_suffix": ".exact",
                                                   "fields"            : ["fieldA^10.0", "fieldA.exact^10.0"]
                                           ]
                                   ]]
                ]
        ]
    }

    def "match all if empty"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))

        Map result = QueryTree.newEmpty().toEsQuery(esSettings)

        expect:
        result == Map.of("match_all", Map.of())
    }

    // TODO: PostFilter
//    def "category ES query"() {
//        given:
//        def q = 'type:T2x workCategory:"https://id.kb.se/term/ktg/Y" workCategory:("https://id.kb.se/term/ktg/A" OR "https://id.kb.se/term/ktg/B")'
//        QueryTree qt = new QueryTree(q, disambiguate)
//        def appConfig = [
//                "statistics": [
//                        "sliceList": [
//                                ["dimensionChain": ["findCategory"], "slice": ["dimensionChain": ["identifyCategory"]]],
//                                ["dimensionChain": ["noneCategory"], "itemLimit": 100, "connective": "OR", "showIf": ["category"]]
//                        ]
//                ]
//        ]
//        AppParams appParams = new AppParams(appConfig, jsonLd)
//        SelectedFacets selectedFacets = new SelectedFacets(qt, appParams.sliceList)
//        QueryTree.ExpandedTree eqt = qt.expand(jsonLd)
//        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings, selectedFacets)
//
//        expect:
//        esQueryTree.getMainQuery() == [
//                "bool": [
//                        "filter": [
//                                "term": [
//                                        "@type": "T2x"
//                                ]
//                        ]
//                ]
//        ]
//        esQueryTree.getPostFilter() == [
//                "bool": [
//                        "must": [[
//                                         "bool": [
//                                                 "should": [[
//                                                                    "bool": [
//                                                                            "filter": [
//                                                                                    "term": [
//                                                                                            "_categoryByCollection.@none.@id": "https://id.kb.se/term/ktg/A"
//                                                                                    ]
//                                                                            ]
//                                                                    ]
//                                                            ], [
//                                                                    "bool": [
//                                                                            "filter": [
//                                                                                    "term": [
//                                                                                            "_categoryByCollection.@none.@id": "https://id.kb.se/term/ktg/B"
//                                                                                    ]
//                                                                            ]
//                                                                    ]
//                                                            ]]
//                                         ]],
//                                 [
//                                         "bool": [
//                                                 "filter": [
//                                                         "term": [
//                                                                 "_categoryByCollection.identify.@id": "https://id.kb.se/term/ktg/Y"
//                                                         ]
//                                                 ]
//                                         ]
//                                 ]]
//                ]
//        ]
//    }

    def "nested (CONDITION)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p4:"https://id.kb.se/x"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "nested": [
                        "ignore_unmapped": true,
                        "query": [
                                "bool": [
                                        "filter": [
                                                "term": [
                                                        "p3.p4.@id": "https://id.kb.se/x"
                                                ]
                                        ]
                                ]
                        ],
                        "path" : "p3"
                ]
        ]
    }

    def "nested (CONDITION, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost(["phrase_boost_divisor": 8]))
        String q = 'p15.p4:"https://id.kb.se/x"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool" : [
                        "filter" : [
                                "term" : [
                                        "p15.p4.@id" : "https://id.kb.se/x"
                                ]
                        ]
                ]
        ]
    }

    def "nested (CONDITION, multi-token value, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p15.p4:(x y)'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
            "nested" : [
                "ignore_unmapped" : true,
                "path" : "p15",
                "query" : [
                    "simple_query_string" : [
                        "default_operator" : "AND",
                        "query" : "x y",
                        "fields" : [ "p15.p4._str" ]
                    ]
                ]
            ]
        ]
    }

    // TODO: Composite properties
//    def "To ES query: nested (CONDITION, multi-token value, composite property)"() {
//        given:
//        String q = 'p3.p16:(x y)'
//        QueryTree qt = new QueryTree(q, disambiguate)
//        QueryTree.ExpandedTree eqt = qt.expand(jsonLd)
//        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings)
//
//        expect:
//        esQueryTree.getMainQuery() == [
//                "nested": [
//                        "query"          : [
//                                "bool": [
//                                        "should": [[
//                                                           "simple_query_string": [
//                                                                   "default_operator"  : "AND",
//                                                                   "query"             : "x y",
//                                                                   "analyze_wildcard"  : true,
//                                                                   "quote_field_suffix": ".exact",
//                                                                   "fields"            : ["p3.p18^5.0", "p3.p17^5.0"]
//                                                           ]
//                                                   ], [
//                                                           "query_string": [
//                                                                   "default_operator"  : "AND",
//                                                                   "query"             : "\"x y\"",
//                                                                   "analyze_wildcard"  : true,
//                                                                   "quote_field_suffix": ".exact",
//                                                                   "fields"            : ["p3.p18^1.0", "p3.p17^1.0"]
//                                                           ]
//                                                   ]]
//                                ]
//                        ],
//                        "path"           : "p3",
//                        "ignore_unmapped": true
//                ]
//        ]
//    }

    def "group nested (AND)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p4:"https://id.kb.se/y"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "nested": [
                        "ignore_unmapped" : true,
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p2": "E1"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p4.@id": "https://id.kb.se/y"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ]]
                                ]
                        ]
                ]
        ]
    }

    def "group nested (OR)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p4:"https://id.kb.se/x" OR p3.p4:"https://id.kb.se/y"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "should": [[
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "p3.p4.@id": "https://id.kb.se/x"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ], [
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "p3.p4.@id": "https://id.kb.se/y"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ]]
                                ]
                        ],
                        "ignore_unmapped": true
                ]
        ]
    }

    def "group nested (OR, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p15.p4:"https://id.kb.se/x" OR p15.p4:"https://id.kb.se/y"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "should": [[
                                           "bool": [
                                                   "filter": [
                                                           "term": [
                                                                   "p15.p4.@id": "https://id.kb.se/x"
                                                           ]
                                                   ]
                                           ]
                                   ], [
                                           "bool": [
                                                   "filter": [
                                                           "term": [
                                                                   "p15.p4.@id": "https://id.kb.se/y"
                                                           ]
                                                   ]
                                           ]
                                   ]]
                ]
        ]
    }

    def "group nested (OR in AND)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = '(p3.p4:"https://id.kb.se/x" OR p3.p4:"https://id.kb.se/y") (p3.p2:E1 OR p3.p2:E2)'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "bool": [
                                                                 "should": [[
                                                                                    "bool": [
                                                                                            "filter": [
                                                                                                    "term": [
                                                                                                            "p3.p4.@id": "https://id.kb.se/x"
                                                                                                    ]
                                                                                            ]
                                                                                    ]
                                                                            ], [
                                                                                    "bool": [
                                                                                            "filter": [
                                                                                                    "term": [
                                                                                                            "p3.p4.@id": "https://id.kb.se/y"
                                                                                                    ]
                                                                                            ]
                                                                                    ]
                                                                            ]]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "should": [[
                                                                                    "bool": [
                                                                                            "filter": [
                                                                                                    "term": [
                                                                                                            "p3.p2": "E1"
                                                                                                    ]
                                                                                            ]
                                                                                    ]
                                                                            ], [
                                                                                    "bool": [
                                                                                            "filter": [
                                                                                                    "term": [
                                                                                                            "p3.p2": "E2"
                                                                                                    ]
                                                                                            ]
                                                                                    ]
                                                                            ]]
                                                         ]
                                                 ]]
                                ]
                        ],
                        "ignore_unmapped" : true
                ]
        ]
    }

    def "group nested (AND in OR)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = '(p3.p4:"https://id.kb.se/y" p3.p2:E1) OR p3.p1:a'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "nested": [
                        "path"           : "p3",
                        "query"          : [
                                "bool": [
                                        "should": [[
                                                           "bool": [
                                                                   "must": [[
                                                                                    "bool": [
                                                                                            "filter": [
                                                                                                    "term": [
                                                                                                            "p3.p4.@id": "https://id.kb.se/y"
                                                                                                    ]
                                                                                            ]
                                                                                    ]
                                                                            ], [
                                                                                    "bool": [
                                                                                            "filter": [
                                                                                                    "term": [
                                                                                                            "p3.p2": "E1"
                                                                                                    ]
                                                                                            ]
                                                                                    ]
                                                                            ]]
                                                           ]
                                                   ], [
                                                           "simple_query_string": [
                                                                   "default_operator": "AND",
                                                                   "query"           : "a",
                                                                   "fields"          : ["p3.p1"]
                                                           ]
                                                   ]]
                                ]
                        ],
                        "ignore_unmapped": true
                ]
        ]
    }

    def "group nested (AND, same non-repeatable field)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p2:E2'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must": [[
                                         "nested": [
                                                 "query": [
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p2": "E1"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ],
                                                 "path" : "p3",
                                                 "ignore_unmapped": true
                                         ]
                                 ], [
                                         "nested": [
                                                 "query": [
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p2": "E2"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ],
                                                 "path" : "p3",
                                                 "ignore_unmapped": true
                                         ]
                                 ]]
                ]
        ]
    }

    def "group nested (AND, same non-repeatable field, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p15.p2:E1 p15.p2:E2'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must": [[
                                         "bool": [
                                                 "filter": [
                                                         "term": [
                                                                 "p15.p2": "E1"
                                                         ]
                                                 ]
                                         ]
                                 ], [
                                         "bool": [
                                                 "filter": [
                                                         "term": [
                                                                 "p15.p2": "E2"
                                                         ]
                                                 ]
                                         ]
                                 ]]
                ]
        ]
    }

    def "group nested (OR, same repeatable field)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p4:"https://id.kb.se/x" OR p3.p4:"https://id.kb.se/y"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "should": [[
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "p3.p4.@id": "https://id.kb.se/x"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ], [
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "p3.p4.@id": "https://id.kb.se/y"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ]]
                                ]
                        ],
                        "ignore_unmapped" : true
                ]
        ]
    }

    def "group nested (OR, non-nested child)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 OR p4:"https://id.kb.se/x"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "should": [[
                                           "nested": [
                                                   "path" : "p3",
                                                   "query": [
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "p3.p2": "E1"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ],
                                                   "ignore_unmapped": true
                                           ]
                                   ], [
                                           "bool": [
                                                   "filter": [
                                                           "term": [
                                                                   "p4.@id": "https://id.kb.se/x"
                                                           ]
                                                   ]
                                           ]
                                   ]]
                ]
        ]
    }

    def "group nested (OR, non-nested child) 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 OR p3.p2:E2 OR p4:"https://id.kb.se/x"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "should": [[
                                           "nested": [
                                                   "path" : "p3",
                                                   "query": [
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "p3.p2": "E1"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ],
                                                   "ignore_unmapped": true
                                           ]
                                   ], [
                                           "nested": [
                                                   "path" : "p3",
                                                   "query": [
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "p3.p2": "E2"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ],
                                                   "ignore_unmapped": true
                                           ]
                                   ], [
                                           "bool": [
                                                   "filter": [
                                                           "term": [
                                                                   "p4.@id": "https://id.kb.se/x"
                                                           ]
                                                   ]
                                           ]
                                   ]]
                ]
        ]
    }

    def "group nested (AND, non-nested child)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p4:"https://id.kb.se/y"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must": [[
                                         "nested": [
                                                 "query": [
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p2": "E1"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ],
                                                 "path" : "p3",
                                                 "ignore_unmapped": true
                                         ]
                                 ], [
                                         "bool": [
                                                 "filter": [
                                                         "term": [
                                                                 "p4.@id": "https://id.kb.se/y"
                                                         ]
                                                 ]
                                         ]
                                 ]]
                ]
        ]
    }

    def "group nested (AND, non-nested child) 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p4:"https://id.kb.se/y" p2:E2'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must": [[
                                         "nested": [
                                                 "path" : "p3",
                                                 "query": [
                                                         "bool": [
                                                                 "must": [[
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p2": "E1"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ], [
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p4.@id": "https://id.kb.se/y"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ]]
                                                         ]
                                                 ],
                                                 "ignore_unmapped": true
                                         ]
                                 ], [
                                         "bool": [
                                                 "filter": [
                                                         "term": [
                                                                 "p2": "E2"
                                                         ]
                                                 ]
                                         ]
                                 ]]
                ]
        ]
    }

    def "group nested (AND, grouped by non-repeatable field)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p4:"https://id.kb.se/x" p3.p2:E2 p3.p4:"https://id.kb.se/y"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must": [[
                                         "nested": [
                                                 "path" : "p3",
                                                 "query": [
                                                         "bool": [
                                                                 "must": [[
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p2": "E1"
                                                                                                  ]
                                                                                          ]
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
                                                 ],
                                                 "ignore_unmapped": true
                                         ]
                                 ], [
                                         "nested": [
                                                 "path" : "p3",
                                                 "query": [
                                                         "bool": [
                                                                 "must": [[
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p2": "E2"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ], [
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p4.@id": "https://id.kb.se/y"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ]]
                                                         ]
                                                 ],
                                                 "ignore_unmapped": true
                                         ]
                                 ]]
                ]
        ]
    }

    def "group nested (AND, grouped by non-repeatable field) 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p4:"https://id.kb.se/x" p3.p4:"https://id.kb.se/y" p3.p2:E2 p3.p4:"https://id.kb.se/z"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must": [[
                                         "nested": [
                                                 "path" : "p3",
                                                 "query": [
                                                         "bool": [
                                                                 "must": [[
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p2": "E1"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ], [
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p4.@id": "https://id.kb.se/x"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ],
                                                                          [
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p4.@id": "https://id.kb.se/y"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ]]
                                                         ]
                                                 ],
                                                 "ignore_unmapped": true
                                         ]
                                 ], [
                                         "nested": [
                                                 "path" : "p3",
                                                 "query": [
                                                         "bool": [
                                                                 "must": [[
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p2": "E2"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ], [
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p4.@id": "https://id.kb.se/z"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ]]
                                                         ]
                                                 ],
                                                 "ignore_unmapped": true
                                         ]
                                 ]]
                ]
        ]
    }

    def "group nested (AND, non-nested as boundary)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p1:x p3.p4:"https://id.kb.se/x" p3.p4:"https://id.kb.se/y"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must": [[
                                         "nested": [
                                                 "path"           : "p3",
                                                 "query"          : [
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p2": "E1"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ],
                                                 "ignore_unmapped": true
                                         ]
                                 ], [
                                         "simple_query_string": [
                                                 "default_operator": "AND",
                                                 "query"           : "x",
                                                 "fields"          : ["p1"]
                                         ]
                                 ], [
                                         "nested": [
                                                 "path"           : "p3",
                                                 "query"          : [
                                                         "bool": [
                                                                 "must": [[
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p4.@id": "https://id.kb.se/x"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ], [
                                                                                  "bool": [
                                                                                          "filter": [
                                                                                                  "term": [
                                                                                                          "p3.p4.@id": "https://id.kb.se/y"
                                                                                                  ]
                                                                                          ]
                                                                                  ]
                                                                          ]]
                                                         ]
                                                 ],
                                                 "ignore_unmapped": true
                                         ]
                                 ]]
                ]
        ]
    }

    def "nested (NOT)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'NOT p3:"https://id.kb.se/x"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must_not": [
                                "nested": [
                                        "query": [
                                                "bool": [
                                                        "filter": [
                                                                "term": [
                                                                        "p3.@id": "https://id.kb.se/x"
                                                                ]
                                                        ]
                                                ]
                                        ],
                                        "path" : "p3",
                                        "ignore_unmapped" : true,
                                ]
                        ]
                ]
        ]
    }

    def "nested (NOT, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'NOT p15:"https://id.kb.se/x"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must_not": [
                                "bool": [
                                        "filter": [
                                                "term": [
                                                        "p15.@id": "https://id.kb.se/x"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ]
    }

    def "group nested (NOT in AND, mixed with non-negated)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 NOT p3.p4:"https://id.kb.se/x"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p2": "E1"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "must_not": [
                                                                         "bool": [
                                                                                 "filter": [
                                                                                         "term": [
                                                                                                 "p3.p4.@id": "https://id.kb.se/x"
                                                                                         ]
                                                                                 ]
                                                                         ]
                                                                 ]
                                                         ]
                                                 ]]
                                ]
                        ],
                        "ignore_unmapped" : true
                ]
        ]
    }

    def "group nested (NOT in AND, mixed with non-negated) 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 NOT p3.p4:"https://id.kb.se/x" NOT p3.p4:"https://id.kb.se/y" NOT p3.p1:1'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "nested": [
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "bool": [
                                                                 "filter": [
                                                                         "term": [
                                                                                 "p3.p2": "E1"
                                                                         ]
                                                                 ]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "must_not": [
                                                                         "bool": [
                                                                                 "filter": [
                                                                                         "term": [
                                                                                                 "p3.p4.@id": "https://id.kb.se/x"
                                                                                         ]
                                                                                 ]
                                                                         ]
                                                                 ]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "must_not": [
                                                                         "bool": [
                                                                                 "filter": [
                                                                                         "term": [
                                                                                                 "p3.p4.@id": "https://id.kb.se/y"
                                                                                         ]
                                                                                 ]
                                                                         ]
                                                                 ]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "must_not": [
                                                                         "simple_query_string": [
                                                                                 "default_operator"  : "AND",
                                                                                 "query"             : "1",
                                                                                 "fields"            : ["p3.p1"]
                                                                         ]
                                                                 ]
                                                         ]
                                                 ]]
                                ]
                        ],
                        "path" : "p3",
                        "ignore_unmapped" : true
                ]
        ]
    }

    def "group nested (NOT in AND, all negated)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'NOT p3.p2:E1 NOT p3.p4:"https://id.kb.se/x"'
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must": [[
                                         "bool": [
                                                 "must_not": [
                                                         "nested": [
                                                                 "path" : "p3",
                                                                 "query": [
                                                                         "bool": [
                                                                                 "filter": [
                                                                                         "term": [
                                                                                                 "p3.p2": "E1"
                                                                                         ]
                                                                                 ]
                                                                         ]
                                                                 ],
                                                                 "ignore_unmapped" : true
                                                         ]
                                                 ]
                                         ]
                                 ], [
                                         "bool": [
                                                 "must_not": [
                                                         "nested": [
                                                                 "path" : "p3",
                                                                 "query": [
                                                                         "bool": [
                                                                                 "filter": [
                                                                                         "term": [
                                                                                                 "p3.p4.@id": "https://id.kb.se/x"
                                                                                         ]
                                                                                 ]
                                                                         ]
                                                                 ],
                                                                 "ignore_unmapped" : true
                                                         ]
                                                 ]
                                         ]
                                 ]]
                ]
        ]
    }

    // TODO: PostFilter
//    def "To ES query: group nested (multi-selectable to post filter)"() {
//        given:
//        String q = 'p2:E1 p3p1:y p3.p4:"https://id.kb.se/x"'
//        QueryTree qt = new QueryTree(q, disambiguate)
//        Map appConfig = [
//                "statistics": [
//                        "sliceList": [
//                                ["dimensionChain": ["p3p1"], "connective": "OR"]
//                        ]
//                ]
//        ]
//        AppParams appParams = new AppParams(appConfig, jsonLd)
//        SelectedFacets selectedFacets = new SelectedFacets(qt, appParams.sliceList)
//        QueryTree.ExpandedTree eqt = qt.expand(jsonLd)
//        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings, selectedFacets)
//
//        expect:
//        esQueryTree.getMainQuery() == [
//                "bool": [
//                        "filter": [
//                                "term": [
//                                        "p2": "E1"
//                                ]
//                        ]
//                ]
//        ]
//        esQueryTree.getPostFilter() == [
//                "nested": [
//                        "ignore_unmapped" : true,
//                        "path" : "p3",
//                        "query": [
//                                "bool": [
//                                        "must": [[
//                                                         "simple_query_string": [
//                                                                 "default_operator"  : "AND",
//                                                                 "query"             : "y",
//                                                                 "analyze_wildcard"  : true,
//                                                                 "quote_field_suffix": ".exact",
//                                                                 "fields"            : ["p3.p1^5.0"]
//                                                         ]
//                                                 ], [
//                                                         "bool": [
//                                                                 "filter": [
//                                                                         "term": [
//                                                                                 "p3.p4.@id": "https://id.kb.se/x"
//                                                                         ]
//                                                                 ]
//                                                         ]
//                                                 ]]
//                                ]
//                        ]
//                ]
//        ]
//    }
//
//    def "To ES query: group nested (multi-selected to post filter)"() {
//        given:
//        String q = 'p2:E1 (p3p1:y OR p3p1:z) p3.p4:"https://id.kb.se/x"'
//        QueryTree qt = new QueryTree(q, disambiguate)
//        Map appConfig = [
//                "statistics": [
//                        "sliceList": [
//                                ["dimensionChain": ["p3p1"], "connective": "OR"]
//                        ]
//                ]
//        ]
//        AppParams appParams = new AppParams(appConfig, jsonLd)
//        SelectedFacets selectedFacets = new SelectedFacets(qt, appParams.sliceList)
//        QueryTree.ExpandedTree eqt = qt.expand(jsonLd)
//        EsQueryTree esQueryTree = new EsQueryTree(eqt, esSettings, selectedFacets)
//
//        expect:
//        esQueryTree.getMainQuery() == [
//                "bool": [
//                        "filter": [
//                                "term": [
//                                        "p2": "E1"
//                                ]
//                        ]
//                ]
//        ]
//        esQueryTree.getPostFilter() == [
//                "nested": [
//                        "ignore_unmapped" : true,
//                        "path" : "p3",
//                        "query": [
//                                "bool": [
//                                        "must": [[
//                                                         "bool": [
//                                                                 "should": [[
//                                                                                    "simple_query_string": [
//                                                                                            "default_operator"  : "AND",
//                                                                                            "query"             : "y",
//                                                                                            "fields"            : ["p3.p1"]
//                                                                                    ]
//                                                                            ], [
//                                                                                    "simple_query_string": [
//                                                                                            "default_operator"  : "AND",
//                                                                                            "query"             : "z",
//                                                                                            "fields"            : ["p3.p1"]
//                                                                                    ]
//                                                                            ]]
//                                                         ]
//                                                 ], [
//                                                         "bool": [
//                                                                 "filter": [
//                                                                         "term": [
//                                                                                 "p3.p4.@id": "https://id.kb.se/x"
//                                                                         ]
//                                                                 ]
//                                                         ]
//                                                 ]]
//                                ]
//                        ]
//                ]
//        ]
//    }

    def "CONDITION expanding to OR should result in dis_max query"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'type:T2x p1:x'
        Map result = new QueryTree(q, disambiguate)
                .expand(jsonLd)
                .toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "must": [[
                                         "bool": [
                                                 "filter": [
                                                         "term": [
                                                                 "@type": "T2x"
                                                         ]
                                                 ]
                                         ]
                                 ], [
                                         "dis_max": [
                                                 "queries": [[
                                                                     "simple_query_string": [
                                                                             "default_operator": "AND",
                                                                             "query"           : "x",
                                                                             "fields"          : ["@reverse.instanceOf.p1"]
                                                                     ]
                                                             ], [
                                                                     "simple_query_string": [
                                                                             "default_operator": "AND",
                                                                             "query"           : "x",
                                                                             "fields"          : ["p1"]
                                                                     ]
                                                             ]]
                                         ]
                                 ]]
                ]
        ]
    }

    def "simple text queries on sub-properties of the same composite property should be combined into a single query clause"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p16:(x y)' // p16 is a composite property
        Map result = new QueryTree(q, disambiguate)
                .expand(jsonLd)
                .toEsQuery(esSettings)

        expect:
        result == [
                "simple_query_string": [
                        "default_operator": "AND",
                        "query"           : "x y",
                        "fields"          : ["p18", "p17"]
                ]
        ]
    }

    def "simple text queries on sub-properties of the same composite property should be combined into a single query clause 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p1:x OR p16:(x y)' // p16 is a composite property
        Map result = new QueryTree(q, disambiguate)
                .expand(jsonLd)
                .toEsQuery(esSettings)

        expect:
        result == [
                "bool": [
                        "should": [[
                                           "simple_query_string": [
                                                   "default_operator": "AND",
                                                   "query"           : "x",
                                                   "fields"          : ["p1"]
                                           ]
                                   ], [
                                           "simple_query_string": [
                                                   "default_operator": "AND",
                                                   "query"           : "x y",
                                                   "fields"          : ["p18", "p17"]
                                           ]
                                   ]]
                ]
        ]
    }

    def "exists query"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))// p16 is a composite property
        Map result = new QueryTree(q, disambiguate).toEsQuery(esSettings)

        expect:
        result == [ "exists" : [ "field" : "p" ] ]

        where:
        q << ["p:()", "p:*"]
    }
}
