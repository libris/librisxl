package whelk.search2.esquerytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.ESSettings
import whelk.search2.EsMappings
import whelk.search2.querytree.ExpandedQueryTree
import whelk.search2.querytree.QueryTree
import whelk.search2.querytree.TestData

class EsQueryTree2Spec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()
    EsMappings esMappings = TestData.getEsMappings()

    def "convert to ES query"() {
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
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

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

    def "match all if empty"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        EsQueryTree2 esQueryTree = new EsQueryTree2(ExpandedQueryTree.newEmpty(), esSettings)

        expect:
        esQueryTree.getMainQuery() == Map.of("match_all", Map.of())
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
//        ExpandedQueryTree eqt = qt.expand(jsonLd)
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

    def "To ES query: nested (CONDITION)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p4:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: nested (CONDITION, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost(["phrase_boost_divisor": 8]))
        String q = 'p15.p4:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
                "bool" : [
                        "filter" : [
                                "term" : [
                                        "p15.p4.@id" : "https://id.kb.se/x"
                                ]
                        ]
                ]
        ]
    }

    def "To ES query: nested (CONDITION, multi-token value, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p15.p4:(x y)'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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
//        ExpandedQueryTree eqt = qt.expand(jsonLd)
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

    def "To ES query: group nested (AND)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p4:"https://id.kb.se/y"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (OR)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p4:"https://id.kb.se/x" OR p3.p4:"https://id.kb.se/y"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (OR, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p15.p4:"https://id.kb.se/x" OR p15.p4:"https://id.kb.se/y"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (OR in AND)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = '(p3.p4:"https://id.kb.se/x" OR p3.p4:"https://id.kb.se/y") (p3.p2:E1 OR p3.p2:E2)'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (AND in OR)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = '(p3.p4:"https://id.kb.se/y" p3.p2:E1) OR p3.p1:a'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (AND, same non-repeatable field)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p2:E2'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (AND, same non-repeatable field, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p15.p2:E1 p15.p2:E2'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (OR, same repeatable field)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p4:"https://id.kb.se/x" OR p3.p4:"https://id.kb.se/y"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (OR, non-nested child)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 OR p4:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (OR, non-nested child) 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 OR p3.p2:E2 OR p4:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (AND, non-nested child)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p4:"https://id.kb.se/y"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (AND, non-nested child) 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p4:"https://id.kb.se/y" p2:E2'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (AND, grouped by non-repeatable field)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p4:"https://id.kb.se/x" p3.p2:E2 p3.p4:"https://id.kb.se/y"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (AND, grouped by non-repeatable field) 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p3.p4:"https://id.kb.se/x" p3.p4:"https://id.kb.se/y" p3.p2:E2 p3.p4:"https://id.kb.se/z"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (AND, non-nested as boundary)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 p1:x p3.p4:"https://id.kb.se/x" p3.p4:"https://id.kb.se/y"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: nested (NOT)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'NOT p3:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: nested (NOT, include_in_parent=true)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'NOT p15:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (NOT in AND, mixed with non-negated)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 NOT p3.p4:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (NOT in AND, mixed with non-negated) 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p3.p2:E1 NOT p3.p4:"https://id.kb.se/x" NOT p3.p4:"https://id.kb.se/y" NOT p3.p1:1'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: group nested (NOT in AND, all negated)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'NOT p3.p2:E1 NOT p3.p4:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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
//        ExpandedQueryTree eqt = qt.expand(jsonLd)
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
//        ExpandedQueryTree eqt = qt.expand(jsonLd)
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

    def "To ES query: CONDITION expanding to OR should result in dis_max query"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'type:T2x p1:x'
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd) // --> 'type:T2x (hasInstance.p1:x OR p1:x)'
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
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

    def "To ES query: CONDITION expanding to OR should result in dis_max query 2"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new EsBoost([:]))
        String q = 'p16:x' // p16 is a composite property
        QueryTree qt = new QueryTree(q, disambiguate)
        ExpandedQueryTree eqt = qt.expand(jsonLd) // --> 'p18:x OR p17:x'
        EsQueryTree2 esQueryTree = new EsQueryTree2(eqt, esSettings)

        expect:
        esQueryTree.getMainQuery() == [
                "dis_max": [
                        "queries": [[
                                            "simple_query_string": [
                                                    "default_operator": "AND",
                                                    "query"           : "x",
                                                    "fields"          : ["p18"]
                                            ]
                                    ], [
                                            "simple_query_string": [
                                                    "default_operator": "AND",
                                                    "query"           : "x",
                                                    "fields"          : ["p17"]
                                            ]
                                    ]]
                ]
        ]
    }
}
