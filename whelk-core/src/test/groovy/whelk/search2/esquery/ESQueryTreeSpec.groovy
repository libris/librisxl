package whelk.search2.esquery

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.AppParams
import whelk.search2.Disambiguate
import whelk.search2.SelectedFacets
import whelk.search2.querytree.QueryTree
import whelk.search2.TestData

class ESQueryTreeSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()
    ESMappings esMappings = TestData.getEsMappings()

    def "category query (use post filter for radio-select facets)"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new ESBoost([:]))
        String q = 'type:T2x workCategory:"https://id.kb.se/term/ktg/Y" workCategory:("https://id.kb.se/term/ktg/A" OR "https://id.kb.se/term/ktg/B")'
        QueryTree qt = new QueryTree(q, disambiguate)
        Map appConfig = [
                "statistics": [
                        "sliceList": [
                                ["dimensionChain": ["findCategory"], "slice": ["dimensionChain": ["identifyCategory"]]],
                                ["dimensionChain": ["noneCategory"], "itemLimit": 100, "connective": "OR", "showIf": ["category"]]
                        ]
                ]
        ]
        AppParams appParams = new AppParams(appConfig, jsonLd)
        new SelectedFacets(qt, appParams.sliceList).flagMultiOrRadioSelectedForPostFilter()
        ESQueryTree esQueryTree = new ESQueryTree(ESQueryTreeBuilder.buildFrom(qt.tree(), esSettings))

        expect:
        esQueryTree.dslWithPostFilter() == [
                "query"      : [
                        "bool": [
                                "filter": [
                                        "term": [
                                                "@type": "T2x"
                                        ]
                                ]
                        ]
                ],
                "post_filter": [
                        "bool": [
                                "must": [[
                                                 "bool": [
                                                         "filter": [
                                                                 "term": [
                                                                         "_categoryByCollection.identify.@id": "https://id.kb.se/term/ktg/Y"
                                                                 ]
                                                         ]
                                                 ]
                                         ], [
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
                                         ]]
                        ]
                ]
        ]
    }

    def "use post filter for multi-select facets"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new ESBoost([:]))
        String q = 'p2:E1 p4:"https://id.kb.se/x"'
        QueryTree qt = new QueryTree(q, disambiguate)
        Map appConfig = [
                "statistics": [
                        "sliceList": [
                                ["dimensionChain": ["p4"], "connective": "OR"]
                        ]
                ]
        ]
        AppParams appParams = new AppParams(appConfig, jsonLd)
        new SelectedFacets(qt, appParams.sliceList).flagMultiOrRadioSelectedForPostFilter()
        ESQueryTree esQueryTree = qt.expand(jsonLd).toEsQuery(esSettings)
        Map result = esQueryTree.dslWithPostFilter()

        expect:
        result == [
            "query" : [
                "bool" : [
                    "filter" : [
                        "term" : [
                            "p2" : "E1"
                        ]
                    ]
                ]
            ],
            "post_filter" : [
                "bool" : [
                    "filter" : [
                        "term" : [
                            "p4.@id" : "https://id.kb.se/x"
                        ]
                    ]
                ]
            ]
        ]
    }

    def "keep nested clause intact when using post filter for multi-select facets"() {
        given:
        ESSettings esSettings = new ESSettings(esMappings, new ESBoost([:]))
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
        new SelectedFacets(qt, appParams.sliceList).flagMultiOrRadioSelectedForPostFilter()
        ESQueryTree esQueryTree = qt.expand(jsonLd).toEsQuery(esSettings)
        Map result = esQueryTree.dslWithPostFilter()

        expect:
        result == [
                "query"      : [
                        "bool": [
                                "filter": [
                                        "term": [
                                                "p2": "E1"
                                        ]
                                ]
                        ]
                ],
                "post_filter": [
                        "nested": [
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
                                                         ],
                                                         [
                                                                 "bool": [
                                                                         "should": [[
                                                                                            "simple_query_string": [
                                                                                                    "default_operator": "AND",
                                                                                                    "query"           : "y",
                                                                                                    "fields"          : ["p3.p1"]
                                                                                            ]
                                                                                    ], [
                                                                                            "simple_query_string": [
                                                                                                    "default_operator": "AND",
                                                                                                    "query"           : "z",
                                                                                                    "fields"          : ["p3.p1"]
                                                                                            ]
                                                                                    ]]
                                                                 ]
                                                         ]]
                                        ]
                                ],
                                "path"           : "p3",
                                "ignore_unmapped": true
                        ]
                ]
        ]
    }
}
