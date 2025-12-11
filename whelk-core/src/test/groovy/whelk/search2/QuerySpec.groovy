package whelk.search2

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.querytree.ExpandedQueryTree
import whelk.search2.querytree.PostFilter
import whelk.search2.querytree.QueryTree
import whelk.search2.querytree.TestData

class QuerySpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()
    ESSettings esSettings = new ESSettings(TestData.getEsMappings(), new ESSettings.Boost([:]))

    def appConfig1 = [
            'statistics': [
                    'sliceList': [
                            ['dimensionChain': ['rdf:type']],
                            ['dimensionChain': ['p2']],
                            ['dimensionChain': ['p6']],
                    ]
            ]
    ]
    def appConfig2 = [
            "statistics": [
                    "sliceList": [
                            ["dimensionChain": ["findCategory"], "slice": ["dimensionChain": ["identifyCategory"]]],
                            ["dimensionChain": ["noneCategory"], "itemLimit": 100, "connective": "OR", "showIf": ["category"]]
                    ]
            ]
    ]

    AppParams appParams1 = new AppParams(appConfig1, jsonLd)
    AppParams appParams2 = new AppParams(appConfig2, jsonLd)

//    def "multi-selectable + nested ES query"() {
//        given:
//        def q = 'p3p1:x p3.p4:"https://id.kb.se/x"'
//        def tree = new QueryTree(q, disambiguate)
//        def appConfig = [
//                "statistics": [
//                        "sliceList": [
//                                ["dimensionChain": ["p3p1"], "connective": "OR"]
//                        ]
//                ]
//        ]
//        def appParams = new AppParams(appConfig, jsonLd)
//        def multiOrRadioSelected = new SelectedFacets(tree, appParams.sliceList).getAllMultiOrRadioSelected()
//        def mmSelectedFacets = multiOrRadioSelected.values().stream().flatMap(List::stream).toList()
//
//        def mainQuery = tree.toEs(jsonLd, esSettings, mmSelectedFacets)
//        def postFilter = Query.getEsMmSelectedFacets(multiOrRadioSelected, [], jsonLd, esSettings)
//
//
//        expect:
//        mainQuery == []
//        postFilter == []
//    }

    def "build agg query"() {
        given:
        SelectedFacets selectedFacets = new SelectedFacets(QueryTree.newEmpty(), appParams1.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams1.sliceList, jsonLd, [], esSettings, selectedFacets)

        expect:
        aggQuery == [
                "@type"    : [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "rdf:type": [
                                        "terms": [
                                                "size" : 10,
                                                "field": "@type",
                                                "order": ["_count": "desc"]
                                        ]
                                ]
                        ]
                ],
                "p2"       : [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "p2": [
                                        "terms": [
                                                "size" : 10,
                                                "field": "p2",
                                                "order": ["_count": "desc"]
                                        ]
                                ]
                        ]
                ],
                "p3.p4.@id": [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "p6": [
                                        "aggs"  : [
                                                "n": [
                                                        "terms": [
                                                                "size" : 10,
                                                                "field": "p3.p4.@id",
                                                                "order": ["_count": "desc"]
                                                        ]
                                                ]
                                        ],
                                        "nested": ["path": "p3"]
                                ]
                        ]
                ]
        ]
    }

    def "build agg query with multi-selected"() {
        given:
        SelectedFacets selectedFacets = new SelectedFacets(new QueryTree("type:(T1x OR T2x)", disambiguate), appParams1.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams1.sliceList, jsonLd, [], esSettings, selectedFacets)


        expect:
        aggQuery == [
                "@type"    : [
                        "aggs"  : [
                                "rdf:type": [
                                        "terms": [
                                                "field": "@type",
                                                "size" : 10,
                                                "order": [
                                                        "_count": "desc"
                                                ]
                                        ]
                                ]
                        ],
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ]
                ],
                "p2"       : [
                        "aggs"  : [
                                "p2": [
                                        "terms": [
                                                "field": "p2",
                                                "size" : 10,
                                                "order": [
                                                        "_count": "desc"
                                                ]
                                        ]
                                ]
                        ],
                        "filter": [
                                "bool": [
                                        "should": [[
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "@type": "T1x"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ], [
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "@type": "T2x"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ]]
                                ]
                        ]
                ],
                "p3.p4.@id": [
                        "aggs"  : [
                                "p6": [
                                        "nested": [
                                                "path": "p3"
                                        ],
                                        "aggs"  : [
                                                "n": [
                                                        "terms": [
                                                                "field": "p3.p4.@id",
                                                                "size" : 10,
                                                                "order": [
                                                                        "_count": "desc"
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ]
                        ],
                        "filter": [
                                "bool": [
                                        "should": [[
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "@type": "T1x"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ], [
                                                           "bool": [
                                                                   "filter": [
                                                                           "term": [
                                                                                   "@type": "T2x"
                                                                           ]
                                                                   ]
                                                           ]
                                                   ]]
                                ]
                        ]
                ]
        ]
    }

    def "build agg query, omit incompatible"() {
        given:
        SelectedFacets selectedFacets = new SelectedFacets(new QueryTree("type:((T1x OR T2x) T3)", disambiguate), appParams1.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams1.sliceList, jsonLd, [], esSettings, selectedFacets)

        expect:
        aggQuery == [
                "p2"       : [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "p2": [
                                        "terms": [
                                                "size" : 10,
                                                "field": "p2",
                                                "order": ["_count": "desc"]
                                        ]
                                ]
                        ]
                ],
                "p3.p4.@id": [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "p6": [
                                        "aggs"  : [
                                                "n": [
                                                        "terms": [
                                                                "size" : 10,
                                                                "field": "p3.p4.@id",
                                                                "order": ["_count": "desc"]
                                                        ]
                                                ]
                                        ],
                                        "nested": ["path": "p3"]
                                ]
                        ]
                ]
        ]
    }

    def "category ES query"() {
        given:
        def q = 'type:T1x category:"https://id.kb.se/term/ktg/Y" category:("https://id.kb.se/term/ktg/A" OR "https://id.kb.se/term/ktg/B")'
        QueryTree qt = new QueryTree(q, disambiguate)
        AppParams appParams = new AppParams(appConfig2, jsonLd)
        SelectedFacets selectedFacets = new SelectedFacets(qt, appParams.sliceList)
        ExpandedQueryTree eqt = qt.expand(jsonLd)
        PostFilter pf = PostFilter.extract(eqt, selectedFacets)

        def mainQuery = eqt.remove(pf.flattenedConditions()).toEs(esSettings)
        def postFilter = pf.qt().toEs(esSettings)


        expect:
        mainQuery == [
                "bool": [
                        "filter": [
                                "term": [
                                        "@type": "T1x"
                                ]
                        ]
                ]
        ]
        postFilter == [
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

    def "build agg query for categories"() {
        given:
        SelectedFacets selectedFacets = new SelectedFacets(QueryTree.newEmpty(), appParams2.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams2.sliceList, jsonLd, [], esSettings, selectedFacets)

        expect:
        aggQuery == [
                "_categoryByCollection.find.@id": [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "librissearch:findCategory": [
                                        "terms": [
                                                "size" : 10,
                                                "field": "_categoryByCollection.find.@id",
                                                "order": [
                                                        "_count": "desc"
                                                ]
                                        ],
                                        "aggs" : [
                                                "_categoryByCollection.identify.@id": [
                                                        "filter": [
                                                                "bool": [
                                                                        "must": []
                                                                ]
                                                        ],
                                                        "aggs"  : [
                                                                "librissearch:identifyCategory": [
                                                                        "terms": [
                                                                                "size" : 10,
                                                                                "field": "_categoryByCollection.identify.@id",
                                                                                "order": [
                                                                                        "_count": "desc"
                                                                                ]
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

    def "build agg query for categories 2"() {
        given:
        SelectedFacets selectedFacets = new SelectedFacets(new QueryTree('category:"https://id.kb.se/term/ktg/X"', disambiguate), appParams2.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams2.sliceList, jsonLd, [], esSettings, selectedFacets)

        expect:
        aggQuery == [
                "_categoryByCollection.find.@id" : [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "librissearch:findCategory": [
                                        "terms": [
                                                "order": [
                                                        "_count": "desc"
                                                ],
                                                "field": "_categoryByCollection.find.@id",
                                                "size" : 10
                                        ],
                                        "aggs" : [
                                                "_categoryByCollection.identify.@id": [
                                                        "filter": [
                                                                "bool": [
                                                                        "must": []
                                                                ]
                                                        ],
                                                        "aggs"  : [
                                                                "librissearch:identifyCategory": [
                                                                        "terms": [
                                                                                "order": [
                                                                                        "_count": "desc"
                                                                                ],
                                                                                "field": "_categoryByCollection.identify.@id",
                                                                                "size" : 10
                                                                        ]
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ]
                        ]
                ],
                "_categoryByCollection.@none.@id": [
                        "filter": [
                                "bool": [
                                        "filter": [
                                                "term": [
                                                        "_categoryByCollection.find.@id": "https://id.kb.se/term/ktg/X"
                                                ]
                                        ]
                                ]
                        ],
                        "aggs"  : [
                                "librissearch:noneCategory": [
                                        "terms": [
                                                "order": [
                                                        "_count": "desc"
                                                ],
                                                "field": "_categoryByCollection.@none.@id",
                                                "size" : 100
                                        ]
                                ]
                        ]
                ]
        ]
    }
}
