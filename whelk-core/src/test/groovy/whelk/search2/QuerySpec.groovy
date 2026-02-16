package whelk.search2

import spock.lang.Specification
import whelk.JsonLd
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
                            ['dimensionChain': ['p6']]
                    ]
            ]
    ]
    def appConfig2 = [
            "statistics": [
                    "sliceList": [
                            ["dimensionChain": ["findCategory"], "slice": ["dimensionChain": ["identifyCategory"]]],
                            ["dimensionChain": ["noneCategory"], "itemLimit": 100, "connective": "OR", "showIf": ["category"]],
                            ["dimensionChain": ["hasInstanceCategory"], "itemLimit": 100]
                    ]
            ]
    ]

    AppParams appParams1 = new AppParams(appConfig1, jsonLd)
    AppParams appParams2 = new AppParams(appConfig2, jsonLd)

    def "build agg query"() {
        given:
        SelectedFacets selectedFacets = new SelectedFacets(QueryTree.newEmpty(), appParams1.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams1.sliceList, jsonLd, [], esSettings, selectedFacets)

        expect:
        aggQuery == [
                "@type"    : [
                        "filter": [
                                "match_all": [:]
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
                                "match_all": [:]
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
                                "match_all": [:]
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
                                "match_all": [:]
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
                                "match_all": [:]
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
                                "match_all": [:]
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

    def "build agg query for categories"() {
        given:
        SelectedFacets selectedFacets = new SelectedFacets(QueryTree.newEmpty(), appParams2.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams2.sliceList, jsonLd, ['T2'], esSettings, selectedFacets)

        expect:
        aggQuery == [
            "_categoryByCollection.find.@id" : [
                "aggs" : [
                    "librissearch:findCategory" : [
                        "terms" : [
                            "order" : [
                                "_count" : "desc"
                            ],
                            "field" : "_categoryByCollection.find.@id",
                            "size" : 10
                        ],
                        "aggs" : [
                            "_categoryByCollection.identify.@id" : [
                                "aggs" : [
                                    "librissearch:identifyCategory" : [
                                        "terms" : [
                                            "order" : [
                                                "_count" : "desc"
                                            ],
                                            "field" : "_categoryByCollection.identify.@id",
                                            "size" : 10
                                        ]
                                    ]
                                ],
                                "filter" : [
                                        "match_all": [:]
                                ]
                            ]
                        ]
                    ]
                ],
                "filter" : [
                        "match_all": [:]
                ]
            ],
            "@reverse.instanceOf.category.@id" : [
                "aggs" : [
                    "hasInstanceCategory" : [
                        "terms" : [
                            "order" : [
                                "_count" : "desc"
                            ],
                            "field" : "@reverse.instanceOf.category.@id",
                            "size" : 100
                        ]
                    ]
                ],
                "filter" : [
                        "match_all": [:]
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
                "aggs" : [
                    "librissearch:findCategory" : [
                        "terms" : [
                            "order" : [
                                "_count" : "desc"
                            ],
                            "size" : 10,
                            "field" : "_categoryByCollection.find.@id"
                        ],
                        "aggs" : [
                            "_categoryByCollection.identify.@id" : [
                                "aggs" : [
                                    "librissearch:identifyCategory" : [
                                        "terms" : [
                                            "order" : [
                                                "_count" : "desc"
                                            ],
                                            "size" : 10,
                                            "field" : "_categoryByCollection.identify.@id"
                                        ]
                                    ]
                                ],
                                "filter" : [
                                        "match_all": [:]
                                ]
                            ]
                        ]
                    ]
                ],
                "filter" : [
                        "match_all": [:]
                ]
            ],
            "_categoryByCollection.@none.@id" : [
                "aggs" : [
                    "librissearch:noneCategory" : [
                        "terms" : [
                            "order" : [
                                "_count" : "desc"
                            ],
                            "size" : 100,
                            "field" : "_categoryByCollection.@none.@id"
                        ]
                    ]
                ],
                "filter" : [
                    "bool" : [
                        "filter" : [
                            "term" : [
                                "_categoryByCollection.find.@id" : "https://id.kb.se/term/ktg/X"
                            ]
                        ]
                    ]
                ]
            ],
            "@reverse.instanceOf.category.@id" : [
                "aggs" : [
                    "hasInstanceCategory" : [
                        "terms" : [
                            "order" : [
                                "_count" : "desc"
                            ],
                            "size" : 100,
                            "field" : "@reverse.instanceOf.category.@id"
                        ]
                    ]
                ],
                "filter" : [
                    "bool" : [
                        "filter" : [
                            "term" : [
                                "_categoryByCollection.find.@id" : "https://id.kb.se/term/ktg/X"
                            ]
                        ]
                    ]
                ]
            ]
        ]
    }
}
