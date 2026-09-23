package whelk.search2.esquery

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.AppParams
import whelk.search2.Disambiguate
import whelk.search2.QueryParams
import whelk.search2.SelectedFacets
import whelk.search2.TestData
import whelk.search2.querytree.QueryTree
import whelk.search2.querytree.selector.Property
import whelk.search2.querytree.value.Link

class ESQueryDefinitionSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()
    ESSettings esSettings = new ESSettings(TestData.getEsMappings(), new ESBoost([:]))

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
                            ["dimensionChain": ["instanceCategory"], "itemLimit": 100]
                    ]
            ]
    ]

    AppParams appParams1 = new AppParams(appConfig1, jsonLd)
    AppParams appParams2 = new AppParams(appConfig2, jsonLd)

    def "build aggs query"() {
        given:
        QueryTree queryTree = QueryTree.newEmpty()
        QueryParams queryParams = new QueryParams([:])
        SelectedFacets selectedFacets = new SelectedFacets(queryTree, appParams1.sliceList)
        ESQueryDefinition.AggsDefinition aggs = new ESQueryDefinition.AggsDefinition(appParams1.sliceList, selectedFacets, [])
        ESQueryDefinition queryDefinition = new ESQueryDefinition(queryTree, esSettings, queryParams, jsonLd, aggs, null)

        expect:
        queryDefinition.buildAggsQuery() == [
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

    def "build aggs query with multi-selected"() {
        given:
        QueryTree queryTree = new QueryTree("type:(T1x OR T2x)", disambiguate)
        QueryParams queryParams = new QueryParams([:])
        SelectedFacets selectedFacets = new SelectedFacets(queryTree, appParams1.sliceList)
        ESQueryDefinition.AggsDefinition aggs = new ESQueryDefinition.AggsDefinition(appParams1.sliceList, selectedFacets, [])
        ESQueryDefinition queryDefinition = new ESQueryDefinition(queryTree, esSettings, queryParams, jsonLd, aggs, null)

        expect:
        queryDefinition.buildAggsQuery() == [
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

    def "build aggs query, omit incompatible"() {
        given:
        QueryTree queryTree = new QueryTree("type:((T1x OR T2x) T3)", disambiguate)
        QueryParams queryParams = new QueryParams([:])
        SelectedFacets selectedFacets = new SelectedFacets(queryTree, appParams1.sliceList)
        ESQueryDefinition.AggsDefinition aggs = new ESQueryDefinition.AggsDefinition(appParams1.sliceList, selectedFacets, [])
        ESQueryDefinition queryDefinition = new ESQueryDefinition(queryTree, esSettings, queryParams, jsonLd, aggs, null)

        expect:
        queryDefinition.buildAggsQuery() == [
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

    def "build aggs query for categories"() {
        given:
        QueryTree queryTree = QueryTree.newEmpty()
        QueryParams queryParams = new QueryParams([:])
        SelectedFacets selectedFacets = new SelectedFacets(queryTree, appParams2.sliceList)
        ESQueryDefinition.AggsDefinition aggs = new ESQueryDefinition.AggsDefinition(appParams2.sliceList, selectedFacets, ['T2'])
        ESQueryDefinition queryDefinition = new ESQueryDefinition(queryTree, esSettings, queryParams, jsonLd, aggs, null)

        expect:
        queryDefinition.buildAggsQuery() == [
                "_categoryByCollection.find.@id" : [
                        "filter" : [
                                "match_all" : [:]
                        ],
                        "aggs" : [
                                "librissearch:findCategory" : [
                                        "terms" : [
                                                "size" : 10,
                                                "field" : "_categoryByCollection.find.@id",
                                                "order" : [
                                                        "_count" : "desc"
                                                ]
                                        ],
                                        "aggs" : [
                                                "_categoryByCollection.identify.@id" : [
                                                        "filter" : [
                                                                "match_all" : [:]
                                                        ],
                                                        "aggs" : [
                                                                "librissearch:identifyCategory" : [
                                                                        "terms" : [
                                                                                "size" : 10,
                                                                                "field" : "_categoryByCollection.identify.@id",
                                                                                "order" : [
                                                                                        "_count" : "desc"
                                                                                ]
                                                                        ]
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ]
                        ]
                ],
                "@reverse.instanceOf._categoryByCollection.@none.@id" : [
                        "filter" : [
                                "match_all" : [:]
                        ],
                        "aggs" : [
                                "librissearch:instanceCategory" : [
                                        "terms" : [
                                                "size" : 100,
                                                "field" : "@reverse.instanceOf._categoryByCollection.@none.@id",
                                                "order" : [
                                                        "_count" : "desc"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ]
    }

    def "build aggs query for categories 2"() {
        given:
        QueryTree queryTree = new QueryTree('workCategory:"https://id.kb.se/term/ktg/X"', disambiguate)
        QueryParams queryParams = new QueryParams([:])
        SelectedFacets selectedFacets = new SelectedFacets(queryTree, appParams2.sliceList)
        ESQueryDefinition.AggsDefinition aggs = new ESQueryDefinition.AggsDefinition(appParams2.sliceList, selectedFacets, ['T2'])
        ESQueryDefinition queryDefinition = new ESQueryDefinition(queryTree, esSettings, queryParams, jsonLd, aggs, null)

        expect:
        queryDefinition.buildAggsQuery() == [
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
                "@reverse.instanceOf._categoryByCollection.@none.@id" : [
                        "aggs" : [
                                "librissearch:instanceCategory" : [
                                        "terms" : [
                                                "order" : [
                                                        "_count" : "desc"
                                                ],
                                                "size" : 100,
                                                "field" : "@reverse.instanceOf._categoryByCollection.@none.@id"
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

    def "build aggs query for categories 3"() {
        given:
        QueryTree queryTree = new QueryTree('workCategory:"https://id.kb.se/term/ktg/Y"', disambiguate)
        QueryParams queryParams = new QueryParams([:])
        SelectedFacets selectedFacets = new SelectedFacets(queryTree, appParams2.sliceList)
        ESQueryDefinition.AggsDefinition aggs = new ESQueryDefinition.AggsDefinition(appParams2.sliceList, selectedFacets, ['T1'])
        ESQueryDefinition queryDefinition = new ESQueryDefinition(queryTree, esSettings, queryParams, jsonLd, aggs, null)

        expect:
        queryDefinition.buildAggsQuery() == [
                "instanceOf._categoryByCollection.find.@id" : [
                        "aggs" : [
                                "librissearch:findCategory" : [
                                        "terms" : [
                                                "size" : 10,
                                                "field" : "instanceOf._categoryByCollection.find.@id",
                                                "order" : [
                                                        "_count" : "desc"
                                                ]
                                        ],
                                        "aggs" : [
                                                "instanceOf._categoryByCollection.identify.@id" : [
                                                        "aggs" : [
                                                                "librissearch:identifyCategory" : [
                                                                        "terms" : [
                                                                                "size" : 10,
                                                                                "field" : "instanceOf._categoryByCollection.identify.@id",
                                                                                "order" : [
                                                                                        "_count" : "desc"
                                                                                ]
                                                                        ]
                                                                ]
                                                        ],
                                                        "filter" : [
                                                                "match_all" : [:]
                                                        ]
                                                ]
                                        ]
                                ]
                        ],
                        "filter" : [
                                "match_all" : [:]
                        ]
                ],
                "instanceOf._categoryByCollection.@none.@id" : [
                        "aggs" : [
                                "librissearch:noneCategory" : [
                                        "terms" : [
                                                "size" : 100,
                                                "field" : "instanceOf._categoryByCollection.@none.@id",
                                                "order" : [
                                                        "_count" : "desc"
                                                ]
                                        ]
                                ]
                        ],
                        "filter" : [
                                "bool" : [
                                        "filter" : [
                                                "term" : [
                                                        "instanceOf._categoryByCollection.identify.@id" : "https://id.kb.se/term/ktg/Y"
                                                ]
                                        ]
                                ]
                        ]
                ],
                "_categoryByCollection.@none.@id" : [
                        "aggs" : [
                                "librissearch:instanceCategory" : [
                                        "terms" : [
                                                "size" : 100,
                                                "field" : "_categoryByCollection.@none.@id",
                                                "order" : [
                                                        "_count" : "desc"
                                                ]
                                        ]
                                ]
                        ],
                        "filter" : [
                                "bool" : [
                                        "filter" : [
                                                "term" : [
                                                        "instanceOf._categoryByCollection.identify.@id" : "https://id.kb.se/term/ktg/Y"
                                                ]
                                        ]
                                ]
                        ]
                ]
        ]
    }

    def "build aggs query with curated predicates"() {
        given:
        QueryTree queryTree = new QueryTree('type:T2x', disambiguate)
        QueryParams queryParams = new QueryParams([:])
        Link object = new Link("https://libris.kb.se/fcrtpljz1qp2bdv#it")
        List<ESQueryDefinition.PredicateDefinition> predicates = [
                new ESQueryDefinition.PredicateDefinition(Property.getProperty('p19', jsonLd), ['T2x']),
                new ESQueryDefinition.PredicateDefinition(Property.getProperty('p20', jsonLd), ['T3'])
        ]
        ESQueryDefinition.PAggsDefinition pAggs = new ESQueryDefinition.PAggsDefinition(object, predicates)
        ESQueryDefinition queryDefinition = new ESQueryDefinition(queryTree, esSettings, queryParams, jsonLd, null, pAggs)

        expect:
        queryDefinition.buildPAggsQuery() == [
                "_p": [
                        "filters": [
                                "filters": [
                                        "p19": [
                                                "bool": [
                                                        "filter": [
                                                                "term": [
                                                                        "@reverse.instanceOf.p19.@id": "https://libris.kb.se/fcrtpljz1qp2bdv#it"
                                                                ]
                                                        ]
                                                ]
                                        ],
                                        "p20": [
                                                "bool": [
                                                        "filter": [
                                                                "term": [
                                                                        "p20.@id": "https://libris.kb.se/fcrtpljz1qp2bdv#it"
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
