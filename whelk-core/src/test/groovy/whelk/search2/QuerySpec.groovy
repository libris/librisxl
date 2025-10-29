package whelk.search2

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.querytree.QueryTree
import whelk.search2.querytree.TestData

class QuerySpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()
    ESSettings esSettings = new ESSettings(TestData.getEsMappings(), new ESSettings.Boost([:]))
    AppParams appParams = TestData.getAppParams()

    def "build agg query"() {
        given:
        SelectedFacets selectedFacets = new SelectedFacets(QueryTree.empty(), appParams.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams.sliceList, jsonLd, [], esSettings, selectedFacets)

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
        SelectedFacets selectedFacets = new SelectedFacets(new QueryTree("type:(T1x OR T2x)", disambiguate), appParams.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams.sliceList, jsonLd, [], esSettings, selectedFacets)


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
        SelectedFacets selectedFacets = new SelectedFacets(new QueryTree("type:((T1x OR T2x) T3)", disambiguate), appParams.sliceList)
        Map aggQuery = Query.buildAggQuery(appParams.sliceList, jsonLd, [], esSettings, selectedFacets)

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
}
