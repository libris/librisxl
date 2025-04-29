package whelk.search2

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.querytree.Link
import whelk.search2.querytree.Property
import whelk.search2.querytree.TestData

class AggsSpec extends Specification {
    JsonLd jsonLd = TestData.getJsonLd()
    EsMappings esMappings = TestData.getEsMappings()

    def "build agg query"() {
        given:
        List<AppParams.Slice> sliceList = ['p1', 'p2', 'p6']
                .collect { new AppParams.Slice(it, ['size': 100, 'sortOrder': 'desc', 'sort': 'count']) }
        Map aggQuery = Aggs.buildAggQuery(sliceList, jsonLd, [], esMappings)

        expect:
        aggQuery == [
                "p1"       : [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "p1": [
                                        "terms": [
                                                "size" : 100,
                                                "field": "p1",
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
                                                "size" : 100,
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
                                                                "size" : 100,
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

    def "build agg query 2"() {
        given:
        List<AppParams.Slice> sliceList = ['p7', 'p8', 'p9']
                .collect { new AppParams.Slice(it, ['size': 100, 'sortOrder': 'desc', 'sort': 'count']) }
        Map aggQuery = Aggs.buildAggQuery(sliceList, jsonLd, ['T1'], esMappings)

        expect:
        aggQuery == [
                "p7.@id"           : [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "p7": [
                                        "terms": [
                                                "order": [
                                                        "_count": "desc"
                                                ],
                                                "field": "p7.@id",
                                                "size" : 100
                                        ]
                                ]
                        ]
                ],
                "instanceOf.p8.@id": [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "p8": [
                                        "terms": [
                                                "order": [
                                                        "_count": "desc"
                                                ],
                                                "field": "instanceOf.p8.@id",
                                                "size" : 100
                                        ]
                                ]
                        ]
                ],
                "p9.@id"           : [
                        "filter": [
                                "bool": [
                                        "must": []
                                ]
                        ],
                        "aggs"  : [
                                "p9": [
                                        "terms": [
                                                "order": [
                                                        "_count": "desc"
                                                ],
                                                "field": "p9.@id",
                                                "size" : 100
                                        ]
                                ]
                        ]
                ]
        ]
    }

    def "build agg query with curated predicates"() {
        given:
        Link object = new Link("https://libris.kb.se/fcrtpljz1qp2bdv#it")
        List<Property> predicates = ['p2', 'p6'].collect { new Property(it, jsonLd) }
        Map aggQuery = Aggs.buildPAggQuery(object, predicates, jsonLd, List.of(), esMappings)

        expect:
        aggQuery == [
                "_p": [
                        "filters": [
                                "filters": [
                                        "p2": [
                                                "bool": [
                                                        "filter": [
                                                                "term": [
                                                                        "p2": "https://libris.kb.se/fcrtpljz1qp2bdv#it"
                                                                ]
                                                        ]
                                                ]
                                        ],
                                        "p6": [
                                                "bool": [
                                                        "must": [
                                                                "nested": [
                                                                        "path" : "p3",
                                                                        "query": [
                                                                                "bool": [
                                                                                        "filter": [
                                                                                                "term": [
                                                                                                        "p3.p4.@id": "https://libris.kb.se/fcrtpljz1qp2bdv#it"
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
                ]
        ]
    }
}
