package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate
import whelk.search2.QueryParams
import whelk.search2.SearchMapping

class SearchMappingSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    QueryParams queryParams = new QueryParams([:])
    String apiParam = 'q'

    def "build search mapping for free text query"() {
        given:
        QueryTree qt = new QueryTree('a b c', disambiguate)
        Map<String, Object> searchMapping = SearchMapping.buildFrom(qt, queryParams, apiParam)

        expect:
        searchMapping == [
            "property" : [
                "@id" : "textQuery",
                "@type" : "DatatypeProperty"
            ],
            "equals" : "a b c",
            "up" : [
                "@id" : "/find?q="
            ]
        ]
    }

    def "build search mapping for condition with free text value"() {
        given:
        QueryTree qt = new QueryTree('p1:v1', disambiguate)
        Map<String, Object> searchMapping = SearchMapping.buildFrom(qt, queryParams, apiParam)

        expect:
        searchMapping == [
                'property': ['@id': 'p1', '@type': 'DatatypeProperty'],
                'equals'  : 'v1',
                'up'      : ['@id': '/find?q='],
                '_key'    : 'p1',
                '_value'  : 'v1'
        ]
    }

    def "build search mapping for negated condition with vocab term value"() {
        given:
        QueryTree qt = new QueryTree('NOT p1.p2:E1', disambiguate)
        Map<String, Object> searchMapping = SearchMapping.buildFrom(qt, queryParams, apiParam)

        expect:
        searchMapping == [
                'not': [
                        'property': [
                                'propertyChainAxiom': [
                                        ['@list': [
                                                ['@id': 'p1', '@type': 'DatatypeProperty'],
                                                ['@id': 'p2', '@type': 'ObjectProperty', 'librisQueryCode': 'P2']
                                        ]]
                                ]
                        ],
                        'equals'  : ['@id': 'E1', '@type': 'Class'],
                        'up'      : ['@id': '/find?q='],
                        '_key'    : 'p1.p2',
                        '_value'  : 'E1'
                ],
                'up' : ['@id': '/find?q=']
        ]
    }

    def "build search mapping for condition with reverse key in selector"() {
        given:
        QueryTree qt = new QueryTree('@reverse.p3.@reverse.p4:v1', disambiguate)
        Map<String, Object> searchMapping = SearchMapping.buildFrom(qt, queryParams, apiParam)

        expect:
        searchMapping == [
                'property': [
                        'propertyChainAxiom': [
                                ['@list': [
                                        ['inverseOf': ['@id': 'p3', '@type': 'ObjectProperty']],
                                        ['inverseOf': ['@id': 'p4', '@type': 'ObjectProperty']]
                                ]]
                        ]
                ],
                'equals'  : 'v1',
                'up'      : ['@id': '/find?q='],
                '_key'    : '@reverse.p3.@reverse.p4',
                '_value'  : 'v1'
        ]
    }

    def "build search mapping for prefer like condition"() {
        given:
        QueryTree qt = new QueryTree('p5:"https://id.kb.se/X"', disambiguate)
        Map<String, Object> searchMapping = SearchMapping.buildFrom(qt, queryParams, apiParam)

        expect:
        searchMapping == [
                "property": [
                        "@id"     : "p5",
                        "@type"   : "ObjectProperty",
                        "category": [
                                "@id": "https://id.kb.se/ns/librissearch/preferLike"
                        ]
                ],
                "equals"  : [:],
                "up"      : [
                        "@id": "/find?q="
                ],
                "toLike"  : [
                        "@id": "/find?q=p5~%22https://id.kb.se/X%22"
                ],
                "_key"    : "p5",
                "_value"  : "\"https://id.kb.se/X\""
        ]
    }

    def "build search mapping for group"() {
        given:
        QueryTree qt = new QueryTree('p1:v1 p2:E1', disambiguate)
        Map<String, Object> searchMapping = SearchMapping.buildFrom(qt, queryParams, apiParam)

        expect:
        searchMapping == [
                "and": [[
                                "property": [
                                        "@id"  : "p1",
                                        "@type": "DatatypeProperty"
                                ],
                                "equals"  : "v1",
                                "up"      : [
                                        "@id": "/find?q=p2:E1"
                                ],
                                "_key"    : "p1",
                                "_value"  : "v1"
                        ], [
                                "property": [
                                        "@id"            : "p2",
                                        "@type"          : "ObjectProperty",
                                        "librisQueryCode": "P2"
                                ],
                                "equals"  : [
                                        "@id"  : "E1",
                                        "@type": "Class"
                                ],
                                "up"      : [
                                        "@id": "/find?q=p1:v1"
                                ],
                                "_key"    : "p2",
                                "_value"  : "E1"
                        ]],
                "up" : [
                        "@id": "/find?q="
                ]
        ]
    }
}
