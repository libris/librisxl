package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.ESSettings
import whelk.search2.EsMappings

class ConditionSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()
    EsMappings esMappings = TestData.getEsMappings()

    def "convert to search mapping 1"() {
        given:
        def searchMapping = QueryTreeBuilder.buildTree('p1:v1', disambiguate)
                .toSearchMapping {n -> ['@id': '/find?_q=*'] }

        expect:
        searchMapping == [
                'property': ['@id': 'p1', '@type': 'DatatypeProperty'],
                'equals'  : 'v1',
                'up'      : ['@id': '/find?_q=*'],
                '_key'    : 'p1',
                '_value'  : 'v1'
        ]
    }

    def "convert to search mapping 2"() {
        given:
        def searchMapping = QueryTreeBuilder.buildTree('NOT p1.p2:E1', disambiguate)
                .toSearchMapping {n -> ['@id': '/find?_q=*'] }

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
                        'up'      : ['@id': '/find?_q=*'],
                        '_key'    : 'p1.p2',
                        '_value'  : 'E1'
                ],
                'up' : ['@id': '/find?_q=*']
        ]
    }

    def "convert to search mapping 3"() {
        given:
        def searchMapping = QueryTreeBuilder.buildTree('@reverse.p3.@reverse.p4:v1', disambiguate)
                .toSearchMapping {n -> ['@id': '/find?_q=*'] }

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
                'up'      : ['@id': '/find?_q=*'],
                '_key'    : '@reverse.p3.@reverse.p4',
                '_value'  : 'v1'
        ]
    }

    def "expand"() {
        given:
        Condition statement = (Condition) QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        statement.expand(jsonLd, []).toString() == result

        where:
        query                        | result
        "type:T3"                    | "type:T3 OR type:T3x"
        "p10:v1"                     | "p4.p1:v1 p4.p3:\"https://id.kb.se/x\""
        "p11:v1"                     | "p3.p4:v1 (\"p3.rdf:type\":T3 OR \"p3.rdf:type\":T3x)"
    }

    def "expand 2"() {
        given:
        Condition statement = (Condition) QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        statement.expand(jsonLd, subjectTypes).toString() == result

        where:
        query               | subjectTypes | result
        "p1:v1"             | []           | "p1:v1"
        "p1:v1"             | ["T1"]       | "instanceOf.p1:v1 OR p1:v1"
        "p1:v1"             | ["T2"]       | "hasInstance.p1:v1 OR p1:v1"
        "p1:v1"             | ["T3"]       | "p1:v1"
        "hasInstance.p7:v7" | ["T1"]       | "p7:v7"
        "hasInstance.p7:v7" | ["T2"]       | "hasInstance.p7:v7"
        "instanceOf.p8:v8"  | ["T1"]       | "instanceOf.p8:v8"
        "instanceOf.p8:v8"  | ["T2"]       | "p8:v8"
        "p5:x"              | []           | "meta.p5:x"
        "meta.p5:x"         | []           | "meta.p5:x"
        "bibliography:x"    | ["T1"]       | "meta.bibliography:x"
        "bibliography:x"    | ["T2"]       | "meta.bibliography:x OR hasInstance.meta.bibliography:x"
    }

    def "To ES query (negation + nested field)"() {
        given:
        var tree = QueryTreeBuilder.buildTree("NOT p3:\"https://id.kb.se/x\"", disambiguate)
        ESSettings esSettings = new ESSettings(esMappings, new ESSettings.Boost([:]))

        expect:
        tree.toEs(esSettings) == [
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
                                        "path" : "p3"
                                ]
                        ]
                ]
        ]
    }
}
