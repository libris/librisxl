package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate

class ConditionSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

    def "convert to search mapping 1"() {
        given:
        def searchMapping = QueryTreeBuilder.buildTree('p1:v1', disambiguate)
                .toSearchMapping ({n -> ['@id': '/find?_q=*']}, {n, n2 -> ['@id': '/find?_q=']})

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
                .toSearchMapping ({n -> ['@id': '/find?_q=*']}, {n, n2 -> ['@id': '/find?_q=']})

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
                .toSearchMapping ({n -> ['@id': '/find?_q=*']}, {n, n2 -> ['@id': '/find?_q=']})

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
}
