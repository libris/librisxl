package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate
import whelk.search2.QueryParams

class PathValueSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

    def "convert to search mapping 1"() {
        given:
        def pathValue = QueryTreeBuilder.buildTree('p1.@id:v1', disambiguate)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), new QueryParams([:]))

        expect:
        searchMapping == [
                'property': ['@id': 'p1', '@type': 'DatatypeProperty'],
                'equals'  : 'v1',
                'up'      : ['@id': '/find?_limit=200&_i=&_q=*'],
                '_key'    : 'p1.@id',
                '_value'  : 'v1'
        ]
    }

    def "convert to search mapping 2"() {
        given:
        def pathValue = QueryTreeBuilder.buildTree('NOT p1.p2:E1', disambiguate)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), new QueryParams([:]))

        expect:
        searchMapping == [
                'property' : [
                        'propertyChainAxiom': [
                                ['@id': 'p1', '@type': 'DatatypeProperty'],
                                ['@id': 'p2', '@type': 'ObjectProperty']
                        ]
                ],
                'notEquals': ['@id': 'E1', '@type': 'Class'],
                'up'       : ['@id': '/find?_limit=200&_i=&_q=*'],
                '_key'    : 'p1.p2',
                '_value'  : 'E1'
        ]
    }

    def "convert to search mapping 3"() {
        given:
        def pathValue = QueryTreeBuilder.buildTree('@reverse.p3.@reverse.p4:v1', disambiguate)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), new QueryParams([:]))

        expect:
        searchMapping == [
                'property': [
                        'propertyChainAxiom': [
                                ['inverseOf': ['@id': 'p3', '@type': 'ObjectProperty']],
                                ['inverseOf': ['@id': 'p4', '@type': 'ObjectProperty']]
                        ]
                ],
                'equals'  : 'v1',
                'up'      : ['@id': '/find?_limit=200&_i=&_q=*'],
                '_key'    : '@reverse.p3.@reverse.p4',
                '_value'  : 'v1'
        ]
    }
}
