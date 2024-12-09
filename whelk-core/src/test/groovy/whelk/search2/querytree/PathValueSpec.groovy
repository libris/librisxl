package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd

import static DummyNodes.eq
import static DummyNodes.neq
import static DummyNodes.pathV
import static DummyNodes.prop1
import static DummyNodes.prop2
import static DummyNodes.v1
import static DummyNodes.v3

class PathValueSpec extends Specification {
    def "convert to search mapping 1"() {
        given:
        def pathValue = pathV(new Path([prop1, '@id']), eq, v1)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), [:])

        expect:
        searchMapping == [
            'property': ['prefLabel': 'p1'],
            'equals': 'v1',
            'up': ['@id': '/find?_i=&_q=*']
        ]
    }

    def "convert to search mapping 2"() {
        given:
        def pathValue = pathV(new Path([prop1, prop2]), neq, v3)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), [:])

        expect:
        searchMapping == [
                'property': [
                        'propertyChainAxiom': [
                                ['prefLabel': 'p1'],
                                ['prefLabel': 'p2']
                        ]
                ],
                'notEquals': ['prefLabel': 'v3'],
                'up': ['@id': '/find?_i=&_q=*']
        ]
    }

    def "convert to search mapping 3"() {
        given:
        def pathValue = pathV(new Path([JsonLd.REVERSE_KEY,  prop1, JsonLd.REVERSE_KEY, prop2]), eq, v1)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), [:])

        expect:
        searchMapping == [
                'property': [
                        'propertyChainAxiom': [
                                ['inverseOf': ['prefLabel': 'p1']],
                                ['inverseOf': ['prefLabel': 'p2']]
                        ]
                ],
                'equals': 'v1',
                'up': ['@id': '/find?_i=&_q=*']
        ]
    }
}
