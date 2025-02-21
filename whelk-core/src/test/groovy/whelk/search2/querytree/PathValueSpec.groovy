package whelk.search2.querytree

import spock.lang.Specification

import static DummyNodes.eq
import static DummyNodes.neq
import static DummyNodes.pathV
import static DummyNodes.prop1
import static DummyNodes.prop2
import static DummyNodes.v1
import static DummyNodes.v3
import static whelk.JsonLd.REVERSE_KEY

class PathValueSpec extends Specification {
    def "convert to search mapping 1"() {
        given:
        def pathValue = pathV(new Path([prop1, new Key.RecognizedKey('@id')]), eq, v1)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), [:])

        expect:
        searchMapping == [
                'property': ['prefLabel': 'p1'],
                'equals'  : 'v1',
                'up'      : ['@id': '/find?_i=&_q=*'],
                '_key'    : 'p1.@id',
                '_value'  : 'v1'
        ]
    }

    def "convert to search mapping 2"() {
        given:
        def pathValue = pathV(new Path([prop1, prop2]), neq, v3)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), [:])

        expect:
        searchMapping == [
                'property' : [
                        'propertyChainAxiom': [
                                ['prefLabel': 'p1'],
                                ['prefLabel': 'p2']
                        ]
                ],
                'notEquals': ['prefLabel': 'v3'],
                'up'       : ['@id': '/find?_i=&_q=*'],
                '_key'    : 'p1.p2',
                '_value'  : 'v3'
        ]
    }

    def "convert to search mapping 3"() {
        given:
        def pathValue = pathV(new Path([new Key.RecognizedKey(REVERSE_KEY), prop1, new Key.RecognizedKey(REVERSE_KEY), prop2]), eq, v1)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), [:])

        expect:
        searchMapping == [
                'property': [
                        'propertyChainAxiom': [
                                ['inverseOf': ['prefLabel': 'p1']],
                                ['inverseOf': ['prefLabel': 'p2']]
                        ]
                ],
                'equals'  : 'v1',
                'up'      : ['@id': '/find?_i=&_q=*'],
                '_key'    : '@reverse.p1.@reverse.p2',
                '_value'  : 'v1'
        ]
    }
}
