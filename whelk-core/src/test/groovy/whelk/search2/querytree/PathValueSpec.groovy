package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.QueryParams

class PathValueSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "convert to search mapping 1"() {
        given:
        def pathValue = QueryTreeBuilder.buildTree('p1.@id:v1', disambiguate)
        def searchMapping = pathValue.toSearchMapping(new QueryTree(pathValue), new QueryParams([:]))

        expect:
        searchMapping == [
                'property': ['@id': 'p1', '@type': 'DatatypeProperty'],
                'equals'  : 'v1',
                'up'      : ['@id': '/find?_limit=200&_q=*'],
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
                'up'       : ['@id': '/find?_limit=200&_q=*'],
                '_key'     : 'p1.p2',
                '_value'   : 'E1'
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
                'up'      : ['@id': '/find?_limit=200&_q=*'],
                '_key'    : '@reverse.p3.@reverse.p4',
                '_value'  : 'v1'
        ]
    }

    def "expand"() {
        given:
        PathValue pathValue = (PathValue) QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        pathValue.expand(jsonLd).toString() == result

        where:
        query                        | result
        "p1:v1"                      | "p1:v1"
        "p3:v1"                      | "p3._str:v1"
        "p1:T1"                      | "p1:T1"
        "p2:T1"                      | "p2:T1"
        "p3:T1"                      | "p3._str:T1"
        "p1:\"https://id.kb.se/v1\"" | "p1:\"https://id.kb.se/v1\""
        "p3:\"https://id.kb.se/v1\"" | "p3.@id:\"https://id.kb.se/v1\""
        "type:T3"                    | "type:T3 OR type:T4"
        "p10:v1"                     | "p4.p1:v1 p4.p3.@id:\"https://id.kb.se/x\""
        "p11:v1"                     | "p3.p4._str:v1 (\"p3.rdf:type\":T3 OR \"p3.rdf:type\":T4)"
    }
}
