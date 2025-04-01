package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate

class NodeSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

    def "convert to ES"() {
        given:
        def tree = QueryTreeBuilder.buildTree('(NOT p1:v1 OR p2:v2) something', disambiguate)

        expect:
        tree.toEs(x -> Optional.empty()) ==
                ['bool': [
                        'must': [
                                ['bool': [
                                        'should': [
                                                ['bool': [
                                                        'filter': [
                                                                'bool': [
                                                                        'must_not': [
                                                                                'simple_query_string': [
                                                                                        'default_operator': 'AND',
                                                                                        'query'           : 'v1',
                                                                                        'fields'          : ['p1']
                                                                                ]
                                                                        ]
                                                                ]
                                                        ]
                                                ]],
                                                ['bool': [
                                                        'filter': [
                                                                'simple_query_string': [
                                                                        'default_operator': 'AND',
                                                                        'query'           : 'v2',
                                                                        'fields'          : ['p2']
                                                                ]
                                                        ]
                                                ]]
                                        ]
                                ]],
                                [
                                        'simple_query_string': [
                                                'default_operator': 'AND',
                                                'analyze_wildcard': true,
                                                'query'           : 'something'
                                        ]
                                ]
                        ]
                ]]
    }
}
