package whelk.search2.querytree

import spock.lang.Specification

import static DummyNodes.and
import static DummyNodes.or
import static DummyNodes.eq
import static DummyNodes.neq
import static DummyNodes.pathV
import static DummyNodes.andXY
import static DummyNodes.notXY
import static DummyNodes.orXY
import static DummyNodes.v1
import static DummyNodes.ft1
import static DummyNodes.notPathV1
import static DummyNodes.path1
import static DummyNodes.path2
import static DummyNodes.pathV1
import static DummyNodes.pathV2
import static DummyNodes.pathV3
import static DummyNodes.propV1
import static DummyNodes.v2

class NodeSpec extends Specification {
    def "convert to ES"() {
        given:
        def tree = and([or([notPathV1, pathV2]), ft1])

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
                                                                        'query'           : 'v:2',
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
                                                'query'           : 'ft1'
                                        ]
                                ]
                        ]
                ]]
    }
}
