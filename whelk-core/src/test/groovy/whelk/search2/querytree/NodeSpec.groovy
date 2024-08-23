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
        tree.toEs() ==
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

    def "fail conversion to ES due to bad node type in tree"() {
        given:
        def tree = and([propV1, ft1])

        when:
        tree.toEs()

        then:
        thrown UnsupportedOperationException
    }

    def "insert operator"() {
        expect:
        tree.insertOperator(eq) == resEq
        tree.insertOperator(neq) == resNeq

        where:
        tree                                                  | resEq                | resNeq
        propV1                                                | propV1               | propV1
        pathV(path1, null, v1)                                | pathV(path1, eq, v1) | pathV(path1, neq, v1)
        and([pathV(path1, null, v1), pathV(path2, null, v2)]) | andXY                | notXY
        or([pathV(path1, null, v1), pathV(path2, null, v2)])  | orXY                 | notXY
    }

    def "fail inserting operator if already exists"() {
        when:
        andXY.insertOperator(eq)

        then:
        thrown UnsupportedOperationException
    }

    def "insert value"() {
        expect:
        tree.insertValue(v2) == res

        where:
        tree                                  | res
        propV1                                | propV1
        pathV1                                | pathV1
        pathV(path2, eq, null)                | pathV2
        and([pathV1, pathV(path2, eq, null)]) | and([pathV1, pathV2])
    }

    def "insert nested"() {
        given:
        def getNestedPath = { p ->
            ['p1', 'p2'].find { p.startsWith(it) }.with { Optional.ofNullable(it) }
        }
        def tree = and([orXY, pathV(new Path(['p1', 'p2']), eq, v1), pathV3]).insertNested(getNestedPath)
        def or = tree.children()[0]

        expect:
        ((PathValue) or.children()[0]).getNestedStem() == Optional.of('p1')
        ((PathValue) or.children()[1]).getNestedStem() == Optional.of('p2')
        ((PathValue) tree.children()[1]).getNestedStem() == Optional.of('p1')
        ((PathValue) tree.children()[2]).getNestedStem() == Optional.empty()
    }

    def "modify paths"() {
        given:
        def modifier = pathValue -> pathValue.path() == path1 ? pathV2 : pathV3

        expect:
        new And([pathV1, pathV2]).modifyAllPathValue(modifier) == new And([pathV2, pathV3])
        new And([ft1, propV1]).modifyAllPathValue(modifier) == new And([ft1, propV1])
    }
}
