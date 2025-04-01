package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate

import static DummyNodes.and
import static DummyNodes.or

import static DummyNodes.pathV1
import static DummyNodes.pathV2
import static DummyNodes.orXY
import static DummyNodes.andXY
import static whelk.search2.querytree.DummyNodes.pathV3
import static whelk.search2.querytree.DummyNodes.pathV4
import static whelk.search2.querytree.QueryTreeBuilder.buildTree

class GroupSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

    def "flatten children on instantiation"() {
        given:
        def nestedAnd = buildTree("p1:v1 (p1:v1 p2:v2)", disambiguate)
        def or = buildTree("p1:v1 OR p2:v2", disambiguate)
        def pv = buildTree("p2:v2", disambiguate)
        def nestedOr = buildTree("(p1:v1 OR p2:v2) OR p2:v2", disambiguate)
        def children = [nestedAnd, or, pv, nestedOr]

        expect:
        new And(children).toString() == 'p1:v1 p2:v2 (p1:v1 OR p2:v2)'
        new Or(children).toString() == '(p1:v1 p2:v2) OR p1:v1 OR p2:v2'
    }

    def "equality check"() {
        given:
        def and1 = buildTree("p1:v1 p2:v2", disambiguate)
        def and2 = buildTree("p2:v2 p1:v1", disambiguate)
        def or1 = buildTree("p1:v1 OR p2:v2", disambiguate)
        def or2 = buildTree("p2:v2 OR p1:v1", disambiguate)

        expect:
        and1 == and2
        or1 == or2
        and1 != or1
        and2 != or2
        ((Set) [and1]).contains(and2)
        ((Set) [or1]).contains(or2)
        !((Set) [and1]).contains(or1)
        !((Set) [or1]).contains(and1)
    }

    def "reduce by condition"() {
        given:
        Group group = (Group) buildTree(_group, disambiguate)
        Node reduced = group.reduceByCondition { a, b -> (a == b) }

        expect:
        reduced.toString() == result

        where:
        _group                                    | result
        'p1:v1 p2:v2'                             | 'p1:v1 p2:v2'
        'p1:v1 (p1:v1 OR p2:v2)'                  | 'p1:v1'
        'p1:v1 OR (p1:v1 p2:v2)'                  | 'p1:v1'
        'p1:v1 (p1:v1 OR p3:v3)'                  | 'p1:v1'
        'p1:v1 (p2:v2 OR p3:v3)'                  | 'p1:v1 (p2:v2 OR p3:v3)'
        'p1:v1 OR (p2:v2 p3:v3)'                  | 'p1:v1 OR (p2:v2 p3:v3)'
        'p1:v1 (p1:v1 OR p2:v2 OR (p2:v2 p3:v3))' | 'p1:v1'
        'p1:v1 OR (p1:v1 2:v2 (p2:v2 OR p3:v3))'  | 'p1:v1'
        'p1:v1 (p2:v2 OR p3:v3 (p3:v3 OR p4:v4))' | 'p1:v1 (p2:v2 OR p3:v3)'
        'p1:v1 OR (p2:v2 p3:v3 (p3:v3 OR p4:v4))' | 'p1:v1 OR (p2:v2 p3:v3)'
        'p1:v1 (p2:v2 OR p3:v3 OR (p1:v1 p4:v4))' | 'p1:v1 (p2:v2 OR p3:v3 OR (p1:v1 p4:v4))'
        'p1:v1 OR (p2:v2 p3:v3 (p1:v1 OR p4:v4))' | 'p1:v1 OR (p2:v2 p3:v3 (p1:v1 OR p4:v4))'
        'p1:v1 p2:v2 (p2:v2 OR p3:v3)'            | 'p1:v1 p2:v2'
        'p1:v1 OR p2:v2 OR (p2:v2 p3:v3)'         | 'p1:v1 OR p2:v2'
        '(p1:v1 OR p2:v2) (p3:v3 OR p4:v4)'       | '(p1:v1 OR p2:v2) (p3:v3 OR p4:v4)'
        '(p1:v1 p2:v2) OR (p3:v3 p4:v4)'          | '(p1:v1 p2:v2) OR (p3:v3 p4:v4)'
        '(p1:v1 OR p2:v2) (p3:v3 OR p4:v4) p1:v1' | 'p1:v1 (p3:v3 OR p4:v4)'
        '(p1:v1 p2:v2) OR (p3:v3 p4:v4) OR p1:v1' | 'p1:v1 OR (p3:v3 p4:v4)'
        '(p1:v1 OR p2:v2) (p2:v2 OR p3:v3)'       | '(p1:v1 OR p2:v2) (p2:v2 OR p3:v3)'
        '(p1:v1 p2:v2) OR (p1:v1 p2:v2)'          | 'p1:v1 p2:v2'
    }
}
