package whelk.search2.querytree

import spock.lang.Specification

import static DummyNodes.and
import static DummyNodes.or

import static DummyNodes.pathV1
import static DummyNodes.pathV2
import static DummyNodes.orXY
import static DummyNodes.andXY
import static whelk.search2.querytree.DummyNodes.pathV3
import static whelk.search2.querytree.DummyNodes.pathV4

class GroupSpec extends Specification {
    def "flatten children on instantiation"() {
        given:
        def children = [and([pathV1, andXY]), orXY, pathV2, or([orXY, pathV2])]

        expect:
        new And(children).children() == [pathV1, pathV2, orXY]
        new Or(children).children() == [andXY, pathV1, pathV2]
    }

    def "equality check"() {
        expect:
        andXY.equals(and([pathV1, pathV2]))
        orXY.equals(or([pathV1, pathV2]))
        ((Set) [andXY]).contains(and([pathV1, pathV2]))
        !((Set) [andXY]).contains(orXY)
        ((Set) [orXY]).contains(or([pathV1, pathV2]))
        !((Set) [orXY]).contains(andXY)
    }

    def "reduce by condition"() {
        expect:
        ((Group) group).reduceByCondition { a, b -> (a == b) } == result

        where:
        group                                                      | result
        andXY                                                      | andXY
        new And([pathV1, pathV1], false)                           | pathV1
        and([pathV1, or([pathV1, pathV2])])                        | pathV1
        or([pathV1, and([pathV1, pathV2])])                        | pathV1
        and([pathV1, or([pathV2, pathV3])])                        | and([pathV1, or([pathV2, pathV3])])
        or([pathV1, and([pathV2, pathV3])])                        | or([pathV1, and([pathV2, pathV3])])
        and([pathV1, or([pathV1, pathV3, and([pathV3, pathV4])])]) | pathV1
        or([pathV1, and([pathV1, pathV3, or([pathV3, pathV4])])])  | pathV1
        and([pathV1, or([pathV2, pathV3, and([pathV3, pathV4])])]) | and([pathV1, or([pathV2, pathV3])])
        or([pathV1, and([pathV2, pathV3, or([pathV3, pathV4])])])  | or([pathV1, and([pathV2, pathV3])])
        and([pathV1, or([pathV2, pathV3, and([pathV1, pathV4])])]) | and([pathV1, or([pathV2, pathV3, and([pathV1, pathV4])])])
        or([pathV1, and([pathV2, pathV3, or([pathV1, pathV4])])])  | or([pathV1, and([pathV2, pathV3, or([pathV1, pathV4])])])
        and([pathV2, pathV3, or([pathV3, pathV4])])                | and([pathV2, pathV3])
        or([pathV2, pathV3, and([pathV3, pathV4])])                | or([pathV2, pathV3])
        and([or([pathV1, pathV2]), or([pathV3, pathV4])])          | and([or([pathV1, pathV2]), or([pathV3, pathV4])])
        or([and([pathV1, pathV2]), and([pathV3, pathV4])])         | or([and([pathV1, pathV2]), and([pathV3, pathV4])])
        and([or([pathV1, pathV2]), or([pathV3, pathV4]), pathV1])  | and([pathV1, or([pathV3, pathV4])])
        or([and([pathV1, pathV2]), and([pathV3, pathV4]), pathV1]) | or([pathV1, and([pathV3, pathV4])])
        and([or([pathV1, pathV2]), or([pathV2, pathV3])])          | or([pathV1, pathV2])
        or([and([pathV1, pathV2]), and([pathV2, pathV3])])         | and([pathV1, pathV2])
        or([and([pathV1, pathV2]), and([pathV1, pathV2])])         | and([pathV1, pathV2])
    }
}
