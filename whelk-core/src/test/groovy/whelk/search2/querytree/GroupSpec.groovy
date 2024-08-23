package whelk.search2.querytree

import spock.lang.Specification

import static DummyNodes.and
import static DummyNodes.or

import static DummyNodes.pathV1
import static DummyNodes.pathV2
import static DummyNodes.orXY
import static DummyNodes.andXY

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
}
