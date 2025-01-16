package whelk.search2.querytree

import spock.lang.Specification
import static DummyNodes.and
import static DummyNodes.or

import static DummyNodes.pathV1
import static DummyNodes.pathV2
import static DummyNodes.pathV3
import static DummyNodes.pathV4
import static DummyNodes.pathV5
import static DummyNodes.orXY
import static DummyNodes.andXY
import static DummyNodes.andXYZ
import static DummyNodes.type1
import static DummyNodes.type2

class AndSpec extends Specification {
    def "contains"() {
        expect:
        and.contains(node) == result

        where:
        and                | node                                | result
        andXY              | pathV1                              | true
        andXYZ             | andXY                               | true
        andXY              | andXYZ                              | false
        andXYZ             | and([andXY, pathV3])                | true
        andXYZ             | and([orXY, pathV3])                 | false
        and([orXY, andXY]) | and([orXY, pathV1])                 | true
        and([orXY, andXY]) | and([or([pathV1, pathV3]), pathV1]) | false
        and([orXY, andXY]) | and([orXY, pathV3])                 | false
    }

    def "remove"() {
        expect:
        and.remove(node) == result

        where:
        and                           | node                         | result
        andXY                         | pathV1                       | pathV2
        andXY                         | pathV3                       | andXY
        andXYZ                        | pathV3                       | andXY
        andXYZ                        | andXY                        | pathV3
        andXYZ                        | andXYZ                       | null
        andXYZ                        | and([andXY, pathV3, pathV4]) | andXYZ
        and([andXYZ, pathV4, pathV5]) | and([andXY, pathV3, pathV4]) | pathV5
    }

    def "add"() {
        expect:
        and.add(node) == result

        where:
        and   | node                                              | result
        andXY | pathV3                                            | andXYZ
        andXY | pathV2                                            | andXY
        andXY | and([pathV4, pathV5])                             | and([pathV1, pathV2, pathV4, pathV5])
        andXY | and([pathV1, pathV3])                             | andXYZ
        andXY | andXY                                             | andXY
        andXY | and([orXY, pathV3])                               | and([pathV1, pathV2, orXY, pathV3])
        andXY | and([andXY, pathV3])                              | andXYZ
        andXY | and([and([or([andXY, pathV3]), pathV2]), pathV3]) | and([pathV1, pathV2, or([andXY, pathV3]), pathV3])
    }

    def "replace"() {
        expect:
        and.replace(replace, replacement) == result

        where:
        and                                 | replace               | replacement           | result
        andXY                               | pathV1                | pathV3                | and([pathV2, pathV3])
        andXYZ                              | and([pathV2, pathV3]) | pathV4                | and([pathV1, pathV4])
        andXYZ                              | and([pathV2, pathV3]) | pathV1                | pathV1
        and([orXY, pathV3])                 | orXY                  | andXY                 | and([pathV3, pathV1, pathV2])
        and([orXY, pathV4, pathV5, pathV3]) | pathV4                | and([pathV1, pathV3]) | and([orXY, pathV5, pathV3, pathV1])
    }

    def "collect ruling types"() {
        expect:
        and.collectRulingTypes() == result

        where:
        and                                             | result
        and([type1, pathV1])                            | ["T1"]
        and([type1, type2, pathV1])                     | ["T1", "T2"]
        and([pathV1, pathV2])                           | []
        and([or([type1, type2]), pathV1])               | ["T1", "T2"]
        and([or([type1, pathV1]), or([type2, pathV2])]) | []
    }
}
