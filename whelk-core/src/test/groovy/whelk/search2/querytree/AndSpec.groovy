package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate

class AndSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

    def "collect ruling types"() {
        expect:
        ((And) QueryTreeBuilder.buildTree(and, disambiguate)).collectRulingTypes() == result

        where:
        and                                     | result
        'type:T1 p1:v1'                         | ["T1"]
        'type:(T1 T2) p1:v1'                    | ["T1", "T2"]
        'p1:v1 p2:v2'                           | []
        'type:(T1 OR T2) p1:v1'                 | ["T1", "T2"]
        '(type:T1 OR p1:v1) (type:T2 OR p2:v2)' | []
    }

    def "invert"() {
        expect:
        ((And) QueryTreeBuilder.buildTree(and, disambiguate)).getInverse().toString() == result

        where:
        and                   | result
        'p1:v1 p2:v2'         | 'NOT p1:v1 OR NOT p2:v2'
        'NOT p1:v1 p2:v2'     | 'p1:v1 OR NOT p2:v2'
        'NOT p1:v1 NOT p2:v2' | 'p1:v1 OR p2:v2'
        'p1:v1 p2>v2'         | 'NOT p1:v1 OR p2<=v2'
    }
}
