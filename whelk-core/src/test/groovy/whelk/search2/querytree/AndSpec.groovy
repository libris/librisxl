package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class AndSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

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
