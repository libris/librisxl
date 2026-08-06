package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class SelectorSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "get expanded path"() {
        given:
        Selector p = ((Condition) QueryTreeBuilder.buildTree("$_p:v", disambiguate)).selector()

        expect:
        p.path().collect { it.toString() } == result

        where:
        _p      | result
        "p1"    | ["p1"]
        "p6"    | ["p3", "p4"]
        "p6.p1" | ["p3", "p4", "p1"]
    }
}
