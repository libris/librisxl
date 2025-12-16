package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class SelectorSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "expand"() {
        given:
        Selector p = ((Condition) QueryTreeBuilder.buildTree("$_p:v", disambiguate)).selector()

        expect:
        p.expand(jsonLd).toString() == result

        where:
        _p      | result
        "p1"    | "p1"
        "p5"    | "meta.p5"
        "p6"    | "p3.p4"
        "p6.p1" | "p3.p4.p1"
    }

    def "get alternative paths for integral relations"() {
        given:
        Selector p = ((Condition) QueryTreeBuilder.buildTree("$_p:v", disambiguate)).selector()

        expect:
        p.getAltSelectors(jsonLd, types).collect { it.expand(jsonLd).toString() } == result

        where:
        _p               | types        | result
        "p1"             | []           | ["p1"]
        "p1"             | ["T1"]       | ["instanceOf.p1", "p1"]
        "p1"             | ["T2"]       | ["hasInstance.p1", "p1"]
        "p1"             | ["T1", "T2"] | ["hasInstance.p1", "instanceOf.p1", "p1"]
        "p1"             | ["T3"]       | ["p1"]
        "p7"             | ["T1"]       | ["p7"]
        "p7"             | ["T2"]       | ["hasInstance.p7"]
        "p7"             | ["T1", "T2"] | ["hasInstance.p7", "p7"]
        "p7"             | ["T3"]       | ["p7"]
        "p8"             | ["T1"]       | ["instanceOf.p8"]
        "p8"             | ["T2"]       | ["p8"]
        "p8"             | ["T1", "T2"] | ["instanceOf.p8", "p8"]
        "p8"             | ["T3"]       | ["p8"]
        "p9"             | ["T1"]       | ["p9"]
        "p9"             | ["T2"]       | ["p9"]
        "p9"             | ["T1", "T2"] | ["p9"]
        "p9"             | ["T3"]       | ["p9"]
        "hasInstance.p7" | ["T2"]       | ["hasInstance.p7"]
        "instanceOf.p8"  | ["T2"]       | ["p8"]
        "type"           | ["T2"]       | ["rdf:type"]
        "hasInstance.p7" | ["T1"]       | ["p7"]
        "instanceOf.p8"  | ["T1"]       | ["instanceOf.p8"]
        "type"           | ["T1"]       | ["rdf:type"]
        "p7.p14"         | ["T2"]       | ["hasInstance.p7.hasComponent.p14", "hasInstance.p7.p14"]
    }
}
