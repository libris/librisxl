package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class NotSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "expand filter alias"() {
        given:
        def filter = QueryTreeBuilder.buildTree("NOT excludeA", disambiguate)

        expect:
        filter.expand(jsonLd, []) == ExpandedNode.newEmpty()
    }

    def "implies"() {
        given:
        Node aTree = QueryTreeBuilder.buildTree(a, disambiguate)
        Node bTree = QueryTreeBuilder.buildTree(b, disambiguate)

        expect:
        aTree.implies(bTree, jsonLd) == result

        where:
        a                   | b                      | result
        "NOT excludeA"      | "NOT excludeA"         | true
        "NOT (p1:X p3:Y)"   | "NOT XY"               | false
        "NOT XY"            | "NOT (p1:X p3:Y)"      | false
        "NOT p1:v1"         | "NOT (p1:v1 p2:v2)"    | true
        "NOT (p1:v1 p2:v2)" | "NOT (p1:v1 p2:v2)"    | true
        "NOT p1:v1"         | "NOT (p1:v1 OR p2:v2)" | false
    }
}
