package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class FilterAliasSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "expand"() {
        given:
        def alias = QueryTreeBuilder.buildTree("excludeA", disambiguate)

        expect:
        alias.expand(jsonLd, []).toString() == "NOT p1:A"
    }

    def "invert"() {
        given:
        def alias = QueryTreeBuilder.buildTree("excludeA", disambiguate)

        expect:
        alias.getInverse().toString() == "NOT excludeA"
    }

    def "invert 2"() {
        given:
        def alias = QueryTreeBuilder.buildTree("includeA", disambiguate)

        expect:
        alias.getInverse().toString() == "excludeA"
    }

    def "implies"() {
        expect:
        QueryTreeBuilder.buildTree(alias, disambiguate).implies(QueryTreeBuilder.buildTree(other, disambiguate), jsonLd) == result

        where:
        alias      | other            | result
        "excludeA" | "excludeA"       | true
        "excludeA" | "NOT p1:A"       | true
        "excludeA" | "NOT includeA"   | true
        "includeA" | "includeA"       | true
        "includeA" | "NOT excludeA"   | true
        "XY"       | "XY"             | true
        "XY"       | "p1:X p3:Y"      | true
        "XY"       | "p1:X"           | true
        "XY"       | "p3:Y"           | true
        "XY"       | "p1:X p3:Y p4:Z" | false
    }
}
