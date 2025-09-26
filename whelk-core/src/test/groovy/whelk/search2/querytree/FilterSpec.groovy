package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class FilterSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "expand"() {
        given:
        def filter = QueryTreeBuilder.buildTree("excludeA", disambiguate)

        expect:
        filter.expand(jsonLd, []).toString() == "NOT p1:A"
    }

    def "invert"() {
        given:
        def filter = QueryTreeBuilder.buildTree("excludeA", disambiguate)

        expect:
        filter.getInverse().toString() == "NOT excludeA"
    }

    def "invert 2"() {
        given:
        def filter = QueryTreeBuilder.buildTree("includeA", disambiguate)

        expect:
        filter.getInverse().toString() == "excludeA"
    }

    def "expand negated"() {
        given:
        def filter = QueryTreeBuilder.buildTree("NOT excludeA", disambiguate)

        expect:
        filter.expand(jsonLd, []) == null
    }

    def "invert negated"() {
        given:
        def filter = QueryTreeBuilder.buildTree("NOT excludeA", disambiguate)

        expect:
        filter.getInverse().toString() == "excludeA"
    }

    def "invert negated 2"() {
        given:
        def filter = QueryTreeBuilder.buildTree("NOT includeA", disambiguate)

        expect:
        filter.getInverse().toString() == "NOT excludeA"
    }
}
