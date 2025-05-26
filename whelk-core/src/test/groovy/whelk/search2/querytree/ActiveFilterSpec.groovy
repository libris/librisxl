package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class ActiveFilterSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "expand"() {
        given:
        ActiveFilter filter = (ActiveFilter) QueryTreeBuilder.buildTree("excludeA", disambiguate)

        expect:
        filter.expand(jsonLd, []).toString() == "NOT p1:A"
    }

    def "invert"() {
        given:
        ActiveFilter filter = (ActiveFilter) QueryTreeBuilder.buildTree("excludeA", disambiguate)

        expect:
        filter.getInverse().toString() == "NOT excludeA"
    }

    def "invert 2"() {
        given:
        ActiveFilter filter = (ActiveFilter) QueryTreeBuilder.buildTree("includeA", disambiguate)

        expect:
        filter.getInverse().toString() == "excludeA"
    }
}
