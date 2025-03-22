package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class InactiveFilterSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "expand"() {
        given:
        InactiveFilter filter = (InactiveFilter) QueryTreeBuilder.buildTree("NOT excludeA", disambiguate)

        expect:
        filter.expand(jsonLd, [], x -> []) == null
    }

    def "invert"() {
        given:
        def filter = QueryTreeBuilder.buildTree("NOT excludeA", disambiguate)

        expect:
        filter.getInverse().toString() == "excludeA"
    }

    def "invert 2"() {
        given:
        def filter = QueryTreeBuilder.buildTree("NOT includeA", disambiguate)

        expect:
        filter.getInverse().toString() == "includeA"
    }
}
