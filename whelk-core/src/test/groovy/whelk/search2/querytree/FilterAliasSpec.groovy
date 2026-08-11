package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate

class FilterAliasSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

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
}
