package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate

import static DummyNodes.pathV1
import static whelk.search2.Disambiguate.Rdfs.RESOURCE

class ActiveBoolFilterSpec extends Specification {
    def "expand"() {
        given:
        def disambiguate = new Disambiguate(['vocab': [:]])
        def outset = RESOURCE
        def excludeA = new ActiveBoolFilter('excludeA', pathV1, [:])
        def includeA = new ActiveBoolFilter('includeA', new InactiveBoolFilter('excludeA'), [:])

        expect:
        excludeA.expand(disambiguate, outset) == pathV1
        includeA.expand(disambiguate, outset) == null
    }
}
