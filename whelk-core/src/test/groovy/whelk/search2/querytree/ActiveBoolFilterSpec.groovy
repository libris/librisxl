package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate
import whelk.search2.OutsetType

import static DummyNodes.pathV1

class ActiveBoolFilterSpec extends Specification {
    def "expand"() {
        given:
        def disambiguate = new Disambiguate(['vocab': [:]])
        def outset = OutsetType.RESOURCE
        def excludeA = new ActiveBoolFilter('excludeA', pathV1, [:])
        def includeA = new ActiveBoolFilter('includeA', new InactiveBoolFilter('excludeA'), [:])

        expect:
        excludeA.expand(disambiguate, outset) == pathV1
        includeA.expand(disambiguate, outset) == null
    }
}
