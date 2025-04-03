package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd

import static DummyNodes.pathV1

class ActiveBoolFilterSpec extends Specification {
    def "expand"() {
        given:
        def jsonLd = new JsonLd([:], [:], [:])
        def excludeA = new ActiveBoolFilter('excludeA', pathV1, [:])
        def includeA = new ActiveBoolFilter('includeA', new InactiveBoolFilter('excludeA'), [:])

        expect:
        excludeA.expand(jsonLd, []) == pathV1
        includeA.expand(jsonLd, []) == null
    }
}
