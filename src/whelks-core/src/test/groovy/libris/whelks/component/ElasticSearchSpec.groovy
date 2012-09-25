package se.kb.libris.whelks.component

import spock.lang.*

class ElasticSearchSpec extends Specification {

    def es = new ElasticSearch() {}

    @Unroll
    def "translate identifier"() {
        expect:
        es.translateIdentifier(new URI(uri)) == id
        where:
        uri                             | id
        "http://kb.se/record/bib"       | "bib"
        "http://kb.se/record/bib/123"   | "bib::123"
    }

}
