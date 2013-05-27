package se.kb.libris.whelks

import spock.lang.Specification

class WhelkSpec extends Specification {

    def whelk = new StandardWhelk("bib")

    def "should store documents"() {
        given:
        def doc = Mock(Document)
        doc.getIdentifier() >> new URI("/auth/123")
        when:
        whelk.add(doc)
        then:
        def e = thrown(se.kb.libris.whelks.exception.WhelkRuntimeException)
        e.message =~ /does not belong/
    }

}
