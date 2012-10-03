package se.kb.libris.whelks.basic

import spock.lang.Specification

class BasicDocumentSpec extends Specification {

    def "should copy document"() {
        given:
        def doc = new BasicDocument().withIdentifier("/bib/123").withData("foo").withContentType("text/plain")
        when:
        def newdoc = new BasicDocument(doc)
        doc.identifier = new URI("/bib/234")
        doc.version = "2"
        then:
        newdoc.identifier == new URI("/bib/123")
        newdoc.dataAsString == doc.dataAsString
        newdoc.contentType == doc.contentType
        newdoc.version == "1"
        newdoc.size == doc.size
    }
}
