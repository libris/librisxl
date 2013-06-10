package se.kb.libris.whelks

import spock.lang.Specification

class DocumentSpec extends Specification {

    def "should copy document"() {
        given:
        def doc = new Document().withIdentifier("/bib/123").withData("foo").withContentType("text/plain")
        when:
        def newdoc = new Document(doc)
        doc.identifier = new URI("/bib/234")
        doc.version = "2"
        then:
        newdoc.identifier == new URI("/bib/123")
        newdoc.dataAsString == doc.dataAsString
        newdoc.contentType == doc.contentType
        newdoc.version == "1"
        newdoc.size == doc.size
    }

    def "should extract map from significant fields"() {
        given:
        def doc = new Document().withIdentifier("/libris/2345").withData("foo").withContentType("application/json").withLink("/libris/1234")
        when:
        def map = doc.toMap()
        then:
        map['identifier'] == "/libris/2345"
        map['data'] == ("foo" as byte[])
        map['links'] == [["type":"", "identifier":"/libris/1234"]]
    }
}
