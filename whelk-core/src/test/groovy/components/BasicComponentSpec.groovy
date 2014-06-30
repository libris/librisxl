package se.kb.libris.whelks.component

import se.kb.libris.whelks.Document
import se.kb.libris.whelks.plugin.FormatConverter
import se.kb.libris.whelks.plugin.LinkExpander

import spock.lang.*


class BasicComponentSpec extends Specification {

    def JSON_CONTENT_TYPE = "application/json"

    def component = new DummyBasicComponent()
    def docs = [new Document().withContentType(JSON_CONTENT_TYPE)]

    def "should prepare docs with formatconverter"() {
        given:
        def fcMock = Mock(FormatConverter)
        component.formatConverters[JSON_CONTENT_TYPE] = fcMock
        when:
        component.prepareDocs(docs, JSON_CONTENT_TYPE)
        then:
        1 * fcMock.convert(!null)
    }

    def "should prepare docs with formatconverter and linkexpander"() {
        given:
        def fcMock = Mock(FormatConverter)
        1 * fcMock.convert(!null) >> { Document doc -> doc }
        component.formatConverters[JSON_CONTENT_TYPE] = fcMock
        and:
        def leMock = Mock(LinkExpander)
        1 * leMock.valid(!null) >> true
        component.linkExpanders << leMock
        when:
        component.prepareDocs(docs, JSON_CONTENT_TYPE)
        then:
        1 * leMock.expand(!null)

    }

    class DummyBasicComponent extends BasicComponent {
        void componentBootstrap(String str) { }
        void batchLoad(List<Document> docs) { }
        Document get(URI uri) { }
        void remove(URI uri) { }
    }

}
