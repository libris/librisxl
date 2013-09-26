package se.kb.libris.whelks.plugin

import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log
import se.kb.libris.whelks.Link
import se.kb.libris.whelks.component.ElasticJsonMapper
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.StandardWhelk
import se.kb.libris.whelks.component.DiskStorage

@Log
class JsonLDLinkEnhancerFormatConverterSpec extends Specification {

    private mapper = new ElasticJsonMapper()
    def bibDoc, doc, whelk, converter

    def "convert should insert auth link into bib jsonld"() {
        given:
        whelk = getInitializedWhelk()
        converter = new JsonLDLinkEnhancerFormatConverter()
        converter.setWhelk(whelk)
        bibDoc = makeDoc ([
                "@id": "/bib/12661",
                "@type": "Record",
                "about": [
                        "@id": "/resource/bib/12661",
                        "@type": "Book",
                        "instanceOf": [
                                "@type": "Book",
                                "creator": [
                                        [
                                         "@type" : "Person",
                                         "controlledLabel": "Strindberg, August, 1849-1912"
                                        ]
                                ]
                        ]
                ]
        ], new Link(new URI("/auth/94541"), "auth"))

        when:
        doc = converter.doConvert(bibDoc)
        then:
        mapper.readValue(doc.dataAsString, Map).about.instanceOf.creator[0]."@id" == "/resource/auth/94541"

    }

    def makeDoc(data, link) {
        return new Document()
                .withIdentifier("http://libris.kb.se/bib/12661")
                .withData(mapper.writeValueAsString(data))
                .withLink(link)
    }

    Whelk getInitializedWhelk() {
          Map settings = ["storageDir": "../work/storage/jsonld", "contentType": "application/ld+json"]
          Whelk whelk = new StandardWhelk("libris")
          whelk.addPlugin(new DiskStorage(settings))
          return whelk
    }

}
