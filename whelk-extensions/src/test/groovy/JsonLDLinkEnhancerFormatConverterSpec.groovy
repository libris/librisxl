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
    def bibDoc, doc, whelk, converter, docMap

    def "convert should insert auth link into bib jsonld"() {
        given:
        whelk = getInitializedWhelk()
        converter = new JsonLDLinkEnhancerFormatConverter()
        converter.setWhelk(whelk)
        def doclinks = new HashSet<Link>()
        doclinks.add(new Link(new URI("/auth/94541"), "auth"))
        doclinks.add(new Link(new URI("/auth/139860"), "auth"))
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
                                ],
                                "subject": [
                                        [
                                            "@type" : "Concept",
                                            "broader" : [
                                                    [
                                                        "@id" : "/topic/sao/Arkiv",
                                                        "prefLabel" : "Arkiv"
                                                    ]
                                            ]
                                        ]
                                ]
                        ]
                ]
        ], doclinks)

        when:
        doc = converter.doConvert(bibDoc)
        docMap = mapper.readValue(doc.dataAsString, Map)
        then:
        docMap.about.instanceOf.creator[0]."@id" == "/resource/auth/94541"
        docMap.about.instanceOf.subject[0].broader[0]."@id" == "/resource/auth/139860"

    }

    def makeDoc(data, links) {
        def doc = new Document()
                .withIdentifier("http://libris.kb.se/bib/12661")
                .withData(mapper.writeValueAsString(data))
        doc.links = links
        return doc
    }

    Whelk getInitializedWhelk() {
          Map settings = ["storageDir": "../work/storage/jsonld", "contentType": "application/ld+json"]
          Whelk whelk = new StandardWhelk("libris")
          whelk.addPlugin(new DiskStorage(settings))
          return whelk
    }

}
