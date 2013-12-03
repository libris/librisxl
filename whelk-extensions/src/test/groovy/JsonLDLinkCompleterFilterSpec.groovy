package se.kb.libris.whelks.plugin

import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log
import se.kb.libris.whelks.Link
import se.kb.libris.whelks.component.ElasticJsonMapper
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Whelk
import se.kb.libris.whelks.StandardWhelk
import se.kb.libris.whelks.component.FlatDiskStorage

@Log
class JsonLDLinkCompleterFilterSpec extends Specification {

    private mapper = new ElasticJsonMapper()
    def bibDoc, doc, whelk, filter, docMap

    def "convert should insert auth link into bib jsonld"() {
        given:
        whelk = getInitializedWhelk()
        filter = new JsonLDLinkCompleterFilter()
        filter.setWhelk(whelk)
        def doclinks = [
            "/auth/94541",
            "/auth/139860",
            "/auth/191503",
            "/auth/140482",
            "/auth/349968",
            "/auth/345526"
        ].collect {
            new Link(new URI(it), "auth")
        }
        bibDoc = makeDoc ([
            "@id": "/bib/12661",
            "@type": "Record",
            "about": [
                "@id": "/resource/bib/12661",
                "@type": "Book",
                "instanceOf": [
                    "@type": "Book",
                    "attributedTo": [
                        "@type" : "Person",
                        "controlledLabel": "Strindberg, August, 1849-1912"
                    ],
                    "influencedBy":  [
                        [
                            "@type" : "Person",
                            "controlledLabel" : "Jansson, Tove, 1914-2001"
                        ]
                    ],
                    "subject": [
                        [
                            "@type" : "Concept",
                            "sameAs": ["@id" : "/topic/sao/Arkiv"],
                            "prefLabel" : "Arkiv"
                        ],
                        [
                            "@type" : "Concept",
                            "broader" : [
                                [
                                    "@type" : "Concept",
                                    "@id" : "/topic/sao/Arkiv",
                                ],
                                [
                                    "@type" : "Concept",
                                    "sameAs": ["@id" : "/topic/sao/Arkiv"]
                                ],
                                [
                                    "@type" : "Concept",
                                    "prefLabel" : "Allegorier"
                                ]
                            ]
                        ],
                        [
                            "@type": "Work",
                            "uniformTitle": "Metamorphoses",
                            "attributedTo": [
                                [
                                    "@type": "Person",
                                    "controlledLabel": "Ovidius Naso, Publius, 43-"
                                ]
                            ]
                        ]
                    ],
                    "class": [
                        [
                            "@type" : "Concept",
                            "prefLabel": "Barnpsykologi",
                            "sameAs": ["@id": "/topic/sao/Barnpsykologi"]
                        ],
                        [
                            "@type" : "Concept",
                            "prefLabel" : "Arkiv"
                        ]
                    ]
                ]
            ]
        ], doclinks)

        when:
        doc = filter.doFilter(bibDoc)
        docMap = mapper.readValue(doc.dataAsString, Map)
        then:
        def work = docMap.about.instanceOf
        work.attributedTo."@id" == "/resource/auth/94541"
        work.influencedBy[0]."@id" == "/resource/auth/191503"
        work.subject[0]."@id" == "/resource/auth/139860"
        work.subject[1].broader[0]."@id" == "/resource/auth/139860"
        work.subject[1].broader[0].sameAs."@id" == "/topic/sao/Arkiv"
        work.subject[1].broader[1]."@id" == "/resource/auth/139860"
        work.subject[1].broader[2]."@id" == "/resource/auth/349968"
        work.subject[2]."@id" == "/resource/auth/345526"
        work["class"][0]."@id" == "/resource/auth/140482"
        work["class"][1]."@id" == "/resource/auth/139860"

    }

    def makeDoc(data, links) {
        def doc = new Document()
                .withIdentifier("http://libris.kb.se/bib/12661")
                .withData(mapper.writeValueAsString(data))
        links.each {
            doc.withLink(it.identifier.toString(), it.type)
        }
        return doc
    }

    Whelk getInitializedWhelk() {
          Map settings = ["storageDir": "../work/storage/flat/main", "contentType": "application/ld+json"]
          Whelk whelk = new StandardWhelk("libris")
          whelk.addPlugin(new FlatDiskStorage(settings))
          return whelk
    }

}
