package se.kb.libris.whelks.plugin

import spock.lang.Specification
import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log
import se.kb.libris.whelks.component.ElasticJsonMapper
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Whelk

@Log
class JsonLDLinkCompleterFilterSpec extends Specification {

    private mapper = new ElasticJsonMapper()

    def "convert should insert auth link into bib jsonld"() {
        given:
        def whelk = Mock(Whelk) // TODO: let mock fetch up example documents
        def filter = new JsonLDLinkCompleterFilter()
        filter.setWhelk(whelk)
        def doclinks = [
            "authority:94541",
            "authority:139860",
            "authority:191503",
            "authority:140482",
            "authority:349968",
            "authority:345526"
        ]
        def bibDoc = makeDoc ([
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
        def doc = filter.doFilter(bibDoc)
        def docMap = mapper.readValue(doc.dataAsString, Map)
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
                .withContentType("application/ld+json")
        links.each {
            doc.meta.get("oaipmhSetSpecs", []).add(it)
        }
        return doc
    }

}
