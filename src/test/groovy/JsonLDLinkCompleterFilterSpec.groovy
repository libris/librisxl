package whelk.converter

import org.codehaus.jackson.map.ObjectMapper
import spock.lang.Specification
import spock.lang.Ignore
import whelk.Document
import whelk.Whelk

class JsonLDLinkCompleterFilterSpec extends Specification {

    private mapper = new ObjectMapper()

    def authData = [
        "/auth/94541": [about: ["@id": "/resource/auth/94541", "@type": "Person",
            "familyName":"Strindberg","givenName":"August","birthYear":"1849","deathYear":"1912"
        ]],
        "/auth/139860": [about: ["@id": "/resource/auth/139860", "@type":"Concept",
            "inScheme":["@type":"ConceptScheme","@id":"/topic/sao","notation":"sao"],
            "prefLabel":"Arkiv"],
            "sameAs":[["@id":"/topic/sao/Arkiv"]]],
        "/auth/191503": [about: ["@id": "/resource/auth/191503", "@type": "Person",
            "familyName":"Jansson","givenName":"Tove","birthYear":"1914","deathYear":"2001"
        ]],
        "/auth/140482": [about: ["@id": "/resource/auth/140482", "@type": "Concept",
            "inScheme":["@type":"ConceptScheme","@id":"/topic/sao","notation":"sao"],
            "prefLabel":"Barnpsykologi",
            "sameAs":[["@id":"/topic/sao/Barnpsykologi"]]
        ]],
        "/auth/349968": [about: ["@id": "/resource/auth/349968", "@type": "Concept",
            "inScheme":["@type":"ConceptScheme","@id":"/topic/saogf","notation":"saogf"],
            "prefLabel":"Allegorier",
            "sameAs":[["@id":"/topic/saogf/Allegorier"]]
        ]],
        "/auth/345526": [about: ["@id": "/resource/auth/345526", "@type":"ConceptualWork",
            "uniformTitle":"Metamorphoses",
            "attributedTo":["@type":"Person",
                "@id": "/person/ovidiusnaso",
                "familyName":"Ovidius Naso", "givenName":"Publius", "birthYear":"43"]
        ]]
    ]

    // FIXME Not used yet
    @Ignore
    def "convert should insert auth link into bib jsonld"() {
        given:
        def whelk = Mock(Whelk)
        whelk.get(_ as URI) >> { args -> def id = args[0].toString(); makeDoc(id, authData[id]) }
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
        def bibDoc = makeDoc("http://libris.kb.se/bib/12661", [
            "@id": "/bib/12661",
            "@type": "Record",
            "about": [
                "@id": "/resource/bib/12661",
                "@type": ["Text"],
                "attributedTo": [
                    "@type" : "Person",
                    "familyName": "Strindberg",
                    "givenName": "August",
                    "birthYear": "1849",
                    "deathYear": "1912"
                ],
                "influencedBy":  [
                    [
                        "@type" : "Person",
                        "familyName": "Jansson",
                        "givenName": "Tove",
                        "birthYear": "1914",
                        "deathYear": "2001"
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
                                "prefLabel" : "Arkiv"
                            ],
                            [
                                "@type" : "Concept",
                                "sameAs": ["@id" : "/topic/sao/Arkiv"],
                                "prefLabel" : "Arkiv"
                            ],
                            [
                                "@type" : "Concept",
                                "prefLabel" : "Allegorier"
                            ]
                        ]
                    ],
                    [
                        "@type": "ConceptualWork",
                        "uniformTitle": "Metamorphoses",
                        "attributedTo": [
                            "@type": "Person",
                            "familyName": "Ovidius Naso",
                            "givenName": "Publius",
                            "birthYear": "43"
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
        ], doclinks)

        when:
        def doc = filter.doFilter(bibDoc)
        def docMap = mapper.readValue(doc.dataAsString, Map)
        then:
        def resource = docMap.about
        resource.attributedTo."@id" == "/resource/auth/94541"
        resource.influencedBy[0]."@id" == "/resource/auth/191503"
        resource.subject[0]."@id" == "/resource/auth/139860"
        resource.subject[1].broader[0]?."@id" == "/resource/auth/139860"
        resource.subject[1].broader[0]?.sameAs?."@id" == "/topic/sao/Arkiv"
        resource.subject[1].broader[1]?."@id" == "/resource/auth/139860"
        resource.subject[1].broader[2]?."@id" == "/resource/auth/349968"
        resource.subject[2]."@id" == "/resource/auth/345526"
        resource.subject[2].attributedTo."@id" == "/person/ovidiusnaso"
        resource["class"][0]."@id" == "/resource/auth/140482"
        resource["class"][1]."@id" == "/resource/auth/139860"
    }

    def makeDoc(path, data, links=[]) {
        def doc = new Document()
                .withIdentifier(path)
                .withData(mapper.writeValueAsString(data))
                .withContentType("application/ld+json")
        links.each {
            doc.meta.get("oaipmhSetSpecs", []).add(it)
        }
        return doc
    }

}
