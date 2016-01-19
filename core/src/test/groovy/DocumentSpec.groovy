package whelk

import spock.lang.Specification

class DocumentSpec extends Specification {

    static uri = "https://id.kb.se/marc/characterCoding"
    static descriptions = [
            "entry": [
                "@id": "https://id.kb.se/marc/characterCoding",
                "@type": "ObjectProperty",
                "sameAs": [
                    ["@id": "https://libris.kb.se/fnrblrghr1234567"],
                ],
                "rangeIncludes": [
                    ["@id": "https://id.kb.se/marc/AuthorityCharacterCodingType"],
                    ["@id": "https://id.kb.se/marc/CharacterCodingType"],
                    ["@id": "https://id.kb.se/marc/HoldingCharacterCodingType"]
                ]
            ],
            "quoted": [
                [
                    "@graph": [
                        "@id": "https://id.kb.se/marc/AuthorityCharacterCodingType"
                    ]
                ],
                [
                    "@graph": [
                        "@id": "https://id.kb.se/marc/CharacterCodingType"
                    ]
                ],
                [
                    "@graph": [
                        "@id": "https://id.kb.se/marc/HoldingCharacterCodingType"
                    ]
                ]
            ]
        ]

    static identifiers = ["https://id.kb.se/marc/characterCoding",
                          "https://libris.kb.se/fnrblrghr1234567"]

    static quoted = ["https://id.kb.se/marc/AuthorityCharacterCodingType",
                     "https://id.kb.se/marc/CharacterCodingType",
                     "https://id.kb.se/marc/HoldingCharacterCodingType"]

    static getDocVariants() {
        return [
            toDoc(uri, [descriptions: descriptions]),
            toDoc(uri, [(Document.GRAPH_KEY): [descriptions.entry] + descriptions.quoted]),
        ]
    }

    static toDoc(uri, data) {
        new Document(uri, data).withContentType("application/ld+json")
    }

    def "should find identifiers"() {
        expect:
        doc.getIdentifiers().sort() == identifiers
        where:
        doc << docVariants
    }

    def "should find quoted"() {
        expect:
        doc.getQuoted()*.get(Document.GRAPH_KEY)*.get(JsonLd.ID_KEY) == quoted
        where:
        doc << docVariants
    }

}
