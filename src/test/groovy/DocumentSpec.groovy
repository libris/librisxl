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
                    ["@id": "ftp://ppftpuser:welcome@@ftp01.penguingroup.com/Booksellers and Media/Covers/2008@03322009@0332New@0332Covers/9780399168123.jpg"],
                    ["@id": "http://kulturarvsdata.se/raa/samla/html/6743 |z Fritt tillgänglig via Riksantikvarieämbetets webbplats"],
                    ["@id": "http://images.contentreserve.com/ImageType-10\n" +
                                "0/1559-1/{C07B6BAA-9D1B-4FC5-B724-42628F4BBF59}Img100.jpg"]
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

    static identifiers = ["ftp://ppftpuser:welcome@@ftp01.penguingroup.com/Booksellers%20and%20Media/Covers/2008@03322009@0332New@0332Covers/9780399168123.jpg",
                          "http://images.contentreserve.com/ImageType-100/1559-1/{C07B6BAA-9D1B-4FC5-B724-42628F4BBF59}Img100.jpg",
                          "http://kulturarvsdata.se/raa/samla/html/6743",
                          "https://id.kb.se/marc/characterCoding",
                          "https://libris.kb.se/fnrblrghr1234567"
            ]



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
