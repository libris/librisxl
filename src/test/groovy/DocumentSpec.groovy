package whelk

import spock.lang.Specification
import spock.lang.Unroll

class DocumentSpec extends Specification {

    static examples = [

        [
            data: [
                [
                    "@id": "http://example.org/record",
                    "@type": "Record",
                    "sameAs": [
                        ["@id": "https://libris.kb.se/fnrblrghr1234567"],
                    ],
                    "mainEntity": ["@id": "http://example.org/thing"]
                ],
                [
                    "@id": "http://example.org/thing",
                    "@type": "Thing",
                    "sameAs": [
                        ["@id": "https://libris.kb.se/fnrblrghr1234567#it"],
                        //["@id": "ftp://ppftpuser:welcome@@ftp01.penguingroup.com/Booksellers and Media/Covers/2008@03322009@0332New@0332Covers/9780399168123.jpg"],
                        //["@id": "http://kulturarvsdata.se/raa/samla/html/6743 |z Fritt tillgänglig via Riksantikvarieämbetets webbplats"],
                        //["@id": "http://images.contentreserve.com/ImageType-10\n" +
                        //            '0/1559-1/{C07B6BAA-9D1B-4FC5-B724-42628F4BBF59}Img100.jpg']
                    ]
                ],
                [
                    "@id": "http://dbpedia.org/data/Quotation",
                    "@graph": [
                        "@id": "http://dbpedia.org/resource/Quotation",
                        "@type": "Thing"
                    ]
                ]
            ],
            recordIds: [
                //"ftp://ppftpuser:welcome@@ftp01.penguingroup.com/Booksellers%20and%20Media/Covers/2008@03322009@0332New@0332Covers/9780399168123.jpg",
                //"http://images.contentreserve.com/ImageType-100/1559-1/{C07B6BAA-9D1B-4FC5-B724-42628F4BBF59}Img100.jpg",
                //"http://kulturarvsdata.se/raa/samla/html/6743",
                "http://example.org/record",
                "https://libris.kb.se/fnrblrghr1234567"
            ],
            thingIds: [
                "http://example.org/thing",
                "https://libris.kb.se/fnrblrghr1234567#it"
            ]
        ],

        [
            data: [
                ["@id": "http://example.org/record", "@type": "Record"],
            ],
            recordIds: ["http://example.org/record"],
            thingIds: []
        ]

    ]

    @Unroll
    def "should find identifiers"() {
        given:
        def doc = new Document(['@graph': example.data])
        expect:
        doc.getRecordIdentifiers().sort() == example.recordIds
        doc.getThingIdentifiers().sort() == example.thingIds
        where:
        example << examples
    }

    def "should add identifier if missing"() {
        given:
        String id = '/foo'
        Document doc = new Document(['@graph': []])
        Document expected = new Document(['@graph': [['@id': id]]])
        doc.addRecordIdentifier(id)
        expect:
        assert doc.data == expected.data
    }

    def "should not add identifier if already added"() {
        given:
        String id = '/foo'
        Document doc = new Document(['@graph': [['@id': id]]])
        Document expected = new Document(['@graph': [['@id': id]]])
        doc.addRecordIdentifier(id)
        expect:
        assert doc.data == expected.data
    }

    def "should add sameAs identifier"() {
        given:
        String id = '/foo'
        String altId = '/bar'
        Document doc = new Document(['@graph': [['@id': id]]])
        Document expected = new Document(['@graph': [['@id': id,
                                                      'sameAs': [['@id': altId]]
                                                      ]]])
        doc.addRecordIdentifier(altId)
        expect:
        assert doc.data == expected.data
    }

    def "should not add sameAs identifier if already added"() {
        given:
        String id = '/foo'
        String altId = '/bar'
        Document doc = new Document(['@graph': [['@id': id,
                                                 'sameAs': [['@id': altId]]
                                                 ]]])
        Document expected = new Document(['@graph': [['@id': id,
                                                      'sameAs': [['@id': altId]]
                                                      ]]])
        doc.addRecordIdentifier(altId)
        expect:
        assert doc.data == expected.data
    }

    def "should not allow adding null identifier"() {
        when:
        String id = null
        Document doc = new Document(['@graph': []])
        doc.addRecordIdentifier(id)
        then:
        thrown NullPointerException
    }

    def "should get external refs"() {
        given:
        Map input = ["@graph": [["@id": "/foo",
                                 "bar": ["@id": "/externalBar"],
                                 "extra": ["baz": ["@id": "/externalBaz"]],
                                 "quux": ["@id": "/quux"]],
                                ["@id": "/quux",
                                 "someValue": 1],
                                ["someOtherValue": 2]]]
        Document doc = new Document(input)
        List expected = ["/externalBar", "/externalBaz"]
        expect:
        assert doc.getExternalRefs() == expected

    }

    def "should embellish document"() {
        given:
        Map input = ["@graph": [["@id": "/foo",
                                 "bar": ["@id": "/externalBar"],
                                 "extra": ["baz": "/externalBaz"],
                                 "quux": ["@id": "/quux"]],
                                ["@id": "/quux",
                                 "someValue": 1],
                                ["someOtherValue": 2],
                                "A lonely string"]]

        Map extra = ["/externalBar": ["@id": "/externalBar",
                                      "someThirdValue": 3],
                     "/externalBaz": ["@id": "/externalBaz",
                                      "someFourthValue": 4]]

        Map expected = ["@graph": [["@id": "/foo",
                                    "bar": ["@id": "/externalBar"],
                                    "extra": ["baz": "/externalBaz"],
                                    "quux": ["@id": "/quux"]],
                                   ["@id": "/quux",
                                    "someValue": 1],
                                   ["someOtherValue": 2],
                                   "A lonely string",
                                   ["@id": "/externalBar",
                                      "someThirdValue": 3],
                                   ["@id": "/externalBaz",
                                      "someFourthValue": 4]]]


        Document doc = new Document(input)
        when:
        doc.embellish(extra)
        then:
        assert doc.data == expected
    }
}
