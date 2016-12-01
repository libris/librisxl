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
                                 "extra": ["@id": "/externalBaz"],
                                 "quux": ["@id": "/quux"],
                                 "oneMore": ["@id": "/externalBaz"]],
                                ["@id": "/quux",
                                 "someValue": 1],
                                ["someOtherValue": 2],
                                "A lonely string"]]

        Map extra = ["/externalBar": ["@id": "/externalBar",
                                      "@type": "Work",
                                      "hasTitle": "aTitle",
                                      "language": "danska",
                                      "someThirdValue": 3],
                     "/externalBaz": ["@id": "/externalBaz",
                                      "@type": "ProvisionActivity",
                                      "date": "aDate",
                                      "place": "aPlace",
                                      "someFourthValue": 4]]

        Map expected = ["@graph": [["@id": "/foo",
                                    "bar": ["@id": "/externalBar"],
                                    "extra": ["@id": "/externalBaz"],
                                    "quux": ["@id": "/quux"],
                                    "oneMore": ["@id": "/externalBaz"]],
                                   ["@id": "/quux",
                                    "someValue": 1],
                                   ["someOtherValue": 2],
                                   "A lonely string",
                                   ["@graph": ["@id": "/externalBar",
                                               "@type": "Work",
                                               "hasTitle": "aTitle",
                                               "language": "danska"]],
                                   ["@graph": ["@id": "/externalBaz",
                                               "@type": "ProvisionActivity",
                                               "date": "aDate",
                                               "place": "aPlace"]]]]

        Map displayData = ["lensGroups":
                                   ["chips":
                                            ["lenses":
                                                     ["Work": ["showProperties": ["hasTitle",
                                                                                  "contribution",
                                                                                  "language"]],
                                                      "ProvisionActivity": ["showProperties": ["date",
                                                                                               "agent",
                                                                                               "place"]]]],
                                    "cards":
                                            ["lenses":
                                                     ["Instance":
                                                              ["showProperties": ["mediaType",
                                                                                  "hasTitle",
                                                                                  "instanceOf"]]]]]]


        Document doc = new Document(input)
        when:
        doc.embellish(extra, displayData)
        then:
        assert doc.data == expected
    }

    def "should convert to cards and chips"() {
        given:
        Map input = ["@type": "Instance",
                     "mediaType": "foobar",
                     "instanceOf": ["@type": "Work",
                                    "contribution": ["@type": "Text",
                                                     "foo": ["mediaType": "bar"]],
                                    "hasTitle": ["@type": "ProvisionActivity",
                                                 "date": "2000-01-01",
                                                 "noValidKey": "shouldBeRemoved",
                                                 "@id": "foo"]],
                     "@aKey": "external-foobar",
                     "hasTitle": ["value1", "value2", "value3", ["someKey": "theValue",
                                                                 "@type": "Work"]],
                     "foo": "bar"]

        Map displayData = ["lensGroups":
                                   ["chips":
                                            ["lenses":
                                                     ["Work": ["showProperties": ["hasTitle",
                                                                                  "contribution",
                                                                                  "language"]],
                                                      "ProvisionActivity": ["showProperties": ["date",
                                                                                               "agent",
                                                                                               "place"]]]],
                                    "cards":
                                            ["lenses":
                                                     ["Instance":
                                                              ["showProperties": ["mediaType",
                                                                                  "hasTitle",
                                                                                  "instanceOf"]]]]]]


        Map output = ["@type": "Instance",
                      "mediaType": "foobar",
                      "instanceOf": ["@type": "Work",
                                     "contribution": ["@type": "Text",
                                                      "foo": ["mediaType": "bar"]],
                                     "hasTitle": ["@type": "ProvisionActivity",
                                                  "date": "2000-01-01",
                                                  "@id": "foo"]],
                      "@aKey": "external-foobar",
                      "hasTitle": ["value1", "value2", "value3", ["@type": "Work"]]]


        Document doc = new Document(input)
        doc.toCard(displayData)
        expect:
        assert doc.data == output
    }

}
