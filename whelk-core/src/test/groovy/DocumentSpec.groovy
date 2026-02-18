package whelk

import spock.lang.Specification
import spock.lang.Unroll
import whelk.converter.marc.MarcFrameConverter

import java.sql.Timestamp

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

    JsonLd jsonld

    def setup() {
        Map contextData = ["@context":
            [
                    "@vocab":"https://id.kb.se/vocab/",
                    "iAmASet": ["@container": "@set"],
                    "iAmAList": ["@container": "@list"],
            ]
        ]
        Map displayData = ["lensGroups":
            ["chips": ["lenses":
                                ["Work": ["showProperties": ["hasTitle",
                                                            "contribution",
                                                            "language"]],
                                "ProvisionActivity": ["showProperties": ["date",
                                                                        "agent",
                                                                        "place"]]]],
            "cards": ["lenses":
                                ["Instance":
                                        ["showProperties": ["mediaType",
                                                            "hasTitle",
                                                            "instanceOf"]]]]]]
        Map vocabData = ["@graph": [
            [
                "@id": "https://id.kb.se/vocab/Item",
                "category":["@id":"https://id.kb.se/marc/hold"]
            ]
        ]]
        jsonld = new JsonLd(contextData, displayData, vocabData)
    }


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

    def "deepcopy should not fail for map"() {
        given:
        LinkedHashMap map = ['@graph':[['marc:encLevel':['@id':'marc:MinimalLevel'], '@type':'Record', 'controlNumber':'5067018', 'created':'1986-12-05T00:00:00.0+01:00', 'marc:catalogingSource':['@id':'marc:CooperativeCatalogingProgram'], 'modified':'2010-04-29T17:00:41.0+02:00', 'librisIIINumber':['0582348854'], 'assigner':['@type':'Organization', 'name':'Uh'], '@id':'http://localhost/8rkbh8ll0d17ptf', 'sameAs':[['@id':'http://libris.kb.se/bib/5067018']], 'mainEntity':['@id':'http://localhost/8rkbh8ll0d17ptf#it']], ['issuanceType':'Monograph', 'marc:publicationStatus':['@id':'marc:ReprintReissueDateAndOriginalDate'], 'marc:publishedYear':'1985', 'marc:otherYear':'1966', 'publicationCountry':[['@id':'https://id.kb.se/country/xxk']], 'identifiedBy':[['@type':'ISBN', 'value':'0-582-34885-4']], 'hasTitle':[['@type':'InstanceTitle', 'marc:searchElement':false, 'mainTitle':'Billy Liar']], 'responsibilityStatement':'Keith Waterhouse. The loneliness of the long-distance runner / Alan Sillitoe', 'publication':[['@type':'Publication', 'place':['@type':'Place', 'label':'Harlow, Essex'], 'agent':['@type':'Agent', 'label':'Longman'], 'date':'1985']], 'extent':'263 s.', 'hasSeries':[['@type':'Serial', 'title':'Heritage of literature']], 'influentialRelation':[['@type':'UniformWork', 'title':'The loneliness of the long-distance runner']], '@type':'Instance', '@id':'http://localhost/8rkbh8ll0d17ptf#it', 'sameAs':[['@id':'http://libris.kb.se/resource/bib/5067018']], 'instanceOf':['@id':'http://localhost/8rkbh8ll0d17ptf#work']], ['@type':'Text', 'language':[['@id':'https://id.kb.se/language/eng']], 'marc:literaryForm':['@id':'marc:NotFictionNotFurtherSpecified'], 'qualifiedAttribution':[['@type':'Principal', 'agent':['@type':'Person', 'familyName':'Waterhouse', 'givenName':'Keith']], ['@type':'Additional', 'agent':['@type':'Person', 'familyName':'Sillitoe', 'givenName':'Alan', 'birthYear':'1928', 'deathYear':'2010']]], '@id':'http://localhost/8rkbh8ll0d17ptf#work']]]

        when:
        def copy = Document.deepCopy(map)
        then:

        copy.inspect() == map.inspect()

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
        Set expected = [
                new Link("/externalBar", "meta.bar"),
                new Link("/externalBaz", "meta.extra.baz")
        ]
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

        List extra = [["@id": "/externalBar",
                                      "@type": "Work",
                                      "hasTitle": "aTitle",
                                      "language": "danska",
                                      "someThirdValue": 3],
                     ["@id": "/externalBaz",
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
                                               "language": "danska",
                                               "someThirdValue": 3]],
                                   ["@graph": ["@id": "/externalBaz",
                                               "@type": "ProvisionActivity",
                                               "date": "aDate",
                                               "place": "aPlace",
                                               "someFourthValue": 4]]]]

        when:

        Map result = jsonld.embellish(input, extra)

        then:
        assert result == expected
    }

    def "should return true for holding"() {
        given:
        def id = "/1234"
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def content = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["notation": "S"]]]]
        Document doc = new Document(content)

        expect:
        assert doc.isHolding(jsonld) == true
    }

    def "should return false for non-holding"() {
        given:
        def id = "/1234"
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def content = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Work",
                                      "contains": "some new other data"]]]
        Document doc = new Document(content)

        expect:
        assert doc.isHolding(jsonld) == false
    }

    def "should get sigel for holding"() {
        given:
        def id = "/1234"
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def content = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["notation": "S", "@id": "https://libris.kb.se/library/S"]]]]
        Document doc = new Document(content)
        expect:
        assert doc.getHeldBySigel() == 'S'
    }

    def "should return null for holding without sigel, I"() {
        given:
        def id = "/1234"
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def content = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data",
                                      "heldBy": []]]]
        Document doc = new Document(content)
        expect:
        assert doc.getHeldBySigel() == null
    }

    def "should return null for holding without sigel, II"() {
        given:
        def id = "/1234"
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def content = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Item",
                                      "contains": "some new other data"]]]
        Document doc = new Document(content)
        expect:
        assert doc.getHeldBySigel() == null
    }

    def "should return null sigel for non-holding"() {
        given:
        def id = "/1234"
        def createdDate = "2009-04-21T00:00:00.0+02:00"
        def modifiedDate = new Date()
        def content = ["@graph": [["@id": id,
                                      "@type": "Record",
                                      "created": createdDate,
                                      "modified": modifiedDate,
                                      "contains": "some updated data"],
                                     ["@id": "/itemId",
                                      "@type": "Work",
                                      "contains": "some new other data",
                                      "heldBy":
                                              ["notation": "S"]]]]
        Document doc = new Document(content)
        expect:
        assert doc.getHeldBySigel() == null
    }

    def "can set modified"() {
        given:
        var doc = new Document(["@graph": [["@id": "/id", "@type": "Record"], ["@id": "/itemId", "@type": "Work"]]])
        doc.modified = Timestamp.valueOf("2001-12-11 00:00:00.0")

        expect:
        doc.modifiedTimestamp == Timestamp.valueOf("2001-12-11 00:00:00.0").toInstant()
    }

    def "checksum is not affected by created/modified"() {
        given:
        def doc = new Document(readFile("cryptosporidium.jsonld"))
        def doc2 = doc.clone()
        doc2.setCreated(new Date())
        doc2.setModified(new Date())
        def doc3 = doc2.clone()
        doc3.setDescriptionCreator("Z")

        expect:
        doc.getChecksum(jsonld) == doc2.getChecksum(jsonld)
        doc.getChecksum(jsonld) != doc3.getChecksum(jsonld)
    }
    
    def "checksum handles @container @list/@set"() {
        given: 
        def set1 = new Document(["@graph": [["@id": "/id", "@type": "Record"], ["@id": "/itemId", "@type": "Work", "iAmASet": [1, 2, 3]]]])
        def set2 = new Document(["@graph": [["@id": "/id", "@type": "Record"], ["@id": "/itemId", "@type": "Work", "iAmASet": [3, 1, 2]]]])
        def list1 = new Document(["@graph": [["@id": "/id", "@type": "Record"], ["@id": "/itemId", "@type": "Work", "iAmAList": [1, 2, 3]]]])
        def list2 = new Document(["@graph": [["@id": "/id", "@type": "Record"], ["@id": "/itemId", "@type": "Work", "iAmAList": [3, 1, 2]]]])
        def implicitList1 = new Document(["@graph": [["@id": "/id", "@type": "Record"], ["@id": "/itemId", "@type": "Work", "iAmUndefined": [1, 2, 3]]]])
        def implicitList2 = new Document(["@graph": [["@id": "/id", "@type": "Record"], ["@id": "/itemId", "@type": "Work", "iAmUndefined": [3, 1, 2]]]])

        expect:
        set1.getChecksum(jsonld) == set2.getChecksum(jsonld)
        list1.getChecksum(jsonld) != list2.getChecksum(jsonld)
        implicitList1.getChecksum(jsonld) != implicitList2.getChecksum(jsonld)
    }

    def "checksum is affected by values moving from one key to another on the same level"() {
        given:
        def set1 = new Document(["@graph": [["@id": "/id", "@type": "Record"], ["@id": "/itemId", "@type": "Work", "prop1": "19uu", "prop2":"[19??]"]]])
        def set2 = new Document(["@graph": [["@id": "/id", "@type": "Record"], ["@id": "/itemId", "@type": "Work", "prop2": "19uu", "prop1":"[19??]"]]])

        expect:
        set1.getChecksum(jsonld) != set2.getChecksum(jsonld)
    }

    static String readFile(String filename) {
        return DocumentSpec.class.getClassLoader()
                .getResourceAsStream(filename).getText("UTF-8")
    }
}
