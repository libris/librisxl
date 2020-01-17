package whelk

import spock.lang.Specification
import spock.lang.Unroll
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter
import whelk.filter.LinkFinder
import whelk.util.LegacyIntegrationTools
import whelk.util.PropertyLoader

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
            ["@vocab":"https://id.kb.se/vocab/"]
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

        Map extra = [["@id": "/externalBar",
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
                                               "language": "danska"]],
                                   ["@graph": ["@id": "/externalBaz",
                                               "@type": "ProvisionActivity",
                                               "date": "aDate",
                                               "place": "aPlace"]]]]

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
        assert doc.getSigel() == 'S'
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
        assert doc.getSigel() == null
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
        assert doc.getSigel() == null
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
        assert doc.getSigel() == null
    }

    def "Cannot set created"() {
        //TODO: Fix this and make this a positive test.
        //TODO: see JIRA Ticket LXL-329
        given:

        String docString = "['leader':'01114cam a22002897ar4500', 'fields':[['001':'714'], ['005':'20100621095537.0'], ['008':'810114s1980    gw a          001 0 ger c'], ['020':['ind1':' ', 'ind2':' ', 'subfields':[['a':'3-13-200904-0']]]], ['035':['ind1':' ', 'ind2':' ', 'subfields':[['9':'3132009040']]]], ['040':['ind1':' ', 'ind2':' ', 'subfields':[['a':'Lkc']]]], ['080':['ind1':' ', 'ind2':' ', 'subfields':[['a':'547']]]], ['084':['ind1':' ', 'ind2':' ', 'subfields':[['a':'Uceb'], ['2':'kssb/6']]]], ['245':['ind1':'0', 'ind2':'0', 'subfields':[['a':'Methoden der organischen Chemie :'], ['b':'(Houben-Weyl) /'], ['c':'begründet von Eugen Müller und Otto Bayer. Bd 4. 1. c, Reduktion, T. 1 / bearbeitet von A.W. Frahm ... ; Herausgeber Heinz Kropf']]]], ['247':['ind1':'0', 'ind2':'0', 'subfields':[['a':'Methods of organic chemistry.']]]], ['250':['ind1':' ', 'ind2':' ', 'subfields':[['a':'4., völlig neu gest. Aufl.']]]], ['260':['ind1':' ', 'ind2':' ', 'subfields':[['a':'Stuttgart :'], ['b':'Thieme,'], ['c':'1980']]]], ['300':['ind1':' ', 'ind2':' ', 'subfields':[['a':'xxxi, 912 s.'], ['b':'ill.']]]], ['599':['ind1':' ', 'ind2':' ', 'subfields':[['a':'T: Särkatalogiseras ej fr.o.m. okt. 1980. Bestånd se fortsättningskatalogen']]]], ['650':['ind1':' ', 'ind2':'0', 'subfields':[['a':'Chemistry, Organic']]]], ['700':['ind1':'1', 'ind2':' ', 'subfields':[['a':'Müller, Eugen'], ['4':'edt']]]], ['700':['ind1':'1', 'ind2':' ', 'subfields':[['a':'Bayer, Otto'], ['4':'edt']]]], ['700':['ind1':'1', 'ind2':' ', 'subfields':[['a':'Frahm, August W.'], ['4':'aut']]]], ['700':['ind1':'1', 'ind2':' ', 'subfields':[['a':'Kropf, Heinz'], ['4':'edt']]]], ['740':['ind1':'0', 'ind2':' ', 'subfields':[['a':'Reduktion, T. 1']]]], ['772':['ind1':'0', 'ind2':'0', 'subfields':[['7':'nn'], ['t':'Methoden der organischen Chemie : (Houben-Weyl)'], ['b':'4., völlig neu gestaltete Aufl.'], ['d':'Stuttgart : Thieme, 1953-'], ['w':'9900002636'], ['9':'4'], ['9':'1'], ['9':'c']]]], ['976':['ind1':' ', 'ind2':'2', 'subfields':[['a':'Uceb'], ['b':'Kemi Organisk']]]]]]"
        String id = 'gzrbgsks3xl1ndb'
        def converter = new MarcFrameConverter()
        def oldid = "/bib/451"

        Map convertedData = converter.convert(Eval.me(docString),id)
        Document doc = new Document(convertedData)
        def cs = doc.getChecksum()
        def lgc = LegacyIntegrationTools.generateId(oldid)
        doc.modified = Timestamp.valueOf("2001-12-11 00:00:00.0")
        expect:
        doc.modified != Timestamp.valueOf("2001-12-11 00:00:00.0")



    }


}
