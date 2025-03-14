package whelk.util

import org.codehaus.jackson.map.ObjectMapper
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Unroll
import whelk.Document
import whelk.JsonLd
import whelk.Link
import whelk.Whelk
import whelk.exception.FramingException

@Unroll
class JsonLdSpec extends Specification {

    static final ObjectMapper mapper = new ObjectMapper()

    static final Map CONTEXT_DATA = [
        "@context": [
            "@vocab": "http://example.org/ns/",
            "pfx": "http://example.org/pfx/"
        ]
    ]

    static final Map VOCAB_DATA = [
        "@graph": [
            ["@id": "http://example.org/ns/ProvisionActivity",
             "subClassOf": [ ["@id": "http://example.org/ns/Event"] ]],
            ["@id": "http://example.org/ns/Publication",
             "subClassOf": ["@id": "http://example.org/ns/ProvisionActivity"]],
            ["@id": "http://example.org/pfx/SpecialPublication",
             "subClassOf": ["@id": "http://example.org/ns/Publication"]],

            ["@id": "http://example.org/ns/label", "@type": ["@id": "DatatypeProperty" ]],
            ["@id": "http://example.org/ns/prefLabel",
             "subPropertyOf": [ ["@id": "http://example.org/ns/label"] ]],
            ["@id": "http://example.org/ns/preferredLabel",
             "subPropertyOf": ["@id": "http://example.org/ns/prefLabel"]],
            ["@id": "http://example.org/ns/name",
             "subPropertyOf": ["@id": "http://example.org/ns/label"]]
        ]
    ]

    def "should find external references"() {
        given:
        def graph = ['@graph': [
                ['@id': '/foo',
                    'sameAs': [['@id': '/bar'], ['@id': '/baz']],
                    'third': ['@id': '/third']
                ],
                ['@id': '/second',
                    'foo': '/foo',
                    'some': 'value',
                    'bar': ['@id': '/ext1']],
                ['@id': '/third',
                    'external': ['@id': '/external']],
                ['@id': '/fourth',
                    'internal': ['@id': '/some_id']],
                ['@graph': [['@id': '/some_id']]],
                ['@graph': ['@id': '/some_other_id']]
            ],
            '@context': 'base.jsonld']
        Set expected = [
                new Link(iri: '/ext1', relation: 'bar'),
                new Link(iri: '/external', relation: '2.external')
        ]

        expect:
        assert JsonLd.getExternalReferences(graph) == expected
    }

    def "should get local objects"() {
        given:
        def input = ['@graph': [['@id': '/foo',
                                'sameAs': [['@id': '/baz']],
                                'bar': ['@id': '/bar']],
                                ['@graph': [['@id': '/quux',
                                             'some': 'value']]],
                                ['@graph': ['@id': '/some_id']]]]
        Set expected = ['/foo', '/quux', '/some_id']

        expect:
        assert JsonLd.getLocalObjects(input) == expected
    }

    def "should handle malformed sameAs when getting local objects"() {
        given:
        def input = ['@graph': [['@id': '/foo',
                                'sameAs': [['bad_key': '/baz']],
                                'bar': ['@id': '/bar']],
                                ['@graph': [['@id': '/quux',
                                             'some': 'value']]]]]
        Set expected = ['/foo', '/quux']

        expect:
        assert JsonLd.getLocalObjects(input) == expected
    }

   def "should get all references"() {
       given:
       def input = ['@graph': [['@id': '/foo',
                                'bar': ['@id': '/bar'],
                                'extra': ['baz': ['@id': '/baz']],
                                'aList': [['quux': ['@id': '/quux']]],
                                'quux': [['@id': '/quux'], ['@id': '/quux2']]],
                               ['@id': '/bar',
                                'someValue': 1],
                               ['@id': '/baz/',
                                'someOtherValue': 2]]]
       Set expected = [
               new Link(iri: '/quux', relation: 'meta.aList.quux'),
               new Link(iri: '/quux', relation: 'meta.quux'),
               new Link(iri: '/quux2', relation: 'meta.quux'),
               new Link(iri: '/baz', relation: 'meta.extra.baz'),
               new Link(iri: '/bar', relation: 'meta.bar')
       ]
       expect:
       assert JsonLd.getAllReferences(input) == expected
   }

   def "should not get references if @graph is missing"() {
       given:
       def input = ['@id': '/foo']
       when:
       JsonLd.getAllReferences(input)
       then:
       thrown FramingException
   }

   def "should frame flat jsonld"() {
        given:
        def id = "1234"
        def input = ["@graph": [["@id": id, "foo": "bar"]]]
        def expected = ["@id": id, "foo": "bar"]
        expect:
        assert JsonLd.frame(id, input) == expected
    }

   def "should link record as meta when framing jsonld"() {
        given:
        def input = ["@graph": [
            ["@id": "1", "mainEntity": ["@id": "1#it"]],
            ["@id": "1#it", "foo": "bar"]
        ]]
        def expected = [
            "@id": "1#it",
            "foo": "bar",
            "meta": ["@id": "1", "mainEntity": ["@id": "1#it"]]
        ]
        expect:
        assert JsonLd.frame("1#it", input) == expected
    }

    def "framing framed jsonld should preserve input"() {
        given:
        def id = "1234"
        def input = ["@id": id, "foo": "bar"]
        expect:
        assert JsonLd.frame(id, input) == input
    }

    def "should frame on sameAs-link"() {
        given:
        def id = "1234"
        def sameAs = "4321"
        def input = ["@graph": [
                ["@id": id, "foo": "bar", "linksTo": ["@id" : sameAs]],
                ["@id": "other_id", "sameAs":[["@id": sameAs]], "some":"data"]
        ]]
        def expected = ["@id": id, "foo": "bar", "linksTo": ["@id": "other_id", "sameAs":[["@id": sameAs]], "some":"data"]]
        expect:
        assert JsonLd.frame(id, input) == expected
    }

   def "should frame flat jsonld for works with linked contributors"() {
        given:
        def input = ["@graph": [
          ["@id": "record1", "mainEntity": ["@id": "print1"]],
          ["@id": "print1", "instanceOf": ["@id": "text1"]],
          ["@id": "text1", "contribution": ["agent": ["@id": "person1"]]],
          ["@id": "person1", "name": "Person One"]
        ]]
        def expected = [
          "@id": "record1",
          "mainEntity": [
            "@id": "print1",
            "instanceOf": [
              "@id": "text1",
              "contribution": ["agent": ["@id": "person1", "name": "Person One"]],
            ],
            "meta": ["@id": "record1"]
          ]
        ]
        expect:
        assert JsonLd.frame("record1", input, 3) == expected
    }

    def "should flatten framed jsonld"() {
        given:
        def id = "1234"
        def input = ["@id": id, "foo": "bar"]
        def expected = ["@graph": [["@id": id, "foo": "bar"]]]
        expect:
        assert JsonLd.flatten(input) == expected
    }

    def "flattening flat jsonld should preserve input"() {
        given:
        def id = "1234"
        def input = ["@graph": [["@id": id, "foo": "bar"]]]
        expect:
        assert JsonLd.flatten(input) == input
    }

    def "should preserve unframed json"() {
        expect:
        JsonLd.flatten(mundaneJson).equals(mundaneJson)
        where:
        mundaneJson = ["data":"foo","sameAs":"/some/other"]
    }

    def "should detect flat jsonld"() {
        given:
        def flatInput = """
        {"@graph": [{"@id": "/bib/13531679",
                     "encLevel": {"@id": "/def/enum/record/AbbreviatedLevel"},
                     "catForm": {"@id":"/def/enum/record/AACR2"},
                     "@type": "Record",
                     "controlNumber": "13531679",
                     "created": "2013-03-02T00:00:00.0+01:00",
                     "catalogingSource": {"@id":"/def/enum/content/OtherSource"},
                     "modified":"2015-09-23T15:20:09.0+02:00",
                     "systemNumber": ["(Elib)9789174771107"],
                     "technicalNote": ["Ändrad av Elib 2013-03-01"],
                     "about": {"@id": "/resource/bib/13531679"}}]}
        """ // """
        def framedInput = """
        {"@id": "/bib/13531679",
         "encLevel": {"@id": "/def/enum/record/AbbreviatedLevel"},
         "catForm": {"@id":"/def/enum/record/AACR2"},
         "@type": "Record",
         "controlNumber": "13531679",
         "created": "2013-03-02T00:00:00.0+01:00",
         "catalogingSource": {"@id":"/def/enum/content/OtherSource"},
         "modified":"2015-09-23T15:20:09.0+02:00",
         "systemNumber": ["(Elib)9789174771107"],
         "technicalNote": ["Ändrad av Elib 2013-03-01"],
         "about": {"@id": "/resource/bib/13531679"}}]}
        """
        def flatJson = mapper.readValue(flatInput, Map)
        def framedJson = mapper.readValue(framedInput, Map)
        expect:
        JsonLd.isFlat(flatJson) == true
        JsonLd.isFlat(framedJson) == false
    }

    def  "should retrieve actual URI from @id in document"() {
        when:
        URI uri1 = JsonLd.findRecordURI(
            ["@graph": [["@id": "/qowiudhqw",
                         "name": "foo"]]])
        URI uri2 = JsonLd.findRecordURI(
            ["@graph": [["@id": "http://id.kb.se/foo/bar",
                         "name": "foo"]]])
        URI uri3 = JsonLd.findRecordURI(["data":"foo","sameAs":"/some/other"])
        then:
        uri1.toString() == Document.BASE_URI.toString() + "qowiudhqw"
        uri2.toString() == "http://id.kb.se/foo/bar"
        uri3 == null
    }

    def "should find database id from @id in document"() {
        when:
        String s1 = JsonLd.findIdentifier(
            ["@graph": [["@id": "/qowiudhqw",
                         "name": "foo"]]])
        String s2 = JsonLd.findIdentifier(
            ["@graph": [["@id": "http://id.kb.se/foo/bar",
                         "name": "foo"]]])
        String s3 = JsonLd.findIdentifier(
            ["@graph": [["@id": Document.BASE_URI.resolve("/qowiudhqw").toString(),
                         "name": "foo"]]])
        then:
        s1 == "qowiudhqw"
        s2 == "http://id.kb.se/foo/bar"
        s3 == "qowiudhqw"
    }

    def "should convert to cards and chips"() {
        given:
        Map input = ["@type": "Instance",
            "mediaType": "foobar",
            "instanceOf": ["@type": "Work",
                        "contribution": ["@type": "Text",
                                            "foo": ["mediaType": "bar"]],
                        "hasTitle": ["@type": "Publication",
                                        "date": "2000-01-01",
                                        "noValidKey": "shouldBeRemoved",
                                        "@id": "foo"]],
            "@aKey": "external-foobar",
            "hasTitle": ["value1", "value2", "value3", ["someKey": "theValue",
                                                        "@type": "Work"]],
            "foo": "bar"]

        Map displayData = [
            "lensGroups":
                    ["chips":
                            ["lenses":
                                        ["Work": ["showProperties": ["hasTitle",
                                                                    "contribution",
                                                                    "language"]],
                                        "Event": ["showProperties": ["date",
                                                                                "agent",
                                                                                "place"]]]],
                    "cards":
                            ["lenses":
                                        ["Instance":
                                                ["showProperties": ["mediaType",
                                                                    "hasTitle",
                                                                    "instanceOf"]]]]]]

        Map expected = ["@type": "Instance",
                      "mediaType": "foobar",
                      "instanceOf": ["@type": "Work",
                                     "contribution": ["@type": "Text",
                                                      "foo": ["mediaType": "bar"]],
                                     "hasTitle": ["@type": "Publication",
                                                  "date": "2000-01-01",
                                                  "@id": "foo"]],
                      "@aKey": "external-foobar",
                      "hasTitle": ["value1", "value2", "value3", ["@type": "Work"]]]

        def ld = new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)

        expect:
        Map output = ld.toCard(input)
        output == expected
    }

    def "extend lenses with property aliases"() {
        given:
        Map displayData = [
            "@context": [
                "labelByLang": ["@id": "label", "@container": "@language"],
                "titleByLang": ["@id": "title", "@container": "@language"],
                "barByLang": ["@id": "bar", "@container": "@language"]
            ],
            "lensGroups":
                    ["chips":
                            ["lenses": [
                                "Thing": ["showProperties": [
                                        "notation", 
                                        "label", 
                                        "note", 
                                        ["alternateProperties": [
                                                "foo",
                                                ["subPropertyOf": "bar", "range": "Bar"],
                                                "title"
                                        ]]
                                ]]]
                            ]]]
        def ld = new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)
        expect:
        def props = ld.displayData.lensGroups.chips.lenses.Thing.showProperties
        props == [
                'notation', 
                'label', 
                'labelByLang', 
                'note', 
                ["alternateProperties": [
                        "foo",
                        ["subPropertyOf": "bar", "range": "Bar"],
                        ["title", "titleByLang"]
                ]]
        ]
    }

    def "should handle lens inheritance"() {
        given:
        Map displayData = [
                "lensGroups":
                        ["chips":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-chips",
                                               "fresnel:extends": ["@id": "Y-chips"],
                                               "showProperties" : ["x1", "x2", "fresnel:super", "x3"]
                                         ],
                                         "Y": ["@type"         : "fresnel:Lens",
                                               "@id"           : "Y-chips",
                                               "showProperties": ["y1", "y2",]
                                         ],
                                         "Q": ["@type"          : "fresnel:Lens",
                                               "@id"            : "Q-chips",
                                               "fresnel:extends": ["@id": "P-cards"],
                                               "showProperties" : []
                                         ],
                                 ]],
                         "cards":
                                 ["lenses": [
                                         "Z": ["@type"          : "fresnel:Lens",  // no @id
                                               "fresnel:extends": ["@id": "X-chips"],
                                               "showProperties" : ["z1", "z2", "z3"]
                                         ],
                                         "P": ["@type"          : "fresnel:Lens",
                                               "@id"            : "P-cards",
                                               "fresnel:extends": ["@id": "Y-chips"],
                                               "showProperties" : ["p", "fresnel:super",]
                                         ],
                                 ]]]]


        def ld = new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)
        def groups = ld.displayData.lensGroups

        expect:
        ld.getLensFor(thing, groups[group])['showProperties'] == props

        where:
        thing          | group   || props
        ["@type": "X"] | 'chips' || ['x1', 'x2', 'y1', 'y2', 'x3']
        ["@type": "Y"] | 'chips' || ['y1', 'y2']
        ["@type": "Z"] | 'cards' || ['x1', 'x2', 'y1', 'y2', 'x3', 'z1', 'z2', 'z3']
        ["@type": "P"] | 'cards' || ['p', 'y1', 'y2']
        ["@type": "Q"] | 'chips' || ['p', 'y1', 'y2']
    }

    def "should handle lens inheritance loops"() {
        given:
        Map displayData = [
                "lensGroups":
                        ["chips":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-chips",
                                               "fresnel:extends": ["@id": "Y-chips"],
                                               "showProperties" : ["x"]
                                         ],
                                         "Y": ["@type"         : "fresnel:Lens",
                                               "@id"           : "Y-chips",
                                               "fresnel:extends": ["@id": "Z-chips"],
                                               "showProperties": ["y"]
                                         ],
                                         "Z": ["@type"          : "fresnel:Lens",
                                               "@id"            : "Z-chips",
                                               "fresnel:extends": ["@id": "X-chips"],
                                               "showProperties" : ["z"]
                                         ],
                                 ]]
                        ]]


        when:
        new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)

        then:
        thrown JsonLd.FresnelException
    }

    def "should handle bad fresnel:extends id"() {
        given:
        Map displayData = [
                "lensGroups":
                        ["chips":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-chips",
                                               "fresnel:extends": ["@id": "Y-chips"],
                                               "showProperties" : ["x"]
                                         ]
                                 ]]
                        ]]


        when:
        new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)

        then:
        thrown JsonLd.FresnelException
    }

    def "should handle inverse lens properties"() {
        given:
        Map displayData = [
                "lensGroups":
                        ["chips":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-chips",
                                               "showProperties" : ["x1", "x2", ["inverseOf": "prop1"]]
                                         ],
                                         "Q": ["@type"          : "fresnel:Lens",
                                               "@id"            : "Q-chips",
                                               "showProperties" : ["q", ["inverseOf": "propQ"]]
                                         ]
                                 ]],
                         "cards":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-cards",
                                               "fresnel:extends": ["@id": "X-chips"],
                                               "showProperties" : ["fresnel:super", "x3", ["inverseOf": "prop2"]]
                                         ],
                                         "P": ["@type"          : "fresnel:Lens",
                                               "@id"            : "P-cards",
                                               "fresnel:extends": ["@id": "X-cards"],
                                               "showProperties" : ["p", "fresnel:super", ["inverseOf": "prop3"]]
                                         ],

                                 ]]]]


        def ld = new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)

        expect:
        ld.getInverseProperties(thing, group) as List == props

        where:
        thing                                        | group   || props
        ["@type": "X"]                               | 'chips' || ['prop1']
        ["@type": "X"]                               | 'cards' || ['prop1', 'prop2']
        ["@type": "P"]                               | 'cards' || ['prop1', 'prop2', 'prop3']
        ["@graph": [["@type": "Q"], ["@type": "X"]]] | 'chips' || ['propQ', 'prop1']
        ["@graph": [["@type": "X"], ["@type": "X"]]] | 'chips' || ['prop1']
    }

    def "should handle lens alternateProperties"() {
        given:
        Map displayData = [
                "@context": [
                        "a1ByLang": ["@id": "a1", "@container": "@language"],
                ],
                "lensGroups":
                        ["chips":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-chips",
                                               "showProperties" : [["alternateProperties": ["a1", "a2", "a3"]]]
                                         ],
                                 ]],
                         "cards":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-cards",
                                               "showProperties" : ["x1", ["alternateProperties": ["a1", "a2"]], "x2", ["alternateProperties": ["a3", "a4"]]]
                                         ],
                                 ]],
                        ]
                        ]
        
        def ld = new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)
        
        expect:
        group == 'cards' ? ld.toCard(thing) : ld.toChip(thing) == expected

        where:
        thing                                                          | group   || expected
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a1': 'a1', 'a2': 'a2'] | 'chips' || ['@type': 'X', 'a1': 'a1', 'a2': 'a2']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a2': 'a2']             | 'chips' || ['@type': 'X', 'a2': 'a2']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a3': 'a3']             | 'chips' || ['@type': 'X', 'a3': 'a3']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2']                         | 'chips' || ['@type': 'X']

        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a1': 'a1', 'a2': 'a2'] | 'cards' || ['@type': 'X', 'x1': 'x1', 'a1': 'a1', 'x2': 'x2']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a2': 'a2']             | 'cards' || ['@type': 'X', 'x1': 'x1', 'a2': 'a2', 'x2': 'x2']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a3': 'a3']             | 'cards' || ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a3': 'a3']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a2': 'a2', 'a4': 'a4'] | 'cards' || ['@type': 'X', 'x1': 'x1', 'a2': 'a2', 'x2': 'x2', 'a4': 'a4']

        ['@type': 'X', 'a1': 'a1']                                     | 'chips' || ['@type': 'X', 'a1': 'a1']
        ['@type': 'X', 'a1ByLang': ['xx': 'a1']]                       | 'chips' || ['@type': 'X', 'a1ByLang': ['xx': 'a1']]
        ['@type': 'X', 'a1': 'a1', 'a1ByLang': ['xx': 'a1']]           | 'chips' || ['@type': 'X', 'a1': 'a1', 'a1ByLang': ['xx': 'a1']]
    }

    def "should handle lens alternateProperties with subPropertyOf"() {
        given:
        Map displayData = [
                "@context": [
                        "a1ByLang": ["@id": "a1", "@container": "@language"],
                ],
                "lensGroups":
                        ["chips":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-chips",
                                               "showProperties" : [
                                                       ["alternateProperties": [
                                                               "a1", 
                                                               "a2", 
                                                               "a3"
                                                       ]]
                                               ]
                                         ],
                                 ]],
                         "cards":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-cards",
                                               "showProperties" : [
                                                       "x1", 
                                                       ["alternateProperties": [
                                                               ["subPropertyOf": "a1", "range": "Y"],
                                                               "a2"
                                                       ]], 
                                                       "x2", 
                                                       ["alternateProperties": [
                                                               "a3",
                                                               ["subPropertyOf": "a4", "range": "Y"],
                                                       ]]
                                               ]
                                         ],
                                 ]],
                        ]
        ]

        def ld = new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)

        expect:
        group == 'cards' ? ld.toCard(thing) : ld.toChip(thing) == expected

        where:
        thing                                                          | group   || expected
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a1': 'a1', 'a2': 'a2'] | 'chips' || ['@type': 'X', 'a1': 'a1', 'a2': 'a2']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a2': 'a2']             | 'chips' || ['@type': 'X', 'a2': 'a2']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a3': 'a3']             | 'chips' || ['@type': 'X', 'a3': 'a3']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2']                         | 'chips' || ['@type': 'X']

        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a1': 'a1', 'a2': 'a2'] | 'cards' || ['@type': 'X', 'x1': 'x1', 'a1': 'a1', 'x2': 'x2']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a2': 'a2']             | 'cards' || ['@type': 'X', 'x1': 'x1', 'a2': 'a2', 'x2': 'x2']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a3': 'a3']             | 'cards' || ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a3': 'a3']
        ['@type': 'X', 'x1': 'x1', 'x2': 'x2', 'a2': 'a2', 'a4': 'a4'] | 'cards' || ['@type': 'X', 'x1': 'x1', 'a2': 'a2', 'x2': 'x2', 'a4': 'a4']

        ['@type': 'X', 'a1': 'a1']                                     | 'chips' || ['@type': 'X', 'a1': 'a1']
        ['@type': 'X', 'a1ByLang': ['xx': 'a1']]                       | 'chips' || ['@type': 'X', 'a1ByLang': ['xx': 'a1']]
        ['@type': 'X', 'a1': 'a1', 'a1ByLang': ['xx': 'a1']]           | 'chips' || ['@type': 'X', 'a1': 'a1', 'a1ByLang': ['xx': 'a1']]
    }

    def "applyLensAsMapByLang"() {
        given:
        Map displayData = [
                "@context": [
                        "a1ByLang": ["@id": "a1", "@container": "@language"],
                        "a2ByLang": ["@id": "a2", "@container": "@language"],
                        "a3ByLang": ["@id": "a3", "@container": "@language"],
                        "x1ByLang": ["@id": "x1", "@container": "@language"],
                ],
                "lensGroups":
                        ["chips":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-chips",
                                               "showProperties" : [
                                                       ["alternateProperties": [
                                                               "a1", 
                                                               "a2", 
                                                               "a3"]]
                                               ]
                                         ],
                                 ]],
                         "cards":
                                 ["lenses": [
                                         "X": ["@type"          : "fresnel:Lens",
                                               "@id"            : "X-cards",
                                               "showProperties" : [
                                                       "x1", 
                                                       ["alternateProperties": [
                                                               "a1", 
                                                               "a2"]], 
                                                       "x2", 
                                                       ["alternateProperties": [
                                                               ["subPropertyOf": "a3", "range": "Y"],
                                                               ["subPropertyOf": "a3", "range": "Z"],
                                                               "a4"
                                                       ]]
                                               ]
                                         ],
                                         "Y": ["@type"          : "fresnel:Lens",
                                               "@id"            : "Y-cards",
                                               "showProperties" : [
                                                       "a1",
                                               ]
                                         ],
                                         "Z": ["@type"          : "fresnel:Lens",
                                               "@id"            : "Z-cards",
                                               "showProperties" : [
                                                       "a1",
                                               ]
                                         ],
                                 ]],
                        ]
        ]

        def ld = new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)

        expect:
        ld.applyLensAsMapByLang(thing, ['sv'] as Set, [], ['cards']) == expected

        where:
        thing                                                                  || expected
        ['@type': 'X', 'x1': 'x1', 'x1ByLang': ['sv': 'x1ByLang']]             || ['sv': 'x1ByLang']
        ['@type': 'X', 'x1': 'x1', 'x1ByLang': ['de': 'x1ByLang']]             || ['sv': 'x1']

        ['@type': 'X', 'x1': 'x1', 'a1ByLang': ['sv': 'a1ByLang'], 'a1': 'a1'] || ['sv': 'x1, a1ByLang']
        ['@type': 'X', 'x1': 'x1', 'a1ByLang': ['de': 'a1ByLang'], 'a1': 'a1'] || ['sv': 'x1, a1']
        ['@type': 'X', 'x1': 'x1', 'a2': 'a2']                                 || ['sv': 'x1, a2']
        ['@type': 'X', 'x1': 'x1', 'a1ByLang': ['de': 'a1ByLang'], 'a2': 'a2'] || ['sv': 'x1, a2']

        ['@type': 'X', 'x1': 'x1', 'a3': ['@type': 'X', 'a1ByLang': ['sv': 'a1ByLang']]] || ['sv': 'x1']
        ['@type': 'X', 'x1': 'x1', 'a3': ['@type': 'Y', 'a1ByLang': ['sv': 'a1ByLang']]] || ['sv': 'x1, a1ByLang']
        ['@type': 'X', 'x1': 'x1', 'a3': ['@type': 'Y', 'a1ByLang': ['de': 'a1ByLang']]] || ['sv': 'x1']
        ['@type': 'X', 'x1': 'x1', 'a3': ['@type': 'Z', 'a1': 'a1']]                     || ['sv': 'x1, a1']
    }

    def "get term key from URI"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        expect:
        ld.toTermKey(uri) == term
        where:
        term                        | uri
        'Publication'               | 'http://example.org/ns/Publication'
        'pfx:SpecialPublication'    | 'http://example.org/pfx/SpecialPublication'
    }

    def "use vocab to get subclasses of a base class"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        expect:
        ld.getSubClasses('ProvisionActivity') == ['Publication', 'pfx:SpecialPublication'] as Set
    }

    def "use vocab to get subproperties of a base property"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        expect:
        ld.getSubProperties('label') == ['prefLabel', 'preferredLabel', 'name'] as Set
    }

    def "use vocab to match a subclass to a base class"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        expect:
        ld.isSubClassOf('Publication', 'ProvisionActivity')
        !ld.isSubClassOf('Work', 'ProvisionActivity')
        and:
        ld.isSubClassOf('pfx:SpecialPublication', 'Publication')
    }

    def "expand prefixes"() {
        given:
        Map context = ["ex": "http://example.org/ns/"]
        expect:
        ['/path', 'ex:path', 'other:path'].collect({JsonLd.expand(it, context)}) ==
                ['/path', 'http://example.org/ns/path', 'other:path']
    }


    def "should make @id map"() {
        given:
        Map input = ["@graph": [["@id": "/foo",
            "bar": ["@id": "/bar"]],
            ["@id": "/bar",
            "baz": ["quux": ["@id": "/quux"]]],
            ["@id": "/quux",
            "someValue": 1],
            ["someOtherValue": 2],
        ]]
        Map output = ["/foo": ["@id": "/foo",
                               "bar": ["@id": "/bar"]],
                      "/bar": ["@id": "/bar",
                               "baz": ["quux": ["@id": "/quux"]]],
                      "/quux": ["@id": "/quux",
                                "someValue": 1]]
        expect:
        assert JsonLd.getIdMap(input) == output
    }

    def "should recognize link"() {
        expect:
        JsonLd.isLink(data) == expected

        where:
        data                         || expected
        ['@id': 'foo']               || true
        ['@id': 'foo', 'bar': 'bar'] || false
        ['bar': 'bar']               || false
    }

    static final String FLAT_INPUT = readFile("flatten-001-in.jsonld")
    static final String FLAT_OUTPUT = readFile("flatten-001-out.jsonld")
    static final String FRAMED_INPUT = readFile("frame-001-in.jsonld")
    static final String FRAMED_OUTPUT = readFile("frame-001-out.jsonld")

    static final Map simpleGraph = ["@graph": [["@id": "/foo", "bar": "1"]]]
    static final Map simpleGraphFramed = ["@id": "/foo", "bar": "1"]
    static final Map simpleGraph2 = ["@graph": [["@id": "/foo",
                                                 "bar": ["@id": "/bar"]],
                                                ["@id": "/bar",
                                                 "someValue": 1]]]
    static final Map simpleGraphFramed2 = ["@id": "/foo",
                                           "bar": ["@id": "/bar",
                                                   "someValue": 1]]
    static final Map simpleGraph3 = ["@graph": [["@id": "/foo",
                                                 "bar": ["@id": "/bar"]]]]
    static final Map simpleGraphFramed3 = ["@id": "/foo",
                                           "bar": ["@id": "/bar"]]
    static final Map listGraph = ["@graph": [["@id": "/foo",
                                              "bar": [["@id": "/baz"],
                                                      ["@id": "/quux"]]],
                                             ["@id": "/baz",
                                              "someValue": 1]]]
    static final Map listGraphFramed = ["@id": "/foo",
                                        "bar": [["@id": "/baz",
                                                 "someValue": 1],
                                                ["@id": "/quux"]]]
    static final Map nestedGraph = ["@graph": [["@id": "/foo",
                                                "bar": [
                                                    "baz": ["@id": "/baz"]
                                                ]
                                               ],
                                               ["@id": "/baz",
                                                "someValue": 1]]]
    static final Map nestedGraphFramed = ["@id": "/foo",
                                          "bar": ["baz": ["@id": "/baz",
                                                          "someValue": 1]]]
    static final Map nestedGraph2 = ["@graph": [["@id": "/foo",
                                                 "bar": [
                                                     "baz": ["@id": "/baz"]
                                                 ],
                                                 "quux": ["@id": "/baz"]
                                                ],
                                                ["@id": "/baz",
                                                 "someValue": 1]]]
    static final Map nestedGraphFramed2 = ["@id": "/foo",
                                           "bar": ["baz": ["@id": "/baz",
                                                           "someValue": 1]],
                                           "quux": ["@id": "/baz",
                                                    "someValue": 1]]
    static final Map loopedGraph = ["@graph": [["@id": "/foo",
                                                "bar": ["@id": "/bar"]],
                                               ["@id": "/bar",
                                                "foo": ["@id": "/foo"]]]]

    static final String INPUT_ID = "https://libris.kb.se/record/something"

    def "should frame flat jsonld 2"() {
        given:
        Map input = mapper.readValue(FLAT_OUTPUT, Map)
        Map output = mapper.readValue(FLAT_INPUT, Map)
        expect:
        assert JsonLd.frame(INPUT_ID, input) == output
    }

    def "should flatten jsonld"() {
        given:
        Map input = mapper.readValue(FLAT_INPUT, Map)
        Map output = mapper.readValue(FLAT_OUTPUT, Map)
        expect:
        assert JsonLd.flatten(input) == output
    }

    def "should frame and expand"() {
        given:
        Map input = mapper.readValue(FRAMED_INPUT, Map)
        Map output = mapper.readValue(FRAMED_OUTPUT, Map)
        expect:
        assert JsonLd.frame(INPUT_ID, input) == output
    }

    def "should also frame and expand"() {
        expect:
        assert JsonLd.frame(id, input) == output
        where:
        id     | input        | output
        "/foo" | simpleGraph  | simpleGraphFramed
        "/foo" | simpleGraph2 | simpleGraphFramed2
        "/foo" | simpleGraph3 | simpleGraphFramed3
        "/foo" | listGraph  | listGraphFramed
        "/foo" | nestedGraph  | nestedGraphFramed
    }

    def "should flatten framed jsonld2"() {
        expect:
        assert JsonLd.flatten(input) == output
        where:
        input              | output
        simpleGraphFramed  | simpleGraph
        simpleGraphFramed2 | simpleGraph2
        simpleGraphFramed3 | simpleGraph3
        listGraphFramed    | listGraph
        nestedGraphFramed  | nestedGraph
        nestedGraphFramed2 | nestedGraph2
    }

    @Ignore // TODO: remove or alter to our needs?
    def "should not frame and expand"() {
        when:
        JsonLd.frame(id, input)
        then:
        def error = thrown(expectedException)
        assert error.message == expectedMessage
        where:
        id     | input       | expectedException | expectedMessage
        "/foo" | [:]         | FramingException  | "Missing '@graph' key in input"
        "/foo" | loopedGraph | FramingException  | "Circular dependency in input"
    }

    def "should expand references"() {
        given:
        String mainId = "/foo"
        Map input = ["@graph": [["@id": "/foo",
                                 "bar": ["@id": "/bar"]],
                                ["@id": "/bar",
                                 "baz": ["quux": ["@id": "/quux"]]],
                                ["@id": "/quux",
                                 "someValue": 1]]]
        Map output = ["@id": "/foo",
                      "bar": ["@id": "/bar",
                              "baz": ["quux": ["@id": "/quux",
                                               "someValue": 1]]]]
        expect:
        assert JsonLd.frame(mainId, input) == output
    }

    def "should soft merge"() {
        setup:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)

        when:
        def obj = ['@type': 'Publication']
        def into = ['@type': 'ProvisionActivity']
        then:
        ld.isSubClassOf(obj['@type'], into['@type'])
        ld.softMerge(obj, into) == true
        and:
        into['@type'] == 'Publication'

        when:
        obj = ['@type': 'ProvisionActivity']
        into = ['@type': 'Publication']
        then:
        ld.softMerge(obj, into) == false
        and:
        into['@type'] == 'Publication'

        when:
        obj = ['@type': 'Publication', date: '1978']
        into = ['@type': 'ProvisionActivity', date: '1978']
        then:
        ld.softMerge(obj, into) == true
        and:
        into == ['@type': 'Publication', date: '1978']

        when:
        obj = ['@type': 'Publication', date: '1978', label: 'Primary']
        into = ['@type': 'ProvisionActivity', date: '1978']
        then:
        ld.softMerge(obj, into) == true
        and:
        into == ['@type': 'Publication', date: '1978', label: 'Primary']

        when:
        obj = ['@type': 'Publication', date: '1978-01-01']
        into = ['@type': 'ProvisionActivity', date: '1978']
        then:
        ld.softMerge(obj, into) == false
        and:
        into == ['@type': 'ProvisionActivity', date: '1978']

        when:
        obj = ['@type': 'Publication', date: '1978-01-01']
        into = ['@type': 'ProvisionActivity', date: '1978']
        then:
        ld.softMerge(obj, into) == false
        and:
        into == ['@type': 'ProvisionActivity', date: '1978']

        when:
        obj = ['@type': 'Publication', date: '1978']
        into = ['@type': 'ProvisionActivity', date: ['1978', '1980']]
        then:
        ld.softMerge(obj, into) == false
        and:
        into == ['@type': 'ProvisionActivity', date: ['1978', '1980']]
    }

    def "should preserve links"() {
        given:
        Map doc = readMap("preserve-links/molecular-aspects.jsonld")

        def ld = new JsonLd(
                readMap("preserve-links/context.jsonld"),
                readMap("preserve-links/display-data.jsonld"),
                readMap("preserve-links/vocab.jsonld"))

        Set<String> preserveLinks = ["http://libris.kb.se.localhost:5000/n602lbw018zh88k#work", "https://id.kb.se/marc/Document"] as Set
        
        expect:
        ld.toChip(doc, preserveLinks) == readMap("preserve-links/molecular-aspects-chips.jsonld")
        ld.toCard(doc, true, false, false, preserveLinks) == readMap("preserve-links/molecular-aspects-cards-chips.jsonld")
        ld.toCard(doc, false, false, false, preserveLinks) == readMap("preserve-links/molecular-aspects-cards-cards.jsonld")
    }

    def "should preserve links 2"() {
        given:
        Map data = readMap("preserve-links/dtestpost.jsonld")

        def ld = new JsonLd(
                readMap("preserve-links/context.jsonld"),
                readMap("preserve-links/display-data.jsonld"),
                readMap("preserve-links/vocab.jsonld"))

        Set<String> preserveLinks = ld.expandLinks(new Document(data).getExternalRefs()).collect{ it.iri }

        expect:
        ld.toChip(data, preserveLinks) == readMap("preserve-links/dtestpost-chips-links.jsonld")
    }

    def "should apply inverses"() {
        def vocabData = [
                "@graph": [
                        ["@id": "http://example.org/ns/broader",
                         "inverseOf": [ ["@id": "http://example.org/ns/narrower"] ]],
                        ["@id": "http://example.org/ns/narrower"]
                ]
        ]

        def ld = new JsonLd(CONTEXT_DATA, [:], vocabData)
        Closure applyInverses =ld.&applyInverses

        given:
        def thing = [
                '@id': '1',
                '@reverse': [
                        'broader': [['@id': '1'], ['@id': '2']]
                ]
        ]
        when:
        applyInverses(thing)
        def revMap = thing.remove('@reverse')
        then:
        thing['narrower'] == [['@id': '1'], ['@id': '2']]

        when: 'adding inverses to existing accumulates results'
        thing['@reverse'] = revMap
        applyInverses(thing)
        thing.remove('@reverse')
        then:
        thing['narrower'] == [['@id': '1'], ['@id': '2']]
    }

    def "should merge inverses with existing relation"() {
        given:
        def vocabData = [
                "@graph": [
                    ["@id": "http://example.org/ns/narrower",
                     "inverseOf": [ ["@id": "http://example.org/ns/broader"] ]],
                    ["@id": "http://example.org/ns/broader"]
                ]
        ]
        def ld = new JsonLd(CONTEXT_DATA, [:], vocabData)

        def thing = [
                '@id': '1',
                'broader': [['@id': '1'], ['@id': '2']],
                '@reverse': [
                    'narrower': [['@id': '2'], ['@id': '3']]
                ]
        ]

        when:
        ld.applyInverses(thing)
        then:
        thing['broader'] == [['@id': '1'], ['@id': '2'], ['@id': '3']]
    }

    def "should apply owl inverses"() {
        def vocabData = [
                "@graph": [
                        ["@id": "http://example.org/ns/thisWay",
                         "owl:inverseOf": [ ["@id": "http://example.org/ns/thatWay"] ]],
                        ["@id": "http://example.org/ns/thatWay",
                         "owl:inverseOf": [ ["@id": "http://example.org/ns/thisWay"] ]]
                ]
        ]

        def ld = new JsonLd(CONTEXT_DATA, [:], vocabData)
        Closure applyInverses =ld.&applyInverses
        given:
        def thing = [
                '@id': '1',
                '@reverse': [
                        'thisWay': [['@id': '2'], ['@id': '3']]
                ]
        ]
        when:
        applyInverses(thing)
        then:
        thing['thatWay'] == [['@id': '2'], ['@id': '3']]
    }

    static Map readMap(String filename) {
        return mapper.readValue(readFile(filename), Map)
    }

    static String readFile(String filename) {
        return JsonLdSpec.class.getClassLoader()
            .getResourceAsStream(filename).getText("UTF-8")
    }
}
