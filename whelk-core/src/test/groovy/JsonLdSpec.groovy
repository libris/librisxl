package whelk.util

import spock.lang.Specification
import spock.lang.Unroll
import spock.lang.Ignore
import org.codehaus.jackson.map.*
import whelk.Document
import whelk.JsonLd
import whelk.exception.FramingException
import whelk.exception.ModelValidationException


@Unroll
class JsonLdSpec extends Specification {

    static final ObjectMapper mapper = new ObjectMapper()

    static final Map CONTEXT_DATA = [
        "@context": [
            "@vocab": "http://example.org/ns/"
        ]
    ]

    static final Map VOCAB_DATA = [
        "@graph": [
            ["@id": "http://example.org/ns/ProvisionActivity",
             "subClassOf": [ ["@id": "http://example.org/ns/Event"] ]],
            ["@id": "http://example.org/ns/Publication",
             "subClassOf": ["@id": "http://example.org/ns/ProvisionActivity"]]
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
                    'some': 'value'],
                ['@id': '/third',
                    'external': ['@id': '/external']],
                ['@id': '/fourth',
                    'internal': ['@id': '/some_id']],
                ['@graph': [['@id': '/some_id']]],
                ['@graph': ['@id': '/some_other_id']]
            ],
            '@context': 'base.jsonld']
        def expected = ['/external']

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
                                'quux': ['@id': '/quux']],
                               ['@id': '/bar',
                                'someValue': 1],
                               ['@id': '/baz/',
                                'someOtherValue': 2]]]
       Set expected = ['/bar', '/baz', '/quux']
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

    def "framing framed jsonld should preserve input"() {
        given:
        def id = "1234"
        def input = ["@id": id, "foo": "bar"]
        expect:
        assert JsonLd.frame(id, input) == input
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
                "labelByLang": ["@id": "label", "@container": "@language"]
            ],
            "lensGroups":
                    ["chips":
                            ["lenses": [
                                "Thing": ["showProperties": ["notation", "label", "note"]]]
                            ]]]
        def ld = new JsonLd(CONTEXT_DATA, displayData, VOCAB_DATA)
        expect:
        def props = ld.displayData.lensGroups.chips.lenses.Thing.showProperties
        props == ['notation', 'label', 'labelByLang', 'note']
    }

    def "use vocab to match a subclass to a base class"() {
        given:
        def ld = new JsonLd(CONTEXT_DATA, [:], VOCAB_DATA)
        expect:
        ld.isSubClassOf('Publication', 'ProvisionActivity')
        !ld.isSubClassOf('Work', 'ProvisionActivity')
    }

    def "expand prefixes"() {
        given:
        Map context = ["ex": "http://example.org/ns/"]
        expect:
        JsonLd.expandLinks(['/path', 'ex:path', 'other:path'], context) ==
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

    def "should flatten framed jsonld"() {
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

    static String readFile(String filename) {
        return JsonLdSpec.class.getClassLoader()
            .getResourceAsStream(filename).getText("UTF-8")
    }

}
