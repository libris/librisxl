package whelk.datatool.form

import spock.lang.Specification

import static whelk.util.Jackson.mapper

class TransformSpec extends Specification {
    static List<Map> specs = TransformSpec.class.getClassLoader()
            .getResourceAsStream('whelk/datatool/form/specs.json')
            .with { mapper.readValue((InputStream) it, Map)['specs'] }
    static Map context = [
            '@vocab': 'https://id.kb.se/vocab/',
            'marc'  : 'https://id.kb.se/marc/',
            'p1'    : ['@container': '@set'],
            'p2'    : ['@container': '@set'],
            'p3'    : ['@container': '@list'],
            'marc:p': ['@container': '@set']
    ]

    def "collect changed paths"() {
        given:
        def transform = new Transform((Map) spec["matchForm"], (Map) spec["targetForm"])
        def addedPaths = spec["addedPaths"]
        def removedPaths = spec["removedPaths"]

        expect:
        transform.addedPaths == addedPaths
        transform.removedPaths == removedPaths

        where:
        spec << specs.findAll { (it["addedPaths"] || it["removedPaths"]) && !it['shouldFailWithException'] }
    }

    def "form to sparql pattern: literal value"() {
        given:
        def form = ['_id': '#1', 'p1': 'x']
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 \"x\" ."

        expect:
        new Transform.MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: iri value"() {
        given:
        def form = ['_id': '#1', 'p1': ['@id': 'https://libris.kb.se/x']]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 <https://libris.kb.se/x> ."

        expect:
        new Transform.MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: marc property"() {
        given:
        def form = ['_id': '#1', 'marc:p': 'x']
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 marc:p \"x\" ."

        expect:
        new Transform.MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: null/empty value"() {
        given:
        def form = ['_id': '#1', 'p1': v]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 [] ."

        expect:
        new Transform.MatchForm(form).getSparqlPattern(context) == expectedPattern

        where:
        v << [null, [:], []]
    }

    def "form to sparql pattern: nested null/empty value"() {
        given:
        def form = ['_id': '#1', 'p1': ['p2': v]]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 [ :p2 [] ] ."

        expect:
        new Transform.MatchForm(form).getSparqlPattern(context) == expectedPattern

        where:
        v << [null, [:], []]
    }

    def "form to sparql pattern: nested values"() {
        given:
        def form = ['_id': '#1', 'p1': ['_id': '#2', 'p2': ['@id': 'https://libris.kb.se/x'], 'marc:p': "x"]]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 [ :p2 <https://libris.kb.se/x> ;\n      marc:p \"x\" ] ."

        expect:
        new Transform.MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: ordered list value"() {
        given:
        def form = ['_id': '#1', 'p3': [['_id': '#2', 'p1': 'x'], ['_id': '#3', 'p2': 'y']]]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p3 ( ( [ :p1 \"x\" ] ) ( [ :p2 \"y\" ] ) ) ."

        expect:
        new Transform.MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: id mappings"() {
        given:
        def recordIds = ['@type': 'AnyOf', 'value': ['https://libris.kb.se/x', 'https://libris.kb.se/y',
                                                     'https://libris.kb.se/z']]
        def thingIds = ['@type': 'AnyOf', 'value': ['https://libris.kb.se/x#it', 'https://libris.kb.se/y#it',
                                                    'https://libris.kb.se/z#it']]
        def values = ['@type': 'AnyOf', 'value': ['https://id.kb.se/x', 'https://id.kb.se/y',
                                                  'https://id.kb.se/z#it']]

        def form = [
                '_id'    : '#1',
                '_idList': thingIds,
                'meta'   : ['_id': '#2', '_idList': recordIds],
                'p1'     : ['_id': '#3', '_idList': values]
        ]

        def expectedPattern = "VALUES ?1 { <https://libris.kb.se/x#it> <https://libris.kb.se/y#it> <https://libris.kb.se/z#it> }\n" +
                "VALUES ?graph { <https://libris.kb.se/x> <https://libris.kb.se/y> <https://libris.kb.se/z> }\n" +
                "VALUES ?3 { <https://id.kb.se/x> <https://id.kb.se/y> <https://id.kb.se/z#it> }\n" +
                "?graph :mainEntity ?1 .\n" +
                "\n" +
                "?1 :p1 ?3 ."

        expect:
        new Transform.MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: unspecified types"() {
        given:
        def form = [
                '_id'  : '#1',
                '@type': 'Any',
                'p1'   : ['_id': '#2', '@type': 'Any'],
                'p2'   : ['_id': '#3', '@type': 'Any', 'p': 'v'],
                'marc:p'   : ['_id': '#4', '@type': 'marc:T', 'p': 'v']
        ]

        def expectedPattern = "?graph :mainEntity ?1 .\n" +
                "\n" +
                "?1 :p1 [] ;\n" +
                "  :p2 [ :p \"v\" ] ;\n" +
                "  marc:p [ a marc:T ;\n" +
                "      :p \"v\" ] ."

        expect:
        new Transform.MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "is equal"() {
        given:
        def a = ["p": ["x": "y"]]
        def b = ["p": ["@type": "t1", "x": "y"]]
        def c = ["p": ["@type": "t2", "x": "y"]]

        expect:
        Transform.isEqual(a, b)
        Transform.isEqual(b, a)
        Transform.isEqual(a, c)
        !Transform.isEqual(b, c)
        Transform.isEqual(["p": [["a": "b"], a]], ["p": [a, ["a": "b"]]])
        Transform.isEqual(["p": [["a":"b"], a]], ["p": [b, ["a":"b"]]])
        !Transform.isEqual(["p": [["a":"b"], c]], ["p": [b, ["a":"b"]]])
    }
}