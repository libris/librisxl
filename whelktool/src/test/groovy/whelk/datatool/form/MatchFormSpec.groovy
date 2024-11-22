package whelk.datatool.form

import spock.lang.Specification

class MatchFormSpec extends Specification {
    static Map context = [
            '@vocab': 'https://id.kb.se/vocab/',
            'marc'  : 'https://id.kb.se/marc/',
            'p1'    : ['@container': '@set'],
            'p2'    : ['@container': '@set'],
            'p3'    : ['@container': '@list'],
            'marc:p': ['@container': '@set']
    ]

    def "match data against form"() {
        given:
        def matchForm = new MatchForm([:])
        matchForm.formBNodeIdToResourceIds = ["#1": ["https://libris.kb.se/x#it", "https://libris.kb.se/y#it"] as Set]
        matchForm.baseTypeToSubtypes = ["T": ["Tx", "Ty"] as Set]

        expect:
        matchForm.matches(form, node) == result

        where:
        form                                                                           | node                                        | result
        "a"                                                                            | "a"                                         | true
        "a"                                                                            | "b"                                         | false
        "a"                                                                            | ["a", "b"]                                  | true
        ["x": "a"]                                                                     | ["x": ["a", "b"]]                           | true
        ["x": "a", "bulk:matchingMode": ["bulk:Exact"]]                                | ["x": ["a", "b"]]                           | false
        ["x": ["a", "b"], "bulk:matchingMode": ["bulk:Exact"]]                         | ["x": ["a", "b"]]                           | true
        ["@type": "T", "bulk:matchingMode": ["bulk:Subtypes"], "a": "b"]               | ["@type": "Tx", "a": "b"]                   | true
        ["@type": "T", "a": "b"]                                                       | ["@type": "Tx", "a": "b"]                   | false
        ["@type": "T", "bulk:matchingMode": ["bulk:Subtypes", "bulk:Exact"], "a": "b"] | ["@type": "Ty", "a": "b"]                   | true
        ["@type": "T", "bulk:matchingMode": ["bulk:Subtypes", "bulk:Exact"], "a": "b"] | ["@type": "Ty", "a": "b", "c": "d"]         | false
        ["@type": "bulk:Any", "a": "b"]                                                | ["@type": "T", "a": "b", "c": "d"]          | true
        ["@type": "bulk:Any", "a": "b", "bulk:matchingMode": ["bulk:Exact"]]           | ["@type": "T", "a": "b", "c": "d"]          | false
        ["x": ["bulk:formBlankNodeId": "#1"]]                                          | ["x": ["@id": "https://libris.kb.se/y#it"]] | true
        ["x": ["bulk:formBlankNodeId": "#1"]]                                          | ["x": ["@id": "https://libris.kb.se/z#it"]] | false
    }

    def "form to sparql pattern: literal value"() {
        given:
        def form = ['bulk:formBlankNodeId': '#1', 'p1': 'x']
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 \"x\" ."

        expect:
        new MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: iri value"() {
        given:
        def form = ['bulk:formBlankNodeId': '#1', 'p1': ['@id': 'https://libris.kb.se/x']]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 <https://libris.kb.se/x> ."

        expect:
        new MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: marc property"() {
        given:
        def form = ['bulk:formBlankNodeId': '#1', 'marc:p': 'x']
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 marc:p \"x\" ."

        expect:
        new MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: null/empty value"() {
        given:
        def form = ['bulk:formBlankNodeId': '#1', 'p1': v]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 [ ] ."

        expect:
        new MatchForm(form).getSparqlPattern(context) == expectedPattern

        where:
        v << [null, [:], []]
    }

    def "form to sparql pattern: nested null/empty value"() {
        given:
        def form = ['bulk:formBlankNodeId': '#1', 'p1': ['bulk:formBlankNodeId': '#2', 'p2': v]]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 [ :p2 [ ] ] ."

        expect:
        new MatchForm(form).getSparqlPattern(context) == expectedPattern

        where:
        v << [null, [:], []]
    }

    def "form to sparql pattern: nested values"() {
        given:
        def form = ['bulk:formBlankNodeId': '#1', 'p1': ['bulk:formBlankNodeId': '#2', 'p2': ['@id': 'https://libris.kb.se/x'], 'marc:p': "x"]]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p1 [ :p2 <https://libris.kb.se/x> ;\n      marc:p \"x\" ] ."

        expect:
        new MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: ordered list value"() {
        given:
        def form = ['bulk:formBlankNodeId': '#1', 'p3': [['bulk:formBlankNodeId': '#2', 'p1': 'x'], ['bulk:formBlankNodeId': '#3', 'p2': 'y']]]
        def expectedPattern = "?graph :mainEntity ?1 .\n\n?1 :p3 ( [ :p1 \"x\" ] [ :p2 \"y\" ] ) ."

        expect:
        new MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: id mappings"() {
        given:
        def recordIds = ['@type': 'bulk:AnyOf', 'value': ['https://libris.kb.se/x', 'https://libris.kb.se/y',
                                                          'https://libris.kb.se/z']]
        def thingIds = ['@type': 'bulk:AnyOf', 'value': ['https://libris.kb.se/x#it', 'https://libris.kb.se/y#it',
                                                         'https://libris.kb.se/z#it']]
        def values = ['@type': 'bulk:AnyOf', 'value': ['https://id.kb.se/x', 'https://id.kb.se/y',
                                                       'https://id.kb.se/z#it']]

        def form = [
                'bulk:formBlankNodeId': '#1',
                'bulk:hasId'          : thingIds,
                'meta'                : ['bulk:formBlankNodeId': '#2', 'bulk:hasId': recordIds],
                'p1'                  : ['bulk:formBlankNodeId': '#3', 'bulk:hasId': values]
        ]

        def expectedPattern = "VALUES ?1 { <https://libris.kb.se/x#it> <https://libris.kb.se/y#it> <https://libris.kb.se/z#it> }\n" +
                "VALUES ?graph { <https://libris.kb.se/x> <https://libris.kb.se/y> <https://libris.kb.se/z> }\n" +
                "VALUES ?3 { <https://id.kb.se/x> <https://id.kb.se/y> <https://id.kb.se/z#it> }\n" +
                "?graph :mainEntity ?1 .\n" +
                "\n" +
                "?1 :p1 ?3 ."

        expect:
        new MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: unspecified types"() {
        given:
        def form = [
                'bulk:formBlankNodeId': '#1',
                '@type'               : 'bulk:Any',
                'p1'                  : ['bulk:formBlankNodeId': '#2', '@type': 'bulk:Any'],
                'p2'                  : ['bulk:formBlankNodeId': '#3', '@type': 'bulk:Any', 'p': 'v'],
                'marc:p'              : ['bulk:formBlankNodeId': '#4', '@type': 'marc:T', 'p': 'v']
        ]

        def expectedPattern = "?graph :mainEntity ?1 .\n" +
                "\n" +
                "?1 :p1 [ ] ;\n" +
                "  :p2 [ :p \"v\" ] ;\n" +
                "  marc:p [ a marc:T ;\n" +
                "      :p \"v\" ] ."

        expect:
        new MatchForm(form).getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: base types"() {
        given:
        def form = [
                'bulk:formBlankNodeId': '#1',
                '@type'               : 'T1',
                'bulk:matchingMode'   : ['bulk:Subtypes']
        ]

        def expectedPattern = "VALUES ?T1 { :T1 :T1x :T1y :T1z }\n" +
                "?graph :mainEntity ?1 .\n" +
                "\n" +
                "?1 a ?T1 ."

        def transform = new MatchForm(form)
        transform.baseTypeToSubtypes['T1'] = ['T1x', 'T1y', 'T1z'] as Set

        expect:
        transform.getSparqlPattern(context) == expectedPattern
    }

    def "form to sparql pattern: no subtypes to match"() {
        given:
        def form = [
                'bulk:formBlankNodeId': '#1',
                '@type'               : 'T1',
                'bulk:matchingMode'   : ['bulk:Subtypes']
        ]

        def expectedPattern = "?graph :mainEntity ?1 .\n" +
                "\n" +
                "?1 a :T1 ."

        def transform = new MatchForm(form)

        expect:
        transform.getSparqlPattern(context) == expectedPattern
    }
}
