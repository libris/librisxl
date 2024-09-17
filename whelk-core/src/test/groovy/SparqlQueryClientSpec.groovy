import spock.lang.Specification
import whelk.component.SparqlQueryClient

class SparqlQueryClientSpec extends Specification {
    static Map context = [
            '@vocab': 'https://id.kb.se/vocab/',
            'marc'  : 'https://id.kb.se/marc/',
            'p1'    : ['@container': '@set'],
            'p2'    : ['@container': '@set'],
            'p3'    : ['@container': '@list'],
            'marc:p': ['@container': '@set']
    ]

    def "form to graph pattern: literal value"() {
        given:
        def form = ['p1': 'x']
        def expectedPattern = "?graph :mainEntity ?mainEntity .\n\n?mainEntity :p1 \"x\" ."

        expect:
        SparqlQueryClient.sparqlify(form, context) == expectedPattern
    }

    def "form to graph pattern: iri value"() {
        given:
        def form = ['p1': ['@id': 'https://libris.kb.se/x']]
        def expectedPattern = "?graph :mainEntity ?mainEntity .\n\n?mainEntity :p1 <https://libris.kb.se/x> ."

        expect:
        SparqlQueryClient.sparqlify(form, context) == expectedPattern
    }

    def "form to graph pattern: marc property"() {
        given:
        def form = ['marc:p': 'x']
        def expectedPattern = "?graph :mainEntity ?mainEntity .\n\n?mainEntity marc:p \"x\" ."

        expect:
        SparqlQueryClient.sparqlify(form, context) == expectedPattern
    }

    def "form to graph pattern: null/empty value"() {
        given:
        def form = ['p1': v]
        def expectedPattern = "?graph :mainEntity ?mainEntity .\n\n?mainEntity :p1 [] ."

        expect:
        SparqlQueryClient.sparqlify(form, context) == expectedPattern

        where:
        v << [null, "", [:], []]
    }

    def "form to graph pattern: nested null/empty value"() {
        given:
        def form = ['p1': ['p2': v]]
        def expectedPattern = "?graph :mainEntity ?mainEntity .\n\n?mainEntity :p1 [ :p2 [] ] ."

        expect:
        SparqlQueryClient.sparqlify(form, context) == expectedPattern

        where:
        v << [null, "", [:], []]
    }

    def "form to graph pattern: nested values"() {
        given:
        def form = ['p1': ['p2': ['@id': 'https://libris.kb.se/x'], 'marc:p': "x"]]
        def expectedPattern = "?graph :mainEntity ?mainEntity .\n" +
                "\n" +
                "?mainEntity :p1 [ :p2 <https://libris.kb.se/x> ;\n" +
                "      marc:p \"x\" ] ."

        expect:
        SparqlQueryClient.sparqlify(form, context) == expectedPattern
    }

    def "form to graph pattern: ordered list value"() {
        given:
        def form = ['p3': [['p1': 'x'], ['p2': 'y']]]
        def expectedPattern = "?graph :mainEntity ?mainEntity .\n" +
                "\n" +
                "?mainEntity :p3 ( ( [ :p1 \"x\" ] ) ( [ :p2 \"y\" ] ) ) ."

        expect:
        SparqlQueryClient.sparqlify(form, context) == expectedPattern
    }
}
