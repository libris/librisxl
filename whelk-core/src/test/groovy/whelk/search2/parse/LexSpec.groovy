package whelk.search2.parse

import spock.lang.Specification
import whelk.exception.InvalidQueryException
import whelk.search2.parse.Lex

class LexSpec extends Specification {

    def "operator no whitespace"() {
        given:
        def input = "AAA:BBB"
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA", 0),
                new Lex.Symbol(Lex.TokenName.OPERATOR, ":", 3),
                new Lex.Symbol(Lex.TokenName.STRING, "BBB", 4),
        ]
    }

    def "operator with whitespace"() {
        given:
        def input = "AAA = BBB"
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA", 0),
                new Lex.Symbol(Lex.TokenName.OPERATOR, "=", 4),
                new Lex.Symbol(Lex.TokenName.STRING, "BBB", 6),
        ]
    }

    def "outer escape"() {
        given:
        def input = "AAA\\=BBB"
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA=BBB", 0)
        ]
    }

    def "escaped white space separation"() {
        given:
        def input = "AAA\\ BBB"
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA BBB", 0)
        ]
    }

    def "error on escaped eol"() {
        given:
        def input = "AAA\\"

        when:
        Lex.lexQuery(input)

        then:
        thrown InvalidQueryException
    }

    def "error on escaped eol2"() {
        given:
        def input = "AAA\"\\"

        when:
        Lex.lexQuery(input)

        then:
        thrown InvalidQueryException
    }

    def "normal lex"() {
        given:
        def input = "published:(2022)"
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "published", 0),
                new Lex.Symbol(Lex.TokenName.OPERATOR, ":", 9),
                new Lex.Symbol(Lex.TokenName.OPERATOR, "(", 10),
                new Lex.Symbol(Lex.TokenName.STRING, "2022", 11),
                new Lex.Symbol(Lex.TokenName.OPERATOR, ")", 15),
        ]
    }

    def "escaped quote in quoted string"() {
        given:
        def input = "AAA:\"BB\\\"B\""
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA", 0),
                new Lex.Symbol(Lex.TokenName.OPERATOR, ":", 3),
                new Lex.Symbol(Lex.TokenName.STRING, "BB\"B", 4),
        ]
    }

    def "escaped escape in quoted string"() {
        given:
        def input = "AAA:\"BB\\\\B\""
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA", 0),
                new Lex.Symbol(Lex.TokenName.OPERATOR, ":", 3),
                new Lex.Symbol(Lex.TokenName.STRING, "BB\\B", 4),
        ]
    }

    def "two-char operators"() {
        given:
        def input = "AAA<=BBB"
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA", 0),
                new Lex.Symbol(Lex.TokenName.OPERATOR, "<=", 3),
                new Lex.Symbol(Lex.TokenName.STRING, "BBB", 5),
        ]
    }

    def "two-char operators with whitespace"() {
        given:
        def input = "AAA <= BBB"
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA", 0),
                new Lex.Symbol(Lex.TokenName.OPERATOR, "<=", 4),
                new Lex.Symbol(Lex.TokenName.STRING, "BBB", 7),
        ]
    }

    def "two-char operators with whitespace 2"() {
        given:
        def input = "AAA<= BBB"
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA", 0),
                new Lex.Symbol(Lex.TokenName.OPERATOR, "<=", 3),
                new Lex.Symbol(Lex.TokenName.STRING, "BBB", 6),
        ]
    }

    def "two-char operators with whitespace 3"() {
        given:
        def input = "AAA <=BBB"
        def lexedSymbols = Lex.lexQuery(input)

        expect:
        lexedSymbols as List == [
                new Lex.Symbol(Lex.TokenName.STRING, "AAA", 0),
                new Lex.Symbol(Lex.TokenName.OPERATOR, "<=", 4),
                new Lex.Symbol(Lex.TokenName.STRING, "BBB", 6),
        ]
    }

}
