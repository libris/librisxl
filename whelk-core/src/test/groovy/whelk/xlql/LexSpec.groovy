package whelk.xlql

import spock.lang.Specification
import whelk.xlql.Lex.LexerException

class LexSpec extends Specification {

    def "operator no whitspace"() {
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

    def "operator with whitspace"() {
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

}
