package whelk.sru.cql

import spock.lang.Specification
import whelk.exception.InvalidQueryException
import whelk.sru.cql.Lex

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
}
