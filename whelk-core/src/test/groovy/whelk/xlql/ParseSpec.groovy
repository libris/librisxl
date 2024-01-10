package whelk.xlql

import spock.lang.Specification;

class ParseSpec extends Specification {

    def "parse"() {
        given:
        def input = "A OR B OR C"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.parseQuery(lexedSymbols)

        expect:
        false
    }
}