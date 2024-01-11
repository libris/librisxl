package whelk.xlql

import spock.lang.Specification;

class ParseSpec extends Specification {

    def "parse"() {
        given:
        //def input = "A OR B OR C"
        //def input = "AAA BBB CCC"
        //def input = "AAA BBB AND CCC"
        //def input = "AAA BBB and CCC or DDD"
        def input = "AAA BBB and (CCC or DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.parseQuery(lexedSymbols)

        expect:
        false
    }
}