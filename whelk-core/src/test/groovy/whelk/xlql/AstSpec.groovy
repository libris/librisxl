package whelk.xlql

import spock.lang.Specification

class AstSpec extends Specification {

    def "normal tree"() {
        given:
        def input = "AAA BBB and (CCC or DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Object ast = Ast.buildFrom(parseTree)

        System.err.println(ast)

        expect:
        ast != null
    }

}
