package whelk.xlql

import spock.lang.Specification

class AstSpec extends Specification {

    def "normal tree"() {
        given:
        def input = "AAA BBB and (CCC or DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Object ast = Ast.buildFrom(parseTree)

        //System.err.println(ast)
        expect:
        ast == new Ast.And([new Ast.Or(["DDD", "CCC"]), "BBB", "AAA"])
    }


    def "normal query"() {
        given:
        def input = "subject: \"lcsh:Physics\" AND NOT published < 2023 AND \"svarta hål\""
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Object ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.And(
                [
                        "svarta hål",
                        new Ast.Not(new Ast.CodeLesserGreaterThan("published", "<", "2023")),
                        new Ast.CodeEquals("subject", "lcsh:Physics")
                ]
        )
    }

    def "normal query2"() {
        given:
        def input = "subject: (\"lcsh:Physics\" OR Fysik) AND NOT published < 2023"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Object ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.And(
                [
                        new Ast.Not(new Ast.CodeLesserGreaterThan("published", "<", "2023")),
                        new Ast.CodeEquals("subject", new Ast.Or(["Fysik", "lcsh:Physics"]))
                ]
        )
    }

}
