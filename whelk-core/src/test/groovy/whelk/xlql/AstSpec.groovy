package whelk.xlql

import spock.lang.Specification
import whelk.exception.InvalidQueryException
import whelk.search2.parse.Ast
import whelk.search2.parse.Lex
import whelk.search2.parse.Parse

class AstSpec extends Specification {

    def "normal tree"() {
        given:
        def input = "AAA BBB and (CCC or DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        //System.err.println(ast)
        expect:
        ast == new Ast.And(
                [
                        new Ast.Leaf("AAA"),
                        new Ast.Leaf("BBB"),
                        new Ast.Or([new Ast.Leaf("CCC"), new Ast.Leaf("DDD")])
                ]
        )
    }

    def "normal query"() {
        given:
        def input = "subject: \"lcsh:Physics\" AND NOT published < 2023 AND \"svarta hål\""
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.And(
                [
                        new Ast.CodeEquals("subject", new Ast.Leaf("lcsh:Physics")),
                        new Ast.Not(new Ast.CodeLesserGreaterThan("published", "<", new Ast.Leaf("2023"))),
                        new Ast.Leaf("svarta hål")
                ]
        )
    }

    def "normal query2"() {
        given:
        def input = "subject: (\"lcsh:Physics\" OR Fysik) AND NOT published < 2023"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.And(
                [
                        new Ast.CodeEquals("subject", new Ast.Or([new Ast.Leaf("lcsh:Physics"), new Ast.Leaf("Fysik")])),
                        new Ast.Not(new Ast.CodeLesserGreaterThan("published", "<", new Ast.Leaf("2023")))
                ]
        )
    }

    def "codes and quotes"() {
        given:
        def input = "\"bf:subject\":\"lcsh:Physics\" AND \"bf:subject\""
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.And(
                [
                        new Ast.CodeEquals("bf:subject", new Ast.Leaf("lcsh:Physics")),
                        new Ast.Leaf("bf:subject")
                ]
        )
    }

    def "comparison"() {
        given:
        def input = "published >= 2000"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.CodeLesserGreaterThan("published", ">=", new Ast.Leaf("2000"))
    }

    def "comparison2"() {
        given:
        def input = "Pippi author=\"Astrid Lindgren\" published<=1970"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.And(
                [
                        new Ast.Leaf("Pippi"),
                        new Ast.CodeEquals("author", new Ast.Leaf("Astrid Lindgren")),
                        new Ast.CodeLesserGreaterThan("published", "<=", new Ast.Leaf("1970"))
                ]
        )
    }

    def "Fail code of code"() {
        given:
        def input = "AAA:(BBB:CCC)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)

        when:
        Ast.buildFrom(parseTree)
        then:
        thrown InvalidQueryException
    }
}
