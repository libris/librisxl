package whelk.search2.parse

import spock.lang.Specification
import whelk.exception.InvalidQueryException
import whelk.search2.Operator

class AstSpec extends Specification {

    def "normal tree"() {
        given:
        def input = "AAA BBB AND (CCC OR DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.And(
                [
                        new Ast.Leaf("AAA"),
                        new Ast.Leaf("BBB"),
                        new Ast.Or([new Ast.Leaf("CCC"), new Ast.Leaf("DDD")])
                ] as List<Ast.Node>
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
                        new Ast.Code("subject", Operator.EQUALS, new Ast.Leaf("lcsh:Physics")),
                        new Ast.Not(new Ast.Code("published", Operator.LESS_THAN, new Ast.Leaf("2023"))),
                        new Ast.Leaf("svarta hål")
                ] as List<Ast.Node>
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
                        new Ast.Code("subject", Operator.EQUALS, new Ast.Or([new Ast.Leaf("lcsh:Physics"), new Ast.Leaf("Fysik")])),
                        new Ast.Not(new Ast.Code("published", Operator.LESS_THAN, new Ast.Leaf("2023")))
                ] as List<Ast.Node>
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
                        new Ast.Code("bf:subject", Operator.EQUALS, new Ast.Leaf("lcsh:Physics")),
                        new Ast.Leaf("bf:subject")
                ] as List<Ast.Node>
        )
    }

    def "comparison"() {
        given:
        def input = "published >= 2000"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.Code("published", Operator.GREATER_THAN_OR_EQUALS, new Ast.Leaf("2000"))
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
                        new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Astrid Lindgren")),
                        new Ast.Code("published", Operator.LESS_THAN_OR_EQUALS, new Ast.Leaf("1970"))
                ] as List<Ast.Node>
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
