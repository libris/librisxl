package whelk.search2.parse

import spock.lang.Specification
import whelk.exception.InvalidQueryException
import whelk.search2.Operator

import static whelk.search2.parse.Lex.TokenName.OPERATOR
import static whelk.search2.parse.Lex.TokenName.QUOTED_STRING
import static whelk.search2.parse.Lex.TokenName.STRING

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
                        new Ast.Leaf(new Lex.Symbol(STRING, "AAA", 0)),
                        new Ast.Leaf(new Lex.Symbol(STRING, "BBB", 4)),
                        new Ast.Or([new Ast.Leaf(new Lex.Symbol(STRING, "CCC", 13)),
                                    new Ast.Leaf(new Lex.Symbol(STRING, "DDD", 20))])
                ] as List<Ast.Node>
        )
    }

    def "normal tree swedish"() {
        given:
        def input = "AAA BBB OCH (CCC ELLER DDD) INTE EEE"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.And(
                [
                        new Ast.Leaf(new Lex.Symbol(STRING, "AAA", 0)),
                        new Ast.Leaf(new Lex.Symbol(STRING, "BBB", 4)),
                        new Ast.Or([new Ast.Leaf(new Lex.Symbol(STRING, "CCC", 13)),
                                    new Ast.Leaf(new Lex.Symbol(STRING, "DDD", 23))]),
                        new Ast.Not(new Ast.Leaf(new Lex.Symbol(STRING, "EEE", 33)))
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
                        new Ast.Code(new Lex.Symbol(STRING, "subject", 0),
                                Operator.EQUALS,
                                new Ast.Leaf(new Lex.Symbol(QUOTED_STRING, "lcsh:Physics", 9))),
                        new Ast.Not(new Ast.Code(new Lex.Symbol(STRING, "published", 32),
                                Operator.LESS_THAN,
                                new Ast.Leaf(new Lex.Symbol(STRING, "2023", 44)))),
                        new Ast.Leaf(new Lex.Symbol(QUOTED_STRING, "svarta hål", 53))
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
                        new Ast.Code(new Lex.Symbol(STRING, "subject", 0),
                                Operator.EQUALS,
                                new Ast.Or([new Ast.Leaf(new Lex.Symbol(QUOTED_STRING, "lcsh:Physics", 10)),
                                            new Ast.Leaf(new Lex.Symbol(STRING, "Fysik", 28))])),
                        new Ast.Not(new Ast.Code(new Lex.Symbol(STRING, "published", 43),
                                Operator.LESS_THAN,
                                new Ast.Leaf(new Lex.Symbol(STRING, "2023", 55))))
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
                        new Ast.Code(new Lex.Symbol(QUOTED_STRING, "bf:subject", 0),
                                Operator.EQUALS,
                                new Ast.Leaf(new Lex.Symbol(QUOTED_STRING, "lcsh:Physics", 13))),
                        new Ast.Leaf(new Lex.Symbol(QUOTED_STRING, "bf:subject", 32))
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
        ast == new Ast.Code(new Lex.Symbol(STRING, "published", 0),
                Operator.GREATER_THAN_OR_EQUALS,
                new Ast.Leaf(new Lex.Symbol(STRING, "2000", 13)))
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
                        new Ast.Leaf(new Lex.Symbol(STRING, "Pippi", 0)),
                        new Ast.Code(new Lex.Symbol(STRING, "author", 6),
                                Operator.EQUALS,
                                new Ast.Leaf(new Lex.Symbol(QUOTED_STRING, "Astrid Lindgren", 13))),
                        new Ast.Code(new Lex.Symbol(STRING, "published", 31),
                                Operator.LESS_THAN_OR_EQUALS,
                                new Ast.Leaf(new Lex.Symbol(STRING, "1970", 42)))
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

    def "empty group as string"() {
        given:
        def input = "AAA OR ()"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.Or(
                [
                        new Ast.Leaf(new Lex.Symbol(STRING, "AAA", 0)),
                        new Ast.And([])
                ] as List<Ast.Node>
        )
    }

    def "empty group as string2"() {
        given:
        def input = "AAA OR (())"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.Or(
                [
                        new Ast.Leaf(new Lex.Symbol(STRING, "AAA", 0)),
                        new Ast.And([])
                ] as List<Ast.Node>
        )
    }

    def "empty group as string3"() {
        given:
        def input = "()"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        ast == new Ast.And([])

    }
}
