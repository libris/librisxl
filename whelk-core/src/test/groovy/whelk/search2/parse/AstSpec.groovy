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

    def "Flatten code groups"() {
        given:
        def input = "AAA:(BBB AND CCC)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        Ast.flattenCodes(ast) == new Ast.And(
                [
                        new Ast.Code("AAA", Operator.EQUALS, new Ast.Leaf("BBB")),
                        new Ast.Code("AAA", Operator.EQUALS, new Ast.Leaf("CCC"))
                ]
        )
    }

    def "Flatten code groups2"() {
        given:
        def input = "author:(Alice AND (Bob OR Cecilia))"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        Ast.flattenCodes(ast) == new Ast.And(
                [
                        new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Alice")),
                        new Ast.Or([
                                new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Bob")),
                                new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Cecilia"))
                        ])
                ] as List<Ast.Node>
        )
    }

    def "Flatten code groups3"() {
        given:
        def input = "author:(Alice AND (Bob OR Cecilia) AND NOT David)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        Ast.flattenCodes(ast) == new Ast.And(
                [
                        new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Alice")),
                        new Ast.Or([
                                new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Bob")),
                                new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Cecilia"))
                        ]),
                        new Ast.Not(new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("David")))
                ] as List<Ast.Node>
        )
    }

    def "Flatten code groups4"() {
        given:
        def input = "\"everything\" OR author:(Alice AND (Bob OR Cecilia) AND NOT David)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        Ast.flattenCodes(ast) == new Ast.Or(
                [
                        new Ast.Leaf("everything"),
                        new Ast.And(
                                [
                                        new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Alice")),
                                        new Ast.Or([
                                                new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Bob")),
                                                new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("Cecilia"))
                                        ]),
                                        new Ast.Not(new Ast.Code("author", Operator.EQUALS, new Ast.Leaf("David")))
                                ] as List<Ast.Node>)
                ] as List<Ast.Node>
        )
    }

    def "flatten negations"() {
        given:
        def input = "\"everything\" AND NOT (author:Alice AND published > 2022)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.flattenCodes(Ast.buildFrom(parseTree))

        expect:
        Ast.flattenNegations(ast) == new Ast.And(
                [
                        new Ast.Leaf("everything"),
                        new Ast.Or(
                                [
                                        new Ast.Code("author", Operator.NOT_EQUALS, new Ast.Leaf("Alice")),
                                        new Ast.Code("published", Operator.LESS_THAN_OR_EQUALS, new Ast.Leaf("2022"))
                                ]
                        )
                ] as List<Ast.Node>
        )
    }

    def "flatten negations 2"() {
        given:
        def input = "\"everything\" AND !(author:Alice AND NOT published: 2022)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.flattenCodes(Ast.buildFrom(parseTree))

        expect:
        Ast.flattenNegations(ast) == new Ast.And(
                [
                        new Ast.Leaf("everything"),
                        new Ast.Or(
                                [
                                        new Ast.Code("author", Operator.NOT_EQUALS, new Ast.Leaf("Alice")),
                                        new Ast.Code("published", Operator.EQUALS, new Ast.Leaf("2022"))
                                ]
                        )
                ] as List<Ast.Node>
        )
    }

    def "flatten negations 3"() {
        given:
        def input = "!(author:Alice AND \"everything\" AND NOT \"something\")"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.flattenCodes(Ast.buildFrom(parseTree))

        expect:
        Ast.flattenNegations(ast) == new Ast.Or(
                [
                        new Ast.Code("author", Operator.NOT_EQUALS, new Ast.Leaf("Alice")),
                        new Ast.Not(new Ast.Leaf("everything")),
                        new Ast.Leaf("something")
                ] as List<Ast.Node>
        )
    }
}
