package whelk.xlql

import spock.lang.Specification

class FlattenedAstSpec extends Specification {

    def "Flatten code groups"() {
        given:
        def input = "AAA:(BBB and CCC)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        FlattenedAst.flattenCodes(ast) == new Ast.And(
                [
                        new Ast.CodeEqualsLeaf("AAA", new Ast.Leaf("BBB")),
                        new Ast.CodeEqualsLeaf("AAA", new Ast.Leaf("CCC"))
                ]
        )
    }

    def "Flatten code groups2"() {
        given:
        def input = "author:(Alice and (Bob or Cecilia))"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        FlattenedAst.flattenCodes(ast) == new Ast.And(
                [
                        new Ast.CodeEqualsLeaf("author", new Ast.Leaf("Alice")),
                        new Ast.Or([
                                new Ast.CodeEqualsLeaf("author", new Ast.Leaf("Bob")),
                                new Ast.CodeEqualsLeaf("author", new Ast.Leaf("Cecilia"))
                        ])
                ] as List<Ast.Node>
        )
    }

    def "Flatten code groups3"() {
        given:
        def input = "author:(Alice and (Bob or Cecilia) and not David)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        FlattenedAst.flattenCodes(ast) == new Ast.And(
                [
                        new Ast.CodeEqualsLeaf("author", new Ast.Leaf("Alice")),
                        new Ast.Or([
                                new Ast.CodeEqualsLeaf("author", new Ast.Leaf("Bob")),
                                new Ast.CodeEqualsLeaf("author", new Ast.Leaf("Cecilia"))
                        ]),
                        new Ast.Not(new Ast.CodeEqualsLeaf("author", new Ast.Leaf("David")))
                ] as List<Ast.Node>
        )
    }

    def "Flatten code groups4"() {
        given:
        def input = "\"everything\" or author:(Alice and (Bob or Cecilia) and not David)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = Ast.buildFrom(parseTree)

        expect:
        FlattenedAst.flattenCodes(ast) == new Ast.Or(
                [
                        new Ast.Leaf("everything"),
                        new Ast.And(
                                [
                                        new Ast.CodeEqualsLeaf("author", new Ast.Leaf("Alice")),
                                        new Ast.Or([
                                                new Ast.CodeEqualsLeaf("author", new Ast.Leaf("Bob")),
                                                new Ast.CodeEqualsLeaf("author", new Ast.Leaf("Cecilia"))
                                        ]),
                                        new Ast.Not(new Ast.CodeEqualsLeaf("author", new Ast.Leaf("David")))
                                ] as List<Ast.Node>)
                ] as List<Ast.Node>
        )
    }

    def "flatten negations"() {
        given:
        def input = "\"everything\" and not (author:Alice and published > 2022)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = FlattenedAst.flattenCodes(Ast.buildFrom(parseTree))

        expect:
        FlattenedAst.flattenNegations(ast) == new FlattenedAst.And(
                [
                        new FlattenedAst.Leaf("everything"),
                        new FlattenedAst.Or(
                                [
                                        new FlattenedAst.Code("author", Operator.NOT_EQUALS, "Alice"),
                                        new FlattenedAst.Code("published", Operator.LESS_THAN_OR_EQUALS, "2022")
                                ]
                        )
                ] as List<FlattenedAst.Node>
        )
    }

    def "flatten negations 2"() {
        given:
        def input = "\"everything\" and !(author:Alice and not published: 2022)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = FlattenedAst.flattenCodes(Ast.buildFrom(parseTree))

        expect:
        FlattenedAst.flattenNegations(ast) == new FlattenedAst.And(
                [
                        new FlattenedAst.Leaf("everything"),
                        new FlattenedAst.Or(
                                [
                                        new FlattenedAst.Code("author", Operator.NOT_EQUALS, "Alice"),
                                        new FlattenedAst.Code("published", Operator.EQUALS, "2022")
                                ]
                        )
                ] as List<FlattenedAst.Node>
        )
    }

    def "flatten negations 3"() {
        given:
        def input = "!(author:Alice and \"everything\" and not \"something\")"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast.Node ast = FlattenedAst.flattenCodes(Ast.buildFrom(parseTree))

        expect:
        FlattenedAst.flattenNegations(ast) == new FlattenedAst.Or(
                [
                        new FlattenedAst.Code("author", Operator.NOT_EQUALS, "Alice"),
                        new FlattenedAst.Not("everything"),
                        new FlattenedAst.Leaf("something")
                ] as List<FlattenedAst.Node>
        )
    }
}
