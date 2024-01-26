package whelk.xlql

import spock.lang.Specification
import whelk.Whelk

class QueryTreeSpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static QueryTree queryTree = new QueryTree(whelk)

    def "get domain of property"() {
        expect:
        queryTree.getDomain(property) == domain

        where:
        property       | domain
        "production"   | "Instance"
        "contentType"  | "Work"
        "bibliography" | "Record"
        "subject"      | null
        "publisher"    | "Instance"
        "isbn"         | "Resource"
    }

    def "collect given properties and types from query"() {
        given:
        def input = "type: Electronic AND utgivning: aaa AND (contentType: bbb OR title: ccc) AND bibliography: ddd"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Object ast = Ast.buildFrom(parseTree)
        Set<String> givenProperties = queryTree.collectGivenProperties(ast)
        Set<Object> givenTypes = queryTree.collectGivenTypes(ast)

        expect:
        givenProperties == ["@type", "publication", "contentType", "title", "bibliography"] as Set
        givenTypes == ["Electronic"] as Set
    }

    def "AST to QT: normal tree"() {
        given:
        def input = "AAA BBB and (CCC or DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Object ast = Ast.buildFrom(parseTree)
        Object qt = queryTree.astToQt(ast)

        expect:
        qt == ast
    }

    def "AST to QT: normal query"() {
        given:
        def input = "subject: \"sao:Fysik\" AND NOT tillkomsttid < 2023 AND \"svarta hål\""
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Object ast = Ast.buildFrom(parseTree)
        QueryTree.And qtTopLevel = queryTree.astToQt(ast)
        List<Object> conjuncts = qtTopLevel.conjuncts()

        expect:
        conjuncts[0] == "svarta hål"
                && ((QueryTree.Field) conjuncts[1]).with {
            it.path().stringify() == "originDate"
                    && it.operator() == QueryTree.Operator.GREATER_THAN_OR_EQUAL
                    && it.value() == "2023"
        }
                && ((QueryTree.Field) conjuncts[2]).with {
            it.path().stringify() == "subject.@id"
                    && it.operator() == QueryTree.Operator.EQUALS
                    && it.value() == "https://id.kb.se/term/sao/Fysik"
        }
    }
}
