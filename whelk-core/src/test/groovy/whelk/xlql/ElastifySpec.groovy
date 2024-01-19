package whelk.xlql

import spock.lang.Specification
import whelk.Whelk

class ElastifySpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static Elastify elastify = new Elastify(whelk)

    def "get domain of property"() {
        expect:
        elastify.getDomain(property) == domain

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
        Set<String> givenProperties = elastify.collectGivenProperties(ast)
        Set<Object> givenTypes = elastify.collectGivenTypes(ast)

        expect:
        givenProperties == ["@type", "publication", "contentType", "title", "bibliography"] as Set
        givenTypes == ["Electronic"] as Set
    }

    def "Reduce to AND/OR tree: normal tree"() {
        given:
        def input = "AAA BBB and (CCC or DDD)"
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Object ast = Ast.buildFrom(parseTree)
        Object andOrReduced = elastify.reduceToAndOrTree(ast)

        expect:
        andOrReduced == ast
    }

    def "Reduce to AND/OR tree: normal query"() {
        given:
        def input = "subject: \"sao:Fysik\" AND NOT originDate < 2023 AND \"svarta hål\""
        def lexedSymbols = Lex.lexQuery(input)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Object ast = Ast.buildFrom(parseTree)
        Object andOrReduced = elastify.reduceToAndOrTree(ast)

        expect:
        andOrReduced == new Ast.And(
                [
                        "svarta hål",
                        new Elastify.ElastifiedField("min-originDate", "2023"),
                        new Elastify.ElastifiedField("subject.@id", "https://id.kb.se/term/sao/Fysik")
                ]
        )
    }
}
