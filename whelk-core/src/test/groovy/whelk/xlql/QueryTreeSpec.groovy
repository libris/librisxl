package whelk.xlql

import spock.lang.Ignore
import spock.lang.Specification
import whelk.Whelk
import whelk.exception.InvalidQueryException
import whelk.search2.Disambiguate
import whelk.search2.querytree.QueryTree
import whelk.search2.parse.Ast
import whelk.search2.parse.FlattenedAst
import whelk.search2.parse.Lex
import whelk.search2.Operator
import whelk.search2.parse.Parse

@Ignore
class QueryTreeSpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static Disambiguate disambiguate = new Disambiguate(whelk)

    private QueryTree getTree(String queryString) {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast ast = new Ast(parseTree)
        FlattenedAst flattened = new FlattenedAst(ast)
        return new QueryTree(flattened, disambiguate)
    }

    def "free text tree"() {
        given:
        QueryTree sqt = getTree("AAA BBB and (CCC or DDD)")

        expect:
        sqt.tree == new QueryTree.And(
                [
                        new QueryTree.FreeText(Operator.EQUALS, "AAA BBB"),
                        new QueryTree.Or(
                                [
                                        new QueryTree.FreeText(Operator.EQUALS, "CCC"),
                                        new QueryTree.FreeText(Operator.EQUALS, "DDD")
                                ]
                        )
                ] as List<QueryTree.Node>
        )
    }

    def "aliased property + free text"() {
        given:
        QueryTree sqt = getTree("ämne: \"lcsh:Physics\" AND \"svarta hål\"")

        expect:
        sqt.tree == new QueryTree.And(
                [
                        new QueryTree.PropertyValue("subject", ["subject"], Operator.EQUALS, "lcsh:Physics"),
                        new QueryTree.FreeText(Operator.EQUALS, "\"svarta hål\"")
                ] as List<QueryTree.Node>
        )
    }

    def "mapped property iri + free text"() {
        given:
        QueryTree sqt = getTree("\"bf:originDate\" < 2023 OR \"svarta hål\"")

        expect:
        sqt.tree == new QueryTree.Or(
                [
                        new QueryTree.PropertyValue("originDate", ["originDate"], Operator.LESS_THAN, "2023"),
                        new QueryTree.FreeText(Operator.EQUALS, "\"svarta hål\"")
                ] as List<QueryTree.Node>
        )
    }

    def "grouping"() {
        given:
        def query = "upphovsuppgift:(Tolkien OR Verne) AND (genre/form: Fantasy OR genre/form: Äventyr)"
        QueryTree sqt = getTree(query)

        expect:
        sqt.tree == new QueryTree.And(
                [
                        new QueryTree.Or(
                                [
                                        new QueryTree.PropertyValue("responsibilityStatement", ["responsibilityStatement"], Operator.EQUALS, "Tolkien"),
                                        new QueryTree.PropertyValue("responsibilityStatement", ["responsibilityStatement"], Operator.EQUALS, "Verne"),
                                ]
                        ),
                        new QueryTree.Or(
                                [
                                        new QueryTree.PropertyValue("genreForm", ["genreForm"], Operator.EQUALS, "Fantasy"),
                                        new QueryTree.PropertyValue("genreForm", ["genreForm"], Operator.EQUALS, "Äventyr")
                                ]
                        )
                ]
        )
    }

    def "fail when unknown property alias"() {
        when:
        getTree("AAA and unknownAlias: BBB")

        then:
        thrown(InvalidQueryException)
    }

    def "property path"() {
        def query = "instanceOf.ämne.@id=\"sao:Hästar\""
        QueryTree sqt = getTree(query)

        expect:
        sqt.tree == new QueryTree.PropertyValue("subject", ["instanceOf", "subject", "@id"], Operator.EQUALS, "sao:Hästar")
    }

    def "disambiguate type"() {
        def query = "typ: Tryck"
        QueryTree sqt = getTree(query)

        expect:
        sqt.tree == new QueryTree.PropertyValue("rdf:type", ["rdf:type"], Operator.EQUALS, "Print")
    }

    def "unrecognized type"() {
        when:
        getTree("type: UnknownType")

        then:
        thrown(InvalidQueryException)
    }

    def "disambiguate enum"() {
        def query = "utgivningssätt: \"Seriell resurs\""
        QueryTree sqt = getTree(query)

        expect:
        sqt.tree == new QueryTree.PropertyValue("issuanceType", ["issuanceType"], Operator.EQUALS, "Serial")
    }

    def "unrecognized enum"() {
        when:
        getTree("utgivningssätt: \"Tryck\"")

        then:
        thrown(InvalidQueryException)
    }

    def "collect given types from query"() {
        given:
        def input = "type: (Electronic OR Print) AND utgivning: aaa"
        QueryTree sqt = getTree(input)
        Set<Object> givenTypes = sqt.collectGivenTypes()

        expect:
        givenTypes == ["Electronic", "Print"] as Set
    }

    def "normalize free text"() {
        given:
        def input = "x y z \"a b c\" d label:l \"e:f\" not g h i"
        def sqt = getTree(input)

        expect:
        sqt.tree == new QueryTree.And(
                [
                        new QueryTree.FreeText(Operator.EQUALS, "x y z \"a b c\" d \"e:f\" h i"),
                        QueryTree.pvEqualsLiteral("label", "l"),
                        new QueryTree.FreeText(Operator.NOT_EQUALS, "g"),

                ] as List<QueryTree.Node>
        )
    }
}
