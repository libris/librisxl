package whelk.xlql

import spock.lang.Specification
import whelk.Whelk
import whelk.exception.InvalidQueryException

class SimpleQueryTreeSpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static Disambiguate disambiguate = new Disambiguate(whelk)

    private SimpleQueryTree getTree(String queryString) {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast ast = new Ast(parseTree)
        FlattenedAst flattened = new FlattenedAst(ast)
        return new SimpleQueryTree(flattened, disambiguate)
    }

    def "free text tree"() {
        given:
        SimpleQueryTree sqt = getTree("AAA BBB and (CCC or DDD)")

        expect:
        sqt.tree == new SimpleQueryTree.And(
                [
                        new SimpleQueryTree.FreeText(Operator.EQUALS, "AAA"),
                        new SimpleQueryTree.FreeText(Operator.EQUALS, "BBB"),
                        new SimpleQueryTree.Or(
                                [
                                        new SimpleQueryTree.FreeText(Operator.EQUALS, "CCC"),
                                        new SimpleQueryTree.FreeText(Operator.EQUALS, "DDD")
                                ]
                        )
                ]
        )
    }

    def "aliased property + free text"() {
        given:
        SimpleQueryTree sqt = getTree("ämne: \"lcsh:Physics\" AND \"svarta hål\"")

        expect:
        sqt.tree == new SimpleQueryTree.And(
                [
                        new SimpleQueryTree.PropertyValue("subject", ["subject"], Operator.EQUALS, "lcsh:Physics"),
                        new SimpleQueryTree.FreeText(Operator.EQUALS, "svarta hål")
                ]
        )
    }

    def "mapped property iri + free text"() {
        given:
        SimpleQueryTree sqt = getTree("\"bf:originDate\" < 2023 OR \"svarta hål\"")

        expect:
        sqt.tree == new SimpleQueryTree.Or(
                [
                        new SimpleQueryTree.PropertyValue("originDate", ["originDate"] ,Operator.LESS_THAN, "2023"),
                        new SimpleQueryTree.FreeText(Operator.EQUALS, "svarta hål")
                ]
        )
    }

    def "grouping"() {
        given:
        def query = "upphovsuppgift:(Tolkien OR Verne) AND (genre/form: Fantasy OR genre/form: Äventyr)"
        SimpleQueryTree sqt = getTree(query)

        expect:
        sqt.tree == new SimpleQueryTree.And(
                [
                        new SimpleQueryTree.Or(
                                [
                                        new SimpleQueryTree.PropertyValue("responsibilityStatement", ["responsibilityStatement"], Operator.EQUALS, "Tolkien"),
                                        new SimpleQueryTree.PropertyValue("responsibilityStatement", ["responsibilityStatement"], Operator.EQUALS, "Verne"),
                                ]
                        ),
                        new SimpleQueryTree.Or(
                                [
                                        new SimpleQueryTree.PropertyValue("genreForm", ["genreForm"], Operator.EQUALS, "Fantasy"),
                                        new SimpleQueryTree.PropertyValue("genreForm", ["genreForm"], Operator.EQUALS, "Äventyr")
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
        SimpleQueryTree sqt = getTree(query)

        expect:
        sqt.tree == new SimpleQueryTree.PropertyValue("subject", ["instanceOf", "subject", "@id"], Operator.EQUALS, "sao:Hästar")
    }
}
