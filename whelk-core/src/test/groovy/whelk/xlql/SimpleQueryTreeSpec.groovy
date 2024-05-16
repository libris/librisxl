package whelk.xlql

import spock.lang.Ignore
import spock.lang.Specification
import whelk.Whelk
import whelk.exception.InvalidQueryException

@Ignore
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
                        new SimpleQueryTree.FreeText(Operator.EQUALS, "AAA BBB"),
                        new SimpleQueryTree.Or(
                                [
                                        new SimpleQueryTree.FreeText(Operator.EQUALS, "CCC"),
                                        new SimpleQueryTree.FreeText(Operator.EQUALS, "DDD")
                                ]
                        )
                ] as List<SimpleQueryTree.Node>
        )
    }

    def "aliased property + free text"() {
        given:
        SimpleQueryTree sqt = getTree("ämne: \"lcsh:Physics\" AND \"svarta hål\"")

        expect:
        sqt.tree == new SimpleQueryTree.And(
                [
                        new SimpleQueryTree.PropertyValue("subject", ["subject"], Operator.EQUALS, "lcsh:Physics"),
                        new SimpleQueryTree.FreeText(Operator.EQUALS, "\"svarta hål\"")
                ] as List<SimpleQueryTree.Node>
        )
    }

    def "mapped property iri + free text"() {
        given:
        SimpleQueryTree sqt = getTree("\"bf:originDate\" < 2023 OR \"svarta hål\"")

        expect:
        sqt.tree == new SimpleQueryTree.Or(
                [
                        new SimpleQueryTree.PropertyValue("originDate", ["originDate"], Operator.LESS_THAN, "2023"),
                        new SimpleQueryTree.FreeText(Operator.EQUALS, "\"svarta hål\"")
                ] as List<SimpleQueryTree.Node>
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

    def "disambiguate type"() {
        def query = "typ: Tryck"
        SimpleQueryTree sqt = getTree(query)

        expect:
        sqt.tree == new SimpleQueryTree.PropertyValue("rdf:type", ["rdf:type"], Operator.EQUALS, "Print")
    }

    def "unrecognized type"() {
        when:
        getTree("type: UnknownType")

        then:
        thrown(InvalidQueryException)
    }

    def "disambiguate enum"() {
        def query = "utgivningssätt: \"Seriell resurs\""
        SimpleQueryTree sqt = getTree(query)

        expect:
        sqt.tree == new SimpleQueryTree.PropertyValue("issuanceType", ["issuanceType"], Operator.EQUALS, "Serial")
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
        SimpleQueryTree sqt = getTree(input)
        Set<Object> givenTypes = sqt.collectGivenTypes()

        expect:
        givenTypes == ["Electronic", "Print"] as Set
    }

    def "normalize free text"() {
        given:
        def input = "x y z \"a b c\" d label:l \"e:f\" not g h i"
        def sqt = getTree(input)

        expect:
        sqt.tree == new SimpleQueryTree.And(
                [
                        new SimpleQueryTree.FreeText(Operator.EQUALS, "x y z \"a b c\" d \"e:f\" h i"),
                        SimpleQueryTree.pvEqualsLiteral("label", "l"),
                        new SimpleQueryTree.FreeText(Operator.NOT_EQUALS, "g"),

                ] as List<SimpleQueryTree.Node>
        )
    }
}
