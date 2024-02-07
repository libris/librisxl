package whelk.xlql

import spock.lang.Specification
import whelk.Whelk

class QueryTreeSpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static Disambiguate disambiguate = new Disambiguate(whelk)

    private SimpleQueryTree getSimpleQueryTree(String queryString) {
        LinkedList<Lex.Symbol> lexedSymbols = Lex.lexQuery(queryString)
        Parse.OrComb parseTree = Parse.parseQuery(lexedSymbols)
        Ast ast = new Ast(parseTree)
        FlattenedAst flattened = new FlattenedAst(ast)
        return new SimpleQueryTree(flattened, disambiguate)
    }

    def "collect given properties from query"() {
        given:
        def input = "type: Electronic AND utgivning: aaa AND (contentType: bbb OR title: ccc) AND bibliography: ddd"
        SimpleQueryTree sqt = getSimpleQueryTree(input)

        expect:
        QueryTree.collectGivenProperties(sqt.tree) == ["@type", "publication", "contentType", "title", "bibliography"] as Set
    }

    def "collect given types from query"() {
        given:
        def input = "type: (Electronic OR Print) AND utgivning: aaa"
        SimpleQueryTree sqt = getSimpleQueryTree(input)
        Set<Object> givenTypes = QueryTree.collectGivenTypes(sqt.tree)

        expect:
        givenTypes == ["Electronic", "Print"] as Set
    }

    def "free text tree"() {
        given:
        SimpleQueryTree sqt = getSimpleQueryTree("AAA BBB and (CCC or DDD)")
        QueryTree qt = new QueryTree(sqt, disambiguate)

        expect:
        qt.tree == new QueryTree.And(
                [
                        new QueryTree.FreeText(Operator.EQUALS, "AAA"),
                        new QueryTree.FreeText(Operator.EQUALS, "BBB"),
                        new QueryTree.Or(
                                [
                                        new QueryTree.FreeText(Operator.EQUALS, "CCC"),
                                        new QueryTree.FreeText(Operator.EQUALS, "DDD")
                                ]
                        )
                ]
        )
    }

    def "object property + datatype property + free text"() {
        given:
        SimpleQueryTree sqt = getSimpleQueryTree("subject: \"sao:Fysik\" AND tillkomsttid < 2023 AND \"svarta hål\"")
        QueryTree qt = new QueryTree(sqt, disambiguate)
        QueryTree.And topNode = qt.tree
        List conjuncts = topNode.conjuncts()
        QueryTree.Or subjectFields = conjuncts[0]
        QueryTree.Field subjectField1 = subjectFields.disjuncts()[0]
        QueryTree.Field subjectField2 = subjectFields.disjuncts()[1]
        QueryTree.Field originDateField = conjuncts[1]

        expect:
        conjuncts[2] == new QueryTree.FreeText(Operator.EQUALS, "svarta hål")

        originDateField.path().stringify() == "originDate"
        originDateField.operator() == Operator.LESS_THAN
        originDateField.value() == "2023"

        subjectField1.path().stringify() == "subject"
        subjectField1.operator() == Operator.EQUALS
        subjectField1.value() == "sao:Fysik"

        subjectField2.path().stringify() == "subject.@id"
        subjectField2.operator() == Operator.EQUALS
        subjectField2.value() == "https://id.kb.se/term/sao/Fysik"
    }

    def "exact fields"() {
        given:
        String queryString = "instanceOf.subject.@id: \"sao:Fysik\" AND instanceOf.subject._str: rymd and utgivningssätt: Monograph"
        SimpleQueryTree sqt = getSimpleQueryTree(queryString)
        QueryTree qt = new QueryTree(sqt, disambiguate)
        QueryTree.And topNode = qt.tree
        List conjuncts = topNode.conjuncts()
        QueryTree.Field subjectField = conjuncts[0]
        QueryTree.Field subjectField2 = conjuncts[1]
        QueryTree.Field issuanceTypeField = conjuncts[2]

        expect:
        subjectField.path().stringify() == "instanceOf.subject.@id"
        subjectField.operator() == Operator.EQUALS
        subjectField.value() == "https://id.kb.se/term/sao/Fysik"

        subjectField2.path().stringify() == "instanceOf.subject._str"
        subjectField2.operator() == Operator.EQUALS
        subjectField2.value() == "rymd"

        issuanceTypeField.path().stringify() == "issuanceType"
        issuanceTypeField.operator() == Operator.EQUALS
        issuanceTypeField.value() == "Monograph"
    }

    // TODO: More tests when settled which alternative paths to search for each property
}
