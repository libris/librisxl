package whelk.xlql

import spock.lang.Ignore
import spock.lang.Specification
import whelk.Whelk

@Ignore
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

    def "object property + datatype property + free text"() {
        given:
        SimpleQueryTree sqt = getSimpleQueryTree("subject: \"sao:Fysik\" AND tillkomsttid < 2023 AND \"svarta hål\"")
        QueryTree qt = new QueryTree(sqt, disambiguate)
        QueryTree.And topNode = qt.tree
        List conjuncts = topNode.conjuncts()
        QueryTree.Field subjectField = conjuncts[0]
        QueryTree.Field originDateField = conjuncts[1]
        QueryTree.FreeText freeText = conjuncts[2]

        expect:
        subjectField.path().stringify() == "subject.@id"
        subjectField.operator() == Operator.EQUALS
        subjectField.value() == "https://id.kb.se/term/sao/Fysik"

        originDateField.path().stringify() == "originDate"
        originDateField.operator() == Operator.LESS_THAN
        originDateField.value() == "2023"

        freeText == new QueryTree.FreeText(Operator.EQUALS, "\"svarta hål\"")
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

    def "work subtype + path inference"() {
        given:
        String queryString = "typ:Text upphovsuppgift:Någon tillkomsttid<2020 language:\"https://id.kb.se/language/swe\""
        SimpleQueryTree sqt = getSimpleQueryTree(queryString)
        QueryTree qt = new QueryTree(sqt, disambiguate)
        QueryTree.And topNode = qt.tree
        List conjuncts = topNode.conjuncts()
        QueryTree.Field typeField = conjuncts[0]
        QueryTree.Field respStatementField = conjuncts[1]
        QueryTree.Field originDateField = conjuncts[2]
        QueryTree.Or languageFields = conjuncts[3]
        QueryTree.Field langField1 = languageFields.disjuncts()[0]
        QueryTree.Field langField2 = languageFields.disjuncts()[1]

        expect:
        typeField.path().stringify() == "@type"
        typeField.operator() == Operator.EQUALS
        typeField.value() == "Text"

        respStatementField.path().stringify() == "@reverse.instanceOf.responsibilityStatement"
        respStatementField.operator() == Operator.EQUALS
        respStatementField.value() == "Någon"

        originDateField.path().stringify() == "originDate"
        originDateField.operator() == Operator.LESS_THAN
        originDateField.value() == "2020"

        langField1.path().stringify() == "language.@id"
        langField1.operator() == Operator.EQUALS
        langField1.value() == "https://id.kb.se/language/swe"

        langField2.path().stringify() == "@reverse.instanceOf.language.@id"
        langField2.operator() == Operator.EQUALS
        langField2.value() == "https://id.kb.se/language/swe"
    }

    def "instance type + path inference"() {
        given:
        String queryString = "typ:Instance upphovsuppgift:Någon tillkomsttid<2020 language:\"https://id.kb.se/language/swe\""
        SimpleQueryTree sqt = getSimpleQueryTree(queryString)
        QueryTree qt = new QueryTree(sqt, disambiguate)
        QueryTree.And topNode = qt.tree
        List conjuncts = topNode.conjuncts()
        QueryTree.Or typeFields = conjuncts[0]
        QueryTree.Field respStatementField = conjuncts[1]
        QueryTree.Field originDateField = conjuncts[2]
        QueryTree.Or languageFields = conjuncts[3]
        QueryTree.Field langField1 = languageFields.disjuncts()[0]
        QueryTree.Field langField2 = languageFields.disjuncts()[1]

        expect:
        typeFields.disjuncts().size() == disambiguate.instanceTypes.size()

        respStatementField.path().stringify() == "responsibilityStatement"
        respStatementField.operator() == Operator.EQUALS
        respStatementField.value() == "Någon"

        originDateField.path().stringify() == "instanceOf.originDate"
        originDateField.operator() == Operator.LESS_THAN
        originDateField.value() == "2020"

        langField1.path().stringify() == "language.@id"
        langField1.operator() == Operator.EQUALS
        langField1.value() == "https://id.kb.se/language/swe"

        langField2.path().stringify() == "instanceOf.language.@id"
        langField2.operator() == Operator.EQUALS
        langField2.value() == "https://id.kb.se/language/swe"
    }

    def "nested"() {
        given:
        String queryString = "author:x not isbn:y"
        SimpleQueryTree sqt = getSimpleQueryTree(queryString)
        QueryTree qt = new QueryTree(sqt, disambiguate)
        QueryTree.And topNode = qt.tree
        List conjuncts = topNode.conjuncts()
        QueryTree.Nested author = conjuncts[0]
        QueryTree.Nested isbn = conjuncts[1]

        expect:
        author.operator() == Operator.EQUALS
        author.fields()[0].path().stringify() == "contribution.agent._str"
        author.fields()[0].value() == "x"
        author.fields()[1].path().stringify() == "contribution.role.@id"
        author.fields()[1].value() == "https://id.kb.se/relator/author"

        isbn.operator() == Operator.NOT_EQUALS
        isbn.fields()[0].path().stringify() == "identifiedBy.value"
        isbn.fields()[0].value() == "y"
        isbn.fields()[1].path().stringify() == "identifiedBy.@type"
        isbn.fields()[1].value() == "ISBN"
    }

    def "nested + path by type inference"() {
        given:
        String queryString = "type:Text author:x not isbn:y"
        SimpleQueryTree sqt = getSimpleQueryTree(queryString)
        QueryTree qt = new QueryTree(sqt, disambiguate)
        QueryTree.And topNode = qt.tree
        List conjuncts = topNode.conjuncts()
        QueryTree.Nested author = conjuncts[1]
        QueryTree.Nested isbn = conjuncts[2]

        expect:
        author.operator() == Operator.EQUALS
        author.fields()[0].path().stringify() == "contribution.agent._str"
        author.fields()[0].value() == "x"
        author.fields()[1].path().stringify() == "contribution.role.@id"
        author.fields()[1].value() == "https://id.kb.se/relator/author"

        isbn.operator() == Operator.NOT_EQUALS
        isbn.fields()[0].path().stringify() == "@reverse.instanceOf.identifiedBy.value"
        isbn.fields()[0].value() == "y"
        isbn.fields()[1].path().stringify() == "@reverse.instanceOf.identifiedBy.@type"
        isbn.fields()[1].value() == "ISBN"
    }
}
