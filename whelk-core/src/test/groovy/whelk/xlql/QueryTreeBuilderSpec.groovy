package whelk.xlql

import spock.lang.Specification
import whelk.Whelk

class QueryTreeBuilderSpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static QueryTreeBuilder queryTree = new QueryTreeBuilder(whelk)

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
        Object disambiguated = queryTree.toDisambiguatedAst(input)
        Set<String> givenProperties = queryTree.collectGivenProperties(disambiguated)
        Set<Object> givenTypes = queryTree.collectGivenTypes(disambiguated)

        expect:
        givenProperties == ["@type", "publication", "contentType", "title", "bibliography"] as Set
        givenTypes == ["Electronic"] as Set
    }

    def "Construct tree: free text only"() {
        given:
        def input = "AAA BBB and (CCC or DDD)"
        def disambiguatedAst = queryTree.toDisambiguatedAst(input)

        expect:
        queryTree.toQueryTree(disambiguatedAst) == new QueryTreeBuilder.And(
                [
                        new QueryTreeBuilder.FreeText("AAA", QueryTreeBuilder.Operator.EQUALS),
                        new QueryTreeBuilder.FreeText("BBB", QueryTreeBuilder.Operator.EQUALS),
                        new QueryTreeBuilder.Or(
                                [
                                        new QueryTreeBuilder.FreeText("CCC", QueryTreeBuilder.Operator.EQUALS),
                                        new QueryTreeBuilder.FreeText("DDD", QueryTreeBuilder.Operator.EQUALS)
                                ]
                        )
                ]
        )
    }

    def "Construct tree: fields + free text"() {
        given:
        def input = "subject: \"sao:Fysik\" AND NOT tillkomsttid < 2023 AND \"svarta hål\""
        def disambiguatedAst = queryTree.toDisambiguatedAst(input)
        QueryTreeBuilder.And qt = queryTree.toQueryTree(disambiguatedAst)
        List conjuncts = qt.conjuncts()
        QueryTreeBuilder.Or subjectFields = conjuncts[0]
        QueryTreeBuilder.Field subjectField1 = subjectFields.disjuncts()[0]
        QueryTreeBuilder.Field subjectField2 = subjectFields.disjuncts()[1]
        QueryTreeBuilder.Field originDateField = conjuncts[1]

        expect:
        conjuncts[2] == new QueryTreeBuilder.FreeText("svarta hål", QueryTreeBuilder.Operator.EQUALS)

        originDateField.path().stringify() == "originDate"
        originDateField.operator() == QueryTreeBuilder.Operator.GREATER_THAN_OR_EQUAL
        originDateField.value() == "2023"

        subjectField1.path().stringify() == "subject"
        subjectField1.operator() == QueryTreeBuilder.Operator.EQUALS
        subjectField1.value() == "sao:Fysik"

        subjectField2.path().stringify() == "subject.@id"
        subjectField2.operator() == QueryTreeBuilder.Operator.EQUALS
        subjectField2.value() == "https://id.kb.se/term/sao/Fysik"
    }
}
