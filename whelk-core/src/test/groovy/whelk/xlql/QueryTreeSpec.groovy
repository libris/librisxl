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

        expect:
        queryTree.toQueryTree(input) == new QueryTree.And(
                [
                        new QueryTree.Or(
                                [
                                        new QueryTree.FreeText("DDD", QueryTree.Operator.EQUALS),
                                        new QueryTree.FreeText("CCC", QueryTree.Operator.EQUALS)
                                ]
                        ),
                        new QueryTree.FreeText("BBB", QueryTree.Operator.EQUALS),
                        new QueryTree.FreeText("AAA", QueryTree.Operator.EQUALS),
                ]
        )
    }

    def "Construct tree: fields + free text"() {
        given:
        def input = "subject: \"sao:Fysik\" AND NOT tillkomsttid < 2023 AND \"svarta hål\""
        QueryTree.And qt = queryTree.toQueryTree(input)
        List conjuncts = qt.conjuncts()
        QueryTree.Field originDateField = conjuncts[1]
        QueryTree.Or subjectFields = conjuncts[2]
        QueryTree.Field subjectField1 = subjectFields.disjuncts()[0]
        QueryTree.Field subjectField2 = subjectFields.disjuncts()[1]

        expect:
        conjuncts[0] == new QueryTree.FreeText("svarta hål", QueryTree.Operator.EQUALS)

        originDateField.path().stringify() == "originDate"
        originDateField.operator() == QueryTree.Operator.GREATER_THAN_OR_EQUAL
        originDateField.value() == "2023"

        subjectField1.path().stringify() == "subject"
        subjectField1.operator() == QueryTree.Operator.EQUALS
        subjectField1.value() == "sao:Fysik"

        subjectField2.path().stringify() == "subject.@id"
        subjectField2.operator() == QueryTree.Operator.EQUALS
        subjectField2.value() == "https://id.kb.se/term/sao/Fysik"
    }
}
