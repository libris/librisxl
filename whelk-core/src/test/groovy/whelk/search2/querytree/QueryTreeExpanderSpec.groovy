package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class QueryTreeExpanderSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "expand type"() {
        given:
        Node tree = QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        QueryTreeExpander.expand(tree, jsonLd, subjectTypes).toString() == result

        where:
        query       | subjectTypes | result
        "type:T1"   | []           | "type:T1 OR type:T1x"
        "t1Type:T1" | ['T1']       | "t1Type:T1 OR t1Type:T1x"
        "t1Type:T1" | ['T2']       | "hasInstance.t1Type:T1 OR hasInstance.t1Type:T1x"
    }

    def "expand restrictions"() {
        given:
        Node tree = QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        QueryTreeExpander.expand(tree, jsonLd, []).toString() == result

        where:
        query                 | result
        "p10:v1"              | "p4.p1:v1 p4.p3:\"https://id.kb.se/x\""
        "p11:v1"              | "p3.p4:v1 (\"p3.rdf:type\":T3 OR \"p3.rdf:type\":T3x)"
        "restrictedP_p1:v1"   | "p3.p1:v1 p3.p4:\"https://id.kb.se/x\""
        "restrictedP_p1_2:v1" | "p3.p1:v1 p3.p4:\"https://id.kb.se/x\""
    }

    def "expand condition by subject types"() {
        given:
        Node tree = QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        QueryTreeExpander.expand(tree, jsonLd, subjectTypes).toString() == result

        where:
        query               | subjectTypes | result
        "p1:v1"             | []           | "p1:v1"
        "p1:v1"             | ["T1"]       | "instanceOf.p1:v1 OR p1:v1"
        "p1:v1"             | ["T2"]       | "hasInstance.p1:v1 OR p1:v1"
        "p1:v1"             | ["T3"]       | "p1:v1"
        "hasInstance.p7:v7" | ["T2"]       | "hasInstance.p7:v7"
        "instanceOf.p8:v8"  | ["T1"]       | "instanceOf.p8:v8"
        "p3.p1:x"           | []           | "p3.p1:x"
        "p3p1:x"            | ["T1"]       | "instanceOf.p3.p1:x OR p3.p1:x"
        "p3p1:x"            | ["T2"]       | "hasInstance.p3.p1:x OR p3.p1:x"
        "p3p1:x"            | ["T3"]       | "p3.p1:x"
        "t1p3p1:x"          | ["T1"]       | "p3.p1:x"
        "t1p3p1:x"          | ["T2"]       | "hasInstance.p3.p1:x"
        "t1p3p1:x"          | ["T3"]       | "p3.p1:x"
    }

    def "expand filter alias"() {
        given:
        Node tree = QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        String.valueOf(QueryTreeExpander.expand(tree, jsonLd, [])) == result

        where:
        query          | result
        "excludeA"     | "NOT p1:A"
        "NOT excludeA" | "null"
    }

    def "expand group"() {
        given:
        Node tree = QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        QueryTreeExpander.expand(tree, jsonLd, subjectTypes).toString() == result

        where:
        query                                | subjectTypes | result
        "p7:v7 p8:v8 p9:v9"                  | []           | "p7:v7 p8:v8 p9:v9"
        "p7:v7 p8:v8 p9:v9"                  | ["T1"]       | "p7:v7 instanceOf.p8:v8 p9:v9"
        "p7:v7 p8:v8 p9:v9"                  | ["T2"]       | "hasInstance.p7:v7 p8:v8 p9:v9"
        "p7:v7 p8:v8 p9:v9"                  | ["T3"]       | "p7:v7 p8:v8 p9:v9"
        "p7:v7 p8:v8 p9:v9"                  | ["T1", "T2"] | "(hasInstance.p7:v7 OR p7:v7) (instanceOf.p8:v8 OR p8:v8) p9:v9"
        "(type:T1 p7:v7) OR (type:T2 p8:v8)" | []           | "((type:T1 OR type:T1x) p7:v7) OR ((type:T2 OR type:T2x) p8:v8)"
        "(type:T2 p7:v7) OR (type:T1 p8:v8)" | []           | "((type:T2 OR type:T2x) hasInstance.p7:v7) OR ((type:T1 OR type:T1x) instanceOf.p8:v8)"
        "(type:T1 p7:v7) OR (type:T2 p8:v8)" | ["T3"]       | "((type:T1 OR type:T1x) p7:v7) OR ((type:T2 OR type:T2x) p8:v8)"
        "(type:T2 p7:v7) OR (type:T2 p8:v8)" | ["T1"]       | "((type:T2 OR type:T2x) hasInstance.p7:v7) OR ((type:T2 OR type:T2x) p8:v8)"
        "(type:T2 p7:v7) OR p8:v8"           | ["T1"]       | "((type:T2 OR type:T2x) hasInstance.p7:v7) OR instanceOf.p8:v8"
    }

    def "expand selector"() {
        given:
        Selector s = ((Condition) QueryTreeBuilder.buildTree("$_p:v", disambiguate)).selector()
        List<Selector> altPaths = QueryTreeExpander.expandSelector(s, jsonLd, subjectTypes, false)

        expect:
        altPaths.collect { it.toString() } == result

        where:
        _p                    | subjectTypes | result
        "p1"                  | []           | ["p1"]
        "p1"                  | ["T1"]       | ["instanceOf.p1", "p1"]
        "p1"                  | ["T2"]       | ["hasInstance.p1", "p1"]
        "p1"                  | ["T1", "T2"] | ["hasInstance.p1", "instanceOf.p1", "p1"]
        "p1"                  | ["T3"]       | ["p1"]
        "p6"                  | []           | ["p3.p4"]
        "p6.p1"               | []           | ["p3.p4.p1"]
        "hasItem"             | ["T1"]       | ["hasItem"]
        "hasItem"             | ["T2"]       | ["hasInstance.hasItem"]
        "hasItem"             | ["T1", "T2"] | ["hasInstance.hasItem", "hasItem"]
        "hasItem"             | ["T3"]       | []
        "p8"                  | ["T1"]       | ["instanceOf.p8"]
        "p8"                  | ["T2"]       | ["p8"]
        "p8"                  | ["T1", "T2"] | ["instanceOf.p8", "p8"]
        "p8"                  | ["T3"]       | []
        "p9"                  | ["T1"]       | []
        "p9"                  | ["T2"]       | []
        "p9"                  | ["T1", "T2"] | []
        "p9"                  | ["T3"]       | ["p9"]
        "hasInstance.hasItem" | ["T2"]       | ["hasInstance.hasItem"]
        "type"                | ["T2"]       | ["rdf:type"]
        "instanceOf.p8"       | ["T1"]       | ["instanceOf.p8"]
        "type"                | ["T1"]       | ["rdf:type"]
        "hasItem.p14"         | ["T2"]       | ["hasInstance.hasItem.hasComponent.p14", "hasInstance.hasItem.p14"]
    }
}
