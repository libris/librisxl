package whelk.search2.querytree;

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate;

class OrSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "implies"() {
        given:
        Node aTree = QueryTreeBuilder.buildTree(a, disambiguate)
        Node bTree = QueryTreeBuilder.buildTree(b, disambiguate)

        expect:
        aTree.implies(bTree, jsonLd) == result

        where:
        a                        | b                                 | result
        "p1:v1 OR p2:v2"         | "p1:v1 OR p2:v2"                  | true
        "p1:v1 OR p2:v2"         | "p1:v1 OR p3:v3"                  | false
        "p1:v1 OR p2:v2"         | "p1:v1"                           | false
        "p1:v1 OR p2:v2"         | "p1:v1 OR p2:v2 OR p3:v3"         | true
        "p1:v1 OR p2:v2"         | "p1:v1 p2:v2"                     | false
        "NOT p1:v1 OR p2:v2"     | "NOT p1:v1 OR p2:v2"              | true
        "NOT p1:v1 OR p2:v2"     | "NOT p1:v1"                       | false
        "NOT p1:v1 OR NOT p2:v2" | "NOT p1:v1 OR p2:v2"              | false
        "NOT p1:v1 OR NOT p2:v2" | "p1:v1 OR p2:v2"                  | false
        "NOT p1:v1 OR NOT p2:v2" | "NOT p1:v1 OR NOT p2:v2"          | true
        "NOT p1:v1 OR NOT p2:v2" | "NOT p1:v1 OR NOT p2:v2 OR p3:v3" | true
        "NOT p1:v1 OR NOT p2:v2" | "NOT (p1:v1 p2:v2)"               | true
        "NOT p1:v1 OR NOT p3:v3" | "NOT (p1:v1 p2:v2)"               | false
    }

    def "expand"() {
        given:
        Node tree = QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        tree.expand(jsonLd, subjectTypes).toString() == result

        where:
        query                                | subjectTypes | result
        "(type:T1 p7:v7) OR (type:T2 p8:v8)" | []           | "((type:T1 OR type:T1x) p7:v7) OR ((type:T2 OR type:T2x) p8:v8)"
        "(type:T2 p7:v7) OR (type:T1 p8:v8)" | []           | "((type:T2 OR type:T2x) hasInstance.p7:v7) OR ((type:T1 OR type:T1x) instanceOf.p8:v8)"
        "(type:T1 p7:v7) OR (type:T2 p8:v8)" | ["T3"]       | "((type:T1 OR type:T1x) p7:v7) OR ((type:T2 OR type:T2x) p8:v8)"
        "(type:T2 p7:v7) OR (type:T2 p8:v8)" | ["T1"]       | "((type:T2 OR type:T2x) hasInstance.p7:v7) OR ((type:T2 OR type:T2x) p8:v8)"
        "(type:T2 p7:v7) OR p8:v8"           | ["T1"]       | "((type:T2 OR type:T2x) hasInstance.p7:v7) OR instanceOf.p8:v8"
    }
}
