package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class AndSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "invert"() {
        expect:
        ((And) QueryTreeBuilder.buildTree(and, disambiguate)).getInverse().toString() == result

        where:
        and                   | result
        'p1:v1 p2:v2'         | 'NOT p1:v1 OR NOT p2:v2'
        'NOT p1:v1 p2:v2'     | 'p1:v1 OR NOT p2:v2'
        'NOT p1:v1 NOT p2:v2' | 'p1:v1 OR p2:v2'
        'p1:v1 p2>v2'         | 'NOT p1:v1 OR p2<=v2'
    }

    def "implies"() {
        given:
        Node aTree = QueryTreeBuilder.buildTree(a, disambiguate)
        Node bTree = QueryTreeBuilder.buildTree(b, disambiguate)

        expect:
        aTree.implies(bTree, jsonLd) == result

        where:
        a                        | b                              | result
        "p1:v1 p2:v2"            | "p1:v1 p2:v2"                  | true
        "p1:v1 p2:v2"            | "p1:v1"                        | true
        "p1:v1 p2:v2"            | "p1:v1 p2:v2 p3:v3"            | false
        "p1:v1 p2:v2"            | "p1:v1 OR p2:v2"               | true
        "p1:v1 p2:v2"            | "p1:v1 OR p3:v3"               | true
        "p1:v1 p2:v2"            | "p3:v3 OR p4:v4"               | false
        "p1:v1 excludeA"         | "excludeA"                     | true
        "p1:v1 excludeA"         | "NOT p1:A"                     | true
        "NOT p1:v1 p2:v2"        | "NOT p1:v1 p2:v2"              | true
        "NOT p1:v1 p2:v2"        | "NOT p1:v1"                    | true
        "NOT p1:v1 NOT p2:v2"    | "NOT p1:v1 OR p2:v2"           | true
        "NOT p1:v1 NOT p2:v2"    | "p1:v1 OR p2:v2"               | false
        "NOT p1:v1 NOT p2:v2"    | "NOT p1:v1 OR NOT p2:v2"       | true
        "NOT p1:v1 NOT p2:v2"    | "NOT p1:v1 OR NOT p2:v2 p3:v3" | true
        "NOT p1:v1 NOT p2:v2"    | "NOT (p1:v1 p2:v2)"            | true
        "p1:v1 (p2:v2 OR p3:v3)" | "(p2:v2 OR p3:v3)"             | true
        "p1:v1 (p2:v2 OR p3:v3)" | "p1:v1 (p2:v2 OR p3:v3)"       | true
        "p1:v1 (p2:v2 OR p3:v3)" | "p1:v1 (p2:v2 OR p4:v4)"       | false
        "type:T1 X p7:v7"        | "X p7:v7"                      | true
    }

    def "expand"() {
        given:
        Node tree = QueryTreeBuilder.buildTree(query, disambiguate)

        expect:
        tree.expand(jsonLd, subjectTypes).toString() == result

        where:
        query               | subjectTypes | result
        "p7:v7 p8:v8 p9:v9" | []           | "p7:v7 p8:v8 p9:v9"
        "p7:v7 p8:v8 p9:v9" | ["T1"]       | "p7:v7 instanceOf.p8:v8 p9:v9"
        "p7:v7 p8:v8 p9:v9" | ["T2"]       | "hasInstance.p7:v7 p8:v8 p9:v9"
        "p7:v7 p8:v8 p9:v9" | ["T3"]       | "p7:v7 p8:v8 p9:v9"
        "p7:v7 p8:v8 p9:v9" | ["T1", "T2"] | "(hasInstance.p7:v7 OR p7:v7) (instanceOf.p8:v8 OR p8:v8) p9:v9"
    }
}
