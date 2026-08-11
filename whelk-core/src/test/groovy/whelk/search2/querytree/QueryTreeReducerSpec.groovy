package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class QueryTreeReducerSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "reduce"() {
        given:
        Node tree = QueryTreeBuilder.buildTree(group, disambiguate)
        Node reduced = QueryTreeReducer.reduce(tree, jsonLd)

        expect:
        reduced.toString() == result

        where:
        group                                     | result
        'p1:v1 p2:v2'                             | 'p1:v1 p2:v2'
        'p1:v1 (p1:v1 OR p2:v2)'                  | 'p1:v1'
        'p1:v1 OR (p1:v1 p2:v2)'                  | 'p1:v1'
        'p1:v1 (p1:v1 OR p3:v3)'                  | 'p1:v1'
        'p1:v1 (p2:v2 OR p3:v3)'                  | 'p1:v1 (p2:v2 OR p3:v3)'
        'p1:v1 OR (p2:v2 p3:v3)'                  | 'p1:v1 OR (p2:v2 p3:v3)'
        'p1:v1 (p1:v1 OR p2:v2 OR (p2:v2 p3:v3))' | 'p1:v1'
        'p1:v1 OR (p1:v1 2:v2 (p2:v2 OR p3:v3))'  | 'p1:v1'
        'p1:v1 (p2:v2 OR p3:v3 (p3:v3 OR p4:v4))' | 'p1:v1 (p2:v2 OR p3:v3)'
        'p1:v1 OR (p2:v2 p3:v3 (p3:v3 OR p4:v4))' | 'p1:v1 OR (p2:v2 p3:v3)'
        'p1:v1 (p2:v2 OR p3:v3 OR (p1:v1 p4:v4))' | 'p1:v1 (p2:v2 OR p3:v3 OR (p1:v1 p4:v4))'
        'p1:v1 OR (p2:v2 p3:v3 (p1:v1 OR p4:v4))' | 'p1:v1 OR (p2:v2 p3:v3 (p1:v1 OR p4:v4))'
        'p1:v1 p2:v2 (p2:v2 OR p3:v3)'            | 'p1:v1 p2:v2'
        'p1:v1 OR p2:v2 OR (p2:v2 p3:v3)'         | 'p1:v1 OR p2:v2'
        '(p1:v1 OR p2:v2) (p3:v3 OR p4:v4)'       | '(p1:v1 OR p2:v2) (p3:v3 OR p4:v4)'
        '(p1:v1 p2:v2) OR (p3:v3 p4:v4)'          | '(p1:v1 p2:v2) OR (p3:v3 p4:v4)'
        '(p1:v1 OR p2:v2) (p3:v3 OR p4:v4) p1:v1' | 'p1:v1 (p3:v3 OR p4:v4)'
        '(p1:v1 p2:v2) OR (p3:v3 p4:v4) OR p1:v1' | 'p1:v1 OR (p3:v3 p4:v4)'
        '(p1:v1 OR p2:v2) (p2:v2 OR p3:v3)'       | '(p1:v1 OR p2:v2) (p2:v2 OR p3:v3)'
        '(p1:v1 p2:v2) OR (p1:v1 p2:v2)'          | 'p1:v1 p2:v2'
        '(p1:v1 p2:v2) OR (p2:v2 p3:v3)'          | '(p1:v1 p2:v2) OR (p2:v2 p3:v3)'
        'type:T1x type:T1'                        | 'type:T1x'
        '(type:T1x OR p1:v1) (type:T1 OR p1:v1)'  | 'type:T1x OR p1:v1'
        'type:T1x OR type:T1'                     | 'type:T1'
        '(type:T1x p1:v1) OR (type:T1 p1:v1)'     | 'type:T1 p1:v1'
    }

    def "implies"() {
        given:
        Node aTree = QueryTreeBuilder.buildTree(a, disambiguate)
        Node bTree = QueryTreeBuilder.buildTree(b, disambiguate)

        expect:
        QueryTreeReducer.implies(aTree, bTree, jsonLd) == result

        where:
        a                        | b                                 | result
        "p1:v1 p2:v2"            | "p1:v1 p2:v2"                     | true
        "p1:v1 p2:v2"            | "p1:v1"                           | true
        "p1:v1 p2:v2"            | "p1:v1 p2:v2 p3:v3"               | false
        "p1:v1 p2:v2"            | "p1:v1 OR p2:v2"                  | true
        "p1:v1 p2:v2"            | "p1:v1 OR p3:v3"                  | true
        "p1:v1 p2:v2"            | "p3:v3 OR p4:v4"                  | false
        "p1:v1 excludeA"         | "excludeA"                        | true
        "p1:v1 excludeA"         | "NOT p1:A"                        | true
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
        "NOT p1:v1 p2:v2"        | "NOT p1:v1 p2:v2"                 | true
        "NOT p1:v1 p2:v2"        | "NOT p1:v1"                       | true
        "NOT p1:v1 NOT p2:v2"    | "NOT p1:v1 OR p2:v2"              | true
        "NOT p1:v1 NOT p2:v2"    | "p1:v1 OR p2:v2"                  | false
        "NOT p1:v1 NOT p2:v2"    | "NOT p1:v1 OR NOT p2:v2"          | true
        "NOT p1:v1 NOT p2:v2"    | "NOT p1:v1 OR NOT p2:v2 p3:v3"    | true
        "NOT p1:v1 NOT p2:v2"    | "NOT (p1:v1 p2:v2)"               | true
        "p1:v1 (p2:v2 OR p3:v3)" | "(p2:v2 OR p3:v3)"                | true
        "p1:v1 (p2:v2 OR p3:v3)" | "p1:v1 (p2:v2 OR p3:v3)"          | true
        "p1:v1 (p2:v2 OR p3:v3)" | "p1:v1 (p2:v2 OR p4:v4)"          | false
        "type:T1 X p7:v7"        | "X p7:v7"                         | true
        "excludeA"               | "excludeA"                        | true
        "excludeA"               | "NOT p1:A"                        | true
        "excludeA"               | "NOT includeA"                    | true
        "includeA"               | "includeA"                        | true
        "includeA"               | "NOT excludeA"                    | true
        "XY"                     | "XY"                              | true
        "XY"                     | "p1:X p3:Y"                       | true
        "XY"                     | "p1:X"                            | true
        "XY"                     | "p3:Y"                            | true
        "XY"                     | "p1:X p3:Y p4:Z"                  | false
        "NOT excludeA"           | "NOT excludeA"                    | true
        "NOT (p1:X p3:Y)"        | "NOT XY"                          | false
        "NOT XY"                 | "NOT (p1:X p3:Y)"                 | false
        "NOT p1:v1"              | "NOT (p1:v1 p2:v2)"               | true
        "NOT (p1:v1 p2:v2)"      | "NOT (p1:v1 p2:v2)"               | true
        "NOT p1:v1"              | "NOT (p1:v1 OR p2:v2)"            | false
    }
}
