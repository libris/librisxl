package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class QueryTreeMergerSpec extends Specification {
    static Disambiguate disambiguate = TestData.getDisambiguate()
    static JsonLd jsonLd = TestData.getJsonLd()

    def "merge"() {
        given:
        Node aTree = QueryTreeBuilder.buildTree(a, disambiguate)
        Node bTree = QueryTreeBuilder.buildTree(b, disambiguate)
        Node merged = QueryTreeMerger.mergeAndReduce(aTree, bTree, jsonLd)

        expect:
        merged.toString() == result

        where:
        a                            | b                                          | result
        "X"                          | "X"                                        | "X"
        "X"                          | "p1:A"                                     | "X p1:A"
        "X"                          | "X p1:A"                                   | "X p1:A"
        "X"                          | "type:T1 excludeA"                         | "X type:T1 excludeA"
        "X type:T1"                  | "type:T1 excludeA"                         | "X type:T1 excludeA"
        "X type:T1"                  | "type:T3 excludeA"                         | "X type:T1" // Incompatible types
        "X type:T1"                  | "type:T2 excludeA"                         | "X type:T1 instanceOf.type:T2 excludeA"
        "X type:T1"                  | "type:T1x excludeA"                        | "X type:T1" // T1x is narrower than T1 -> We can't be sure that excludeA applies to T1
        "X type:T1x"                 | "type:T1 excludeA"                         | "X type:T1x excludeA" // If excludeA applies to T1, then it also applies to the narrower T1x
        "X type:(T1x OR T3x)"        | "type:T1 excludeA"                         | "(type:T1x X excludeA) OR (type:T3x X)" // excludeA only applicable for T1x
        "X type:(T1x OR T2x)"        | "type:T1 excludeA"                         | "(type:T1x X excludeA) OR (type:T2x X hasInstance.type:T1 excludeA)" // excludeA indirectly applicable to T2x via integral relation
        "X type:(T1x OR T3x)"        | "(type:T1 excludeA) OR (type:T3 includeA)" | "(type:T1x X excludeA) OR (type:T3x X includeA)" // excludeA only applicable to T1x, includeA only applicable to T3x
        "X type:T1"                  | "type:(T2 OR T3) p7:v7 p8:v8"              | "X type:T1 instanceOf.type:T2 p7:v7 p8:v8"
        "X type:T2"                  | "type:(T1 OR T3) p7:v7 p8:v8"              | "X type:T2 hasInstance.type:T1 p7:v7 p8:v8"
        "X type:T1"                  | "p7:v7"                                    | "X type:T1 p7:v7"
        "X type:T1"                  | "p8:v8"                                    | "X type:T1 p8:v8"
        "X type:T2"                  | "p7:v7"                                    | "X type:T2 p7:v7"
        "X type:T2"                  | "p8:v8"                                    | "X type:T2 p8:v8"
        "X type:T1"                  | "p7:v7 p8:v8"                              | "X type:T1 p7:v7 p8:v8"
        "X type:T1"                  | "p7:v7 p8:v8 p9:v9"                        | "X type:T1 p7:v7 p8:v8"
        "X type:T3"                  | "p7:v7"                                    | "X type:T3"
        "X type:T3"                  | "p9:v9"                                    | "X type:T3 p9:v9"
        "X type:(T1 OR T3)"          | "p7:v7 p9:v9"                              | "(type:T1 X p7:v7) OR (type:T3 X p9:v9)"
        "X type:(T1 OR T3)"          | "p7:v7 OR p9:v9"                           | "(type:T1 X p7:v7) OR (type:T3 X p9:v9)"
        "X type:(T1 OR T3)"          | "p7:v7 p8:v8 p9:v9"                        | "(type:T1 X p7:v7 p8:v8) OR (type:T3 X p9:v9)"
        "(type:T1 X) OR (type:T3 Y)" | "p7:v7 p8:v8 p9:v9"                        | "(type:T1 X p7:v7 p8:v8) OR (type:T3 Y p9:v9)"
        "x NOT type:T3"              | "type:T1"                                  | "x NOT type:T3 type:T1"
    }
}
