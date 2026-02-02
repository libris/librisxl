package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.AppParams
import whelk.search2.Disambiguate
import whelk.search2.ESSettings
import whelk.search2.Query
import whelk.search2.QueryParams
import whelk.search2.SelectedFacets

class QueryTreeSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "back to query string"() {
        expect:
        new QueryTree(input, disambiguate).toQueryString() == back

        where:
        input                                              | back
        null                                               | ""
        "()"                                               | ""
        ""                                                 | ""
        "*"                                                | "*"
        "* p1:x"                                           | "p1:x"
        "x y"                                              | "x y"
        "\"x y\""                                          | "\"x y\""
        "\"x y\" z"                                        | "\"x y\" z"
        "x OR y z"                                         | "x OR y z"
        "x OR \"y z\""                                     | "x OR \"y z\""
        "NOT x y"                                          | "NOT x y"
        "NOT (x y)"                                        | "NOT (x y)"
        "NOT (x OR y)"                                     | "NOT (x OR y)"
        "p1:x"                                             | "p1:x"
        "p1:\"x y\""                                       | "p1:\"x y\""
        "p1:\"x OR y\""                                    | "p1:\"x OR y\""
        "p1:(x y)"                                         | "p1:(x y)"
        "p1:(x OR y)"                                      | "p1:(x OR y)"
        "NOT p1:(x OR y)"                                  | "NOT p1:(x OR y)"
        "NOT p1:(NOT x)"                                   | "p1:x"
        "p2:e1"                                            | "p2:e1"
        "p2:(e1 e2)"                                       | "p2:e1 p2:e2"
        "p2:(e1 OR e2)"                                    | "p2:e1 OR p2:e2"
        "NOT p2:(e1 e2)"                                   | "NOT p2:e1 OR NOT p2:e2"
        "NOT p2:(e1 OR e2)"                                | "NOT p2:e1 NOT p2:e2"
        "type:(t1 OR t2)"                                  | "type:t1 OR type:t2"
        "p3:x"                                             | "p3:x"
        "p3:\"https://id.kb.se/x\""                        | "p3:\"https://id.kb.se/x\""
        "p3:\"sao:x\""                                     | "p3:\"sao:x\""
        "p3:(\"sao:x\" \"sao:y\")"                         | "p3:\"sao:x\" p3:\"sao:y\""
        "p3:(\"x y\" z \"sao:x\" \"sao:y\")"               | "p3:(\"x y\" z) p3:\"sao:x\" p3:\"sao:y\""
        "p3:(x (\"sao:x\" OR \"sao:y\"))"                  | "p3:x (p3:\"sao:x\" OR p3:\"sao:y\")"
        "NOT p3:(NOT x)"                                   | "p3:x"
        "NOT p3:(x y (\"sao:x\" OR NOT \"sao:y\"))"        | "NOT p3:(x y) OR (NOT p3:\"sao:x\" p3:\"sao:y\")"
        "_x._y:z"                                          | "_x._y:z"
        "x p1:y includeA"                                  | "x p1:y includeA"
        "p1>1990"                                          | "p1>1990"
        "NOT p1>1990"                                      | "p1<=1990"
        "p1=1990"                                          | "p1:1990"
        "p12:1990-01-01"                                   | "p12:1990-01-01"
        "NOT p12<=1990-01-01"                              | "p12>1990-01-01"
        "p12:\"1990-01-01T01:01\""                         | "p12:\"1990-01-01T01:01\""
        "(x OR y) p1:x"                                    | "(x OR y) p1:x"
        "x OR y"                                           | "x OR y"
        "(x OR y) z"                                       | "(x OR y) z"
        "findcategory:\"https://id.kb.se/term/ktg/X\""     | "category:\"https://id.kb.se/term/ktg/X\""
        "identifycategory:\"https://id.kb.se/term/ktg/Y\"" | "category:\"https://id.kb.se/term/ktg/Y\""
        "nonecategory:\"https://id.kb.se/term/ktg/Z\""     | "category:\"https://id.kb.se/term/ktg/Z\""
        "category:\"https://id.kb.se/term/ktg/X\""         | "category:\"https://id.kb.se/term/ktg/X\""
        "category:\"https://id.kb.se/term/ktg/Y\""         | "category:\"https://id.kb.se/term/ktg/Y\""
        "category:\"https://id.kb.se/term/ktg/Z\""         | "category:\"https://id.kb.se/term/ktg/Z\""
    }

    def "to search mapping"() {
        given:
        def tree = QueryTreeBuilder.buildTree('something (NOT p3:v3 OR p4:"v:4") includeA', disambiguate)

        expect:
        new QueryTree(tree).toSearchMapping(new QueryParams([:]), '_q') ==
                [
                        "and": [[
                                        "property": [
                                                "@id"  : "textQuery",
                                                "@type": "DatatypeProperty"
                                        ],
                                        "equals"  : "something",
                                        "up"      : [
                                                "@id": "/find?_q=%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                        ]
                                ], [
                                        "or": [
                                                [
                                                        "not": [
                                                                "property": [
                                                                        "@id"  : "p3",
                                                                        "@type": "ObjectProperty"
                                                                ],
                                                                "equals"  : "v3",
                                                                "up"      : [
                                                                        "@id": "/find?_q=something+p4:%22v:4%22+includeA"
                                                                ],
                                                                "_key"    : "p3",
                                                                "_value"  : "v3"
                                                        ],
                                                        "up" : [
                                                                "@id": "/find?_q=something+p4:%22v:4%22+includeA"
                                                        ]

                                                ],
                                                [
                                                        "property": [
                                                                "@id"  : "p4",
                                                                "@type": "ObjectProperty"
                                                        ],
                                                        "equals"  : "\"v:4\"",
                                                        "up"      : [
                                                                "@id": "/find?_q=something+NOT+p3:v3+includeA"
                                                        ],
                                                        "_key"    : "p4",
                                                        "_value"  : "\"v:4\""
                                                ]],
                                        "up": [
                                                "@id": "/find?_q=something+includeA"
                                        ]
                                ], [
                                        "object": [
                                                "prefLabelByLang": [:],
                                                "alias"          : "includeA",
                                                "raw"            : "NOT excludeA",
                                                "@type"          : "Resource",
                                                "parsedFilter"   : [
                                                        "not": [
                                                                "object": [
                                                                        "prefLabelByLang": [:],
                                                                        "alias"          : "excludeA",
                                                                        "raw"            : "NOT p1:A",
                                                                        "@type"          : "Resource",
                                                                        "parsedFilter"   : [
                                                                                "not": [
                                                                                        "property": [
                                                                                                "@id"  : "p1",
                                                                                                "@type": "DatatypeProperty"
                                                                                        ],
                                                                                        "equals"  : "A",
                                                                                        "up"      : [
                                                                                                "@id": "/find?_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                                                        ],
                                                                                        "_key"    : "p1",
                                                                                        "_value"  : "A"
                                                                                ],
                                                                                "up" : [
                                                                                        "@id": "/find?_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                                                ]
                                                                        ]
                                                                ],
                                                                "value" : "excludeA",
                                                                "up"    : [
                                                                        "@id": "/find?_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                                ]
                                                        ],
                                                        "up" : [
                                                                "@id": "/find?_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                        ]
                                                ]
                                        ],
                                        "value" : "includeA",
                                        "up"    : [
                                                "@id": "/find?_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29"
                                        ]
                                ]],
                        "up" : [
                                "@id": "/find?_q="
                        ]
                ]
    }

    def "concat simple free text on instantiation"() {
        given:
        QueryTree qt = new QueryTree("x y (x OR y) \"a b c\" d \"e:f\" NOT g h i", disambiguate)
        var ft1 = new QueryTree("x y \"a b c\" d \"e:f\" h i", disambiguate).tree
        var ft2 = QueryTreeBuilder.buildTree("x OR y", disambiguate)
        var ft3 = QueryTreeBuilder.buildTree("NOT g", disambiguate)

        expect:
        qt.tree == new And([ft1, ft2, ft3])
    }

    def "add node"() {
        given:
        Node nodeToAdd = QueryTreeBuilder.buildTree(add, disambiguate)
        QueryTree qt = new QueryTree(q, disambiguate)

        expect:
        qt.add(nodeToAdd).toQueryString() == result

        where:
        q                | add                                    | result
        null             | 'p1:v1'                                | 'p1:v1'
        'p1:v1'          | 'p2:v2'                                | 'p1:v1 p2:v2'
        'p1:v1'          | 'p1:v1'                                | 'p1:v1'
        'p1:v1 OR p2:v2' | 'p2:v2'                                | '(p1:v1 OR p2:v2) p2:v2'
        'p1:v1 p2:v2'    | 'p1:v1'                                | 'p1:v1 p2:v2'
        'p1:v1 p2:v2'    | 'p3:v3'                                | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2'    | 'p1:v1 p3:v3'                          | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2'    | 'p1:v1 p2:v2'                          | 'p1:v1 p2:v2'
        'p1:v1 p2:v2'    | '((p1:v1 p2:v2) OR p3:v3) p2:v2 p3:v3' | 'p1:v1 p2:v2 ((p1:v1 p2:v2) OR p3:v3) p3:v3'
    }

    def "remove nodes"() {
        given:
        QueryTree queryTree = new QueryTree(q, disambiguate)
        List<Node> nodesToMatch = remove.collect { QueryTreeBuilder.buildTree(it, disambiguate) }
        List<Node> nodesToRemove = queryTree.allDescendants().filter(nodesToMatch::contains).toList()

        expect:
        queryTree.removeAll(nodesToRemove).toQueryString() == result

        where:
        q                              | remove                               | result
        'p1:v1 p2:v2'                  | ['p1:v1']                            | 'p2:v2'
        'p1:v1 p2:v2'                  | ['p3:v3']                            | 'p1:v1 p2:v2'
        'p1:v1 p2:v2 p3:v3'            | ['p3:v3']                            | 'p1:v1 p2:v2'
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1 p2:v2 p3:v3']                | ''
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1 p2:v2']                      | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1', 'p2:v2']                   | 'p3:v3'
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1 p2:v2 p3:v3 p4:v4']          | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1', 'p2:v2', 'p3:v3', 'p4:v4'] | ''
        'p1:v1 p2:v2 p3:v3 p4:v4'      | ['p1:v1', 'p2:v2', 'p3:v3']          | 'p4:v4'
        'p1:v1 (p2:v2 OR p3:v3) p4:v4' | ['p1:v1', 'p4:v4']                   | 'p2:v2 OR p3:v3'
        'p1:v1 (p2:v2 OR p3:v3) p4:v4' | ['p1:v1', 'p2:v2 OR p3:v3']          | 'p4:v4'
        'p1:v1 (p2:v2 OR p3:v3) p4:v4' | ['p3:v3']                            | 'p1:v1 p2:v2 p4:v4'
    }

    def "replace node"() {
        given:
        QueryTree queryTree = new QueryTree(q, disambiguate)
        Node nodeToMatch = QueryTreeBuilder.buildTree(replace, disambiguate)
        Node nodeToReplace = queryTree.allDescendants().find(nodeToMatch::equals)
        Node replacementNode = QueryTreeBuilder.buildTree(replacement, disambiguate)

        expect:
        queryTree.replace(nodeToReplace, replacementNode).toQueryString() == result

        where:
        q                              | replace             | replacement      | result
        'p1:v1 p2:v2'                  | 'p2:v2'             | 'p3:v3'          | 'p1:v1 p3:v3'
        'p1:v1 p2:v2'                  | 'p3:v3'             | 'p4:v4'          | 'p1:v1 p2:v2'
        'p1:v1 p2:v2 p3:v3'            | 'p1:v1 p2:v2 p3:v3' | 'x y z'          | 'x y z'
        'p1:v1 p2:v2 p3:v3'            | 'p1:v1 p2:v2'       | 'x y z'          | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2 p3:v3'            | 'p2:v2'             | 'p4:v4 OR p5:v5' | 'p1:v1 (p4:v4 OR p5:v5) p3:v3'
        'p1:v1 (p4:v4 OR p5:v5) p3:v3' | 'p5:v5'             | 'p6:v6 p7:v7'    | 'p1:v1 (p4:v4 OR (p6:v6 p7:v7)) p3:v3'
    }

    def "get top level free text as string"() {
        expect:
        new QueryTree(tree, disambiguate).getFreeTextPart() == result

        where:
        tree                | result
        'x y z p1:v1 p2:v2' | 'x y z'
        'x y z'             | 'x y z'
        'p1:v1'             | ''
        'x OR y'            | ''
        null                | ''
    }

    def "merge"() {
        given:
        QueryTree aTree = new QueryTree(a, disambiguate)
        QueryTree bTree = new QueryTree(b, disambiguate)
        QueryTree merged = aTree.merge(bTree, jsonLd)

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
        "X type:(T1x OR T2x)"        | "type:T1 excludeA"                         | "(type:T1x X excludeA) OR (type:T2x X)" // excludeA only applicable for T1x
        "X type:(T1x OR T2x)"        | "(type:T1 excludeA) OR (type:T2 includeA)" | "(type:T1x X excludeA) OR (type:T2x X includeA)"
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

    def "collect subject types"() {
        given:
        def subjectTypes = new QueryTree(tree, disambiguate).getRdfSubjectTypesList()


        expect:
        subjectTypes == result

        where:
        tree                                    | result
        'type:T1 p1:v1'                         | ["T1"]
        'type:(T1 T2) p1:v1'                    | ["T1"] // TODO?
        '(type:T1 p1:v1) OR (type:T2 p2:v2)'    | ["T1", "T2"]
        'p1:v1 p2:v2'                           | []
        'type:(T1 OR T2) p1:v1'                 | ["T1", "T2"]
        '(type:T1 OR p1:v1) (type:T2 OR p2:v2)' | []
    }
}
