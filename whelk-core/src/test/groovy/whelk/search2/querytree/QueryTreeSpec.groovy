package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.Operator

class QueryTreeSpec extends Specification {
    static Disambiguate disambiguate = TestData.getDisambiguate()
    static JsonLd jsonLd = TestData.getJsonLd()

    static var p1 = Property.getProperty("p1", jsonLd)
    static var p1v1 = new Condition(p1, Operator.EQUALS, new FreeText("v1"))
    static var p1v2 = new Condition(p1, Operator.EQUALS, new FreeText("v2"))

    def "back to query string"() {
        expect:
        new QueryTree(input, disambiguate).toQueryString() == back

        where:
        input                                              | back
        null                                               | ""
        "()"                                               | "()"
        ""                                                 | ""
        "*"                                                | "*"
        "x ()"                                             | "x ()"
        "x OR ()"                                          | "x OR ()"
        "p1:()"                                            | "p1:()"
        "p1:*"                                             | "p1:*"
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
        "p1:(x OR y z)"                                    | "p1:x OR p1:(y z)"
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
        "findcategory:\"https://id.kb.se/term/ktg/X\""     | "workCategory:\"https://id.kb.se/term/ktg/X\""
        "identifycategory:\"https://id.kb.se/term/ktg/Y\"" | "workCategory:\"https://id.kb.se/term/ktg/Y\""
        "workcategory:\"https://id.kb.se/term/ktg/X\""     | "workcategory:\"https://id.kb.se/term/ktg/X\""
        "workcategory:\"https://id.kb.se/term/ktg/Y\""     | "workcategory:\"https://id.kb.se/term/ktg/Y\""
        "workcategory:\"https://id.kb.se/term/ktg/Z\""     | "workcategory:\"https://id.kb.se/term/ktg/Z\""
        "workcategory:Y"                                   | "workcategory:Y"
        "workcategory:(X Y)"                               | "workcategory:(X Y)"
        "instancecategory:\"https://id.kb.se/term/ktg/Z\"" | "instancecategory:\"https://id.kb.se/term/ktg/Z\""
        "instancecategory:X"                               | "instancecategory:X"
        "instancecategory:(X Y)"                           | "instancecategory:(X Y)"
        "category:\"https://id.kb.se/term/ktg/X\""         | "category:\"https://id.kb.se/term/ktg/X\""
        "category:\"https://id.kb.se/term/ktg/Y\""         | "category:\"https://id.kb.se/term/ktg/Y\""
        "category:\"https://id.kb.se/term/ktg/Z\""         | "category:\"https://id.kb.se/term/ktg/Z\""
        "category:(X Y)"                                   | "category:(X Y)"
    }

    def "treat invalid code segment as free text"() {
        expect:
        new QueryTree(input, disambiguate).tree() == parsed

        where:
        input                              | parsed
        "k:v"                              | new FreeText("k:v")
        "k=v"                              | new FreeText("k=v")
        "k : v"                            | new FreeText("k : v")
        "k :v"                             | new FreeText("k :v")
        "k: v"                             | new FreeText("k: v")
        "k:()"                             | new FreeText("k:()")
        "k : ()"                           | new FreeText("k : ()")
        "k :()"                            | new FreeText("k :()")
        "k: ()"                            | new FreeText("k: ()")
        "k:(v)"                            | new FreeText("k:(v)")
        "k:(v )"                           | new FreeText("k:(v )")
        "k:( v )"                          | new FreeText("k:( v )")
        "k:( v)"                           | new FreeText("k:( v)")
        "k:( \"v\" )"                      | new FreeText("k:( \"v\" )")
        "k:(\"v\" )"                       | new FreeText("k:(\"v\" )")
        "k:( \"v\")"                       | new FreeText("k:( \"v\")")
        "k:(k : v)"                        | new FreeText("k:(k : v)")
        "k:(x OR (a b))"                   | new FreeText("k:(x OR (a b))")
        "x k:(x OR (a b)  ) y"             | new FreeText("x k:(x OR (a b)  ) y")
        "p1:v1 k:v x"                      | new And([p1v1, new FreeText("k:v x")])
        "k:v x p1:v1"                      | new And([new FreeText("k:v x"), p1v1])
        "k:(v) p1:v1"                      | new And([new FreeText("k:(v)"), p1v1])
        "k:(a (b OR c)) p1:v1"             | new And([new FreeText("k:(a (b OR c))"), p1v1])
        "p1:v1 x k:(\"a\" (b OR c)) p1:v2" | new And([p1v1, new FreeText("x k:(\"a\" (b OR c))"), p1v2])
        "k:p1:v1"                          | new FreeText("k:p1:v1")
        "p1:k:v1"                          | new Condition(p1, Operator.EQUALS, new FreeText("k:v1"))
        "p1:k:\"v1\""                      | new Condition(p1, Operator.EQUALS, new FreeText("k:\"v1\""))
        "p1:p1:v1"                         | new Condition(p1, Operator.EQUALS, new FreeText("p1:v1"))
        "p1:(p1:v1)"                       | new Condition(p1, Operator.EQUALS, new FreeText("p1:v1"))
        "p1:(p1:v1 p1:v2 x) y p1:v1"       | new And([new Condition(p1, Operator.EQUALS, new FreeText("p1:v1 p1:v2 x")), new FreeText("y"), p1v1])
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
}
