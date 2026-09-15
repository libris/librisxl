package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.Operator
import whelk.search2.TestData
import whelk.search2.querytree.node.And
import whelk.search2.querytree.node.Condition
import whelk.search2.querytree.selector.Property
import whelk.search2.querytree.value.FreeText
import whelk.search2.querytree.node.Node

class QueryTreeBuilderSpec extends Specification {
    static Disambiguate disambiguate = TestData.getDisambiguate()
    static JsonLd jsonLd = TestData.getJsonLd()

    static var p1 = Property.getProperty("p1", jsonLd)
    static var p1v1 = new Condition(p1, Operator.EQUALS, new FreeText("v1"))
    static var p1v2 = new Condition(p1, Operator.EQUALS, new FreeText("v2"))

    static def textQuery(String s) {
        return new FreeText(s).asNode();
    }

    def "treat invalid code segment as free text"() {
        given:
        var tree = QueryTreeBuilder.buildTree(input, disambiguate)

        expect:
        tree == parsed

        where:
        input                              | parsed
        "k:v"                              | textQuery("k:v")
        "k=v"                              | textQuery("k=v")
        "k : v"                            | textQuery("k : v")
        "k :v"                             | textQuery("k :v")
        "k: v"                             | textQuery("k: v")
        "k:()"                             | textQuery("k:()")
        "k : ()"                           | textQuery("k : ()")
        "k :()"                            | textQuery("k :()")
        "k: ()"                            | textQuery("k: ()")
        "k:(v)"                            | textQuery("k:(v)")
        "k:(v )"                           | textQuery("k:(v )")
        "k:( v )"                          | textQuery("k:( v )")
        "k:( v)"                           | textQuery("k:( v)")
        "k:( \"v\" )"                      | textQuery("k:( \"v\" )")
        "k:(\"v\" )"                       | textQuery("k:(\"v\" )")
        "k:( \"v\")"                       | textQuery("k:( \"v\")")
        "k:(k : v)"                        | textQuery("k:(k : v)")
        "k:(x OR (a b))"                   | textQuery("k:(x OR (a b))")
        "x k:(x OR (a b)  ) y"             | textQuery("x k:(x OR (a b)  ) y")
        "p1:v1 k:v x"                      | new And([p1v1, textQuery("k:v x")])
        "k:v x p1:v1"                      | new And([textQuery("k:v x"), p1v1])
        "k:(v) p1:v1"                      | new And([textQuery("k:(v)"), p1v1])
        "k:(a (b OR c)) p1:v1"             | new And([textQuery("k:(a (b OR c))"), p1v1])
        "p1:v1 x k:(\"a\" (b OR c)) p1:v2" | new And([p1v1, textQuery("x k:(\"a\" (b OR c))"), p1v2])
        "k:p1:v1"                          | textQuery("k:p1:v1")
        "p1:k:v1"                          | new Condition(p1, Operator.EQUALS, new FreeText("k:v1"))
        "p1:k:\"v1\""                      | new Condition(p1, Operator.EQUALS, new FreeText("k:\"v1\""))
        "p1:p1:v1"                         | new Condition(p1, Operator.EQUALS, new FreeText("p1:v1"))
        "p1:(p1:v1)"                       | new Condition(p1, Operator.EQUALS, new FreeText("p1:v1"))
        "p1:(p1:v1 p1:v2 x) y p1:v1"       | new And([new Condition(p1, Operator.EQUALS, new FreeText("p1:v1 p1:v2 x")), textQuery("y"), p1v1])
    }

    def "concat simple free text segments"() {
        given:
        Node queryTree = QueryTreeBuilder.buildTree("x y (x OR y) \"a b c\" d \"e:f\" NOT g h i", disambiguate)
        Node subTree1 = QueryTreeBuilder.buildTree("x y \"a b c\" d \"e:f\" h i", disambiguate) // concatenated
        Node subTree2 = QueryTreeBuilder.buildTree("x OR y", disambiguate)
        Node subTree3 = QueryTreeBuilder.buildTree("NOT g", disambiguate)

        expect:
        queryTree == new And([subTree1, subTree2, subTree3])
    }
}
