package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.Operator

class QueryTreeBuilderSpec extends Specification {
    static Disambiguate disambiguate = TestData.getDisambiguate()
    static JsonLd jsonLd = TestData.getJsonLd()

    static var p1 = Property.getProperty("p1", jsonLd)
    static var p1v1 = new Condition(p1, Operator.EQUALS, new FreeText("v1"))
    static var p1v2 = new Condition(p1, Operator.EQUALS, new FreeText("v2"))

    def "treat invalid code segment as free text"() {
        expect:
        QueryTreeBuilder.buildTree(input, disambiguate) == parsed

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
