package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate
import whelk.search2.TestData
import whelk.search2.querytree.node.Condition
import whelk.search2.querytree.selector.Key
import whelk.search2.querytree.selector.Path
import whelk.search2.querytree.selector.Selector
import whelk.search2.querytree.value.Token

class SelectorSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "get expanded path"() {
        given:
        Selector p = ((Condition) QueryTreeBuilder.buildTree("$_p:v", disambiguate)).selector()

        expect:
        p.path().collect { it.toString() } == result

        where:
        _p      | result
        "p1"    | ["p1"]
        "p6"    | ["p3", "p4"]
        "p6.p1" | ["p3", "p4", "p1"]
    }

    def "quote the whole query key when any path element needs quoting"() {
        given:
        def path = new Path([
                new Key.RecognizedKey(new Token.Raw("x")),
                new Key.RecognizedKey(new Token.Quoted("y:z"))
        ])

        expect:
        path.queryKey() == '"x.y:z"'
    }

    def "don't quote the query key when no path element needs quoting"() {
        given:
        def path = new Path([
                new Key.RecognizedKey(new Token.Raw("x")),
                new Key.RecognizedKey(new Token.Raw("y"))
        ])

        expect:
        path.queryKey() == "x.y"
    }
}
