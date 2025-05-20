package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.Disambiguate

class PathSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "expand"() {
        given:
        Path path = ((PathValue) QueryTreeBuilder.buildTree("$_path:v", disambiguate)).path()

        expect:
        path.expand(jsonLd).toString() == result

        where:
        _path   | result
        "p1"    | "p1"
        "p5"    | "meta.p5.@id"
        "p6"    | "p3.p4.@id"
        "p6.p1" | "p3.p4.p1"
    }

    def "get alternative paths for integral relations"() {
        given:
        Path.ExpandedPath path = ((PathValue) QueryTreeBuilder.buildTree("$_path:v", disambiguate)).path().expand(jsonLd)

        expect:
        path.getAltPaths(jsonLd, types).collect { it.toString() } == result

        where:
        _path | types        | result
        "p1"  | []           | ["p1"]
        "p1"  | ["T1"]       | ["instanceOf.p1", "p1"]
        "p1"  | ["T2"]       | ["hasInstance.p1", "p1"]
        "p1"  | ["T1", "T2"] | ["instanceOf.p1", "hasInstance.p1", "p1"]
        "p1"  | ["T3"]       | ["p1"]
        "p7"  | ["T1"]       | ["p7.@id"]
        "p7"  | ["T2"]       | ["hasInstance.p7.@id"]
        "p7"  | ["T1", "T2"] | ["hasInstance.p7.@id", "p7.@id"]
        "p7"  | ["T3"]       | ["p7.@id"]
        "p8"  | ["T1"]       | ["instanceOf.p8.@id"]
        "p8"  | ["T2"]       | ["p8.@id"]
        "p8"  | ["T1", "T2"] | ["instanceOf.p8.@id", "p8.@id"]
        "p8"  | ["T3"]       | ["p8.@id"]
        "p9"  | ["T1"]       | ["p9.@id"]
        "p9"  | ["T2"]       | ["p9.@id"]
        "p9"  | ["T1", "T2"] | ["p9.@id"]
        "p9"  | ["T3"]       | ["p9.@id"]
    }
}
