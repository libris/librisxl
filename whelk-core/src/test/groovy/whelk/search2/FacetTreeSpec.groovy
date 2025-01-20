package whelk.search2

import spock.lang.Specification
import whelk.JsonLd

class FacetTreeSpec extends Specification {

    JsonLd jsonLd

    void setup() {
        jsonLd = GroovyMock(JsonLd.class)
        jsonLd.toTermKey(_ as String) >> { String s -> s }
    }

    def "Single observation should return list with one observation"() {
        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                    | sorted
        [["object": ["@id": "parent"]]] | [["object": ["@id": "parent"]]]
    }

    def "Sort one parent and one child"() {
        given:
        jsonLd.getDirectSubclasses("parent") >> ["child"]
        jsonLd.getDirectSubclasses("child") >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                                                  | sorted
        [["object": ["@id": "parent"]],
         ["object": ["@id": "child"]]]             |    [["object": ["@id": "parent"], "_children": [["object": ["@id": "child"]]]]]
    }

    def "Sort one parent with two children"() {
        given:
        jsonLd.getDirectSubclasses("parent") >> ["child1", "child2"]
        jsonLd.getDirectSubclasses("child1") >> []
        jsonLd.getDirectSubclasses("child2") >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "parent"]],
         ["object": ["@id": "child1"]],
         ["object": ["@id": "child2"]]]     |   [["object": ["@id": "parent"],
                                                  "_children": [["object": ["@id": "child1"]],
                                                               ["object": ["@id": "child2"]]]]]
    }

    def "Sort one parent with one child that has one child"() {
        given:
        jsonLd.getDirectSubclasses("parent") >> ["child1"]
        jsonLd.getDirectSubclasses("child1") >> ["child2"]
        jsonLd.getDirectSubclasses("child2") >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "parent"]],
         ["object": ["@id": "child1"]],
         ["object": ["@id": "child2"]]]     |   [["object": ["@id": "parent"],
                                                  "_children": [["object": ["@id": "child1"],
                                                               "_children": [["object": ["@id": "child2"]]]]]]]
    }

    def "One parent, two children"() {
        given:
        jsonLd.getDirectSubclasses("root") >> ["child1", "child2"]
        jsonLd.getDirectSubclasses("child1") >> []
        jsonLd.getDirectSubclasses("child2") >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "root"]],
         ["object": ["@id": "child1"]],
         ["object": ["@id": "child2"]]]        |    [["object": ["@id": "root"], "_children" : [["object": ["@id": "child1"]],
                                                                                                ["object": ["@id": "child2"]]]]]
    }

    def "Three root nodes"() {
        given:
        jsonLd.getDirectSubclasses(_) >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "root1"]],
         ["object": ["@id": "root2"]],
         ["object": ["@id": "root3"]]]          |    [["object": ["@id": "root1"]],
                                                      ["object": ["@id": "root2"]],
                                                      ["object": ["@id": "root3"]]]
    }

    def "Children should not be considered parents of themselves"() {
        given:
        jsonLd.getDirectSubclasses(_) >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                                            | sorted
        [["object": ["@id": "A"]], ["object": ["@id": "A"]]]    |   [["object": ["@id": "A"]], ["object": ["@id": "A"]]]
    }
}
