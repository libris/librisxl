package whelk.search2

import spock.lang.Specification
import whelk.JsonLd

class FacetTreeSpec extends Specification {

    JsonLd jsonLd

    void setup() {
        jsonLd = GroovyMock(JsonLd.class)
        jsonLd.toTermKey(_ as String) >> { String s -> s }
        jsonLd.toTermId(_ as String) >> { String s -> s }
    }

    def "Single observation should return list with one observation"() {
        given:
        jsonLd.getDirectSubclasses("parent") >> []
        jsonLd.getSuperClasses("parent") >> []

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

        jsonLd.getSuperClasses("child") >> ["parent"]
        jsonLd.getSuperClasses("parent") >> []


        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                                                  | sorted
        [["object": ["@id": "parent"]],
         ["object": ["@id": "child"]]]             |    [["object": ["@id": "parent"], "_children": [["object": ["@id": "child"]]]]]
    }

    def "Sort one parent with two children, superclasses of root should be ignored"() {
        given:
        jsonLd.getDirectSubclasses("root") >> ["child1", "child2"]
        jsonLd.getDirectSubclasses("Resource") >> ["root"]
        jsonLd.getDirectSubclasses("child1") >> []
        jsonLd.getDirectSubclasses("child2") >> []

        jsonLd.getSuperClasses("child1") >> ["root", "Resource"]
        jsonLd.getSuperClasses("child2") >> ["child1", "root", "Resource"]
        jsonLd.getSuperClasses("root") >> ["Resource"]
        jsonLd.getSuperClasses("Resource") >> []

        jsonLd.getSubClasses("Resource") >> ["root", "child1", "child2"]
        jsonLd.getSubClasses("root") >> ["child1", "child2"]
        jsonLd.getSubClasses("child1") >> []
        jsonLd.getSubClasses("child2") >> []


        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "root"]],
         ["object": ["@id": "child1"]],
         ["object": ["@id": "child2"]]]     |   [["object": ["@id": "root"],
                                                  "_children": [["object": ["@id": "child1"]],
                                                                ["object": ["@id": "child2"]]]]]
    }

    def "Sort one parent with one child that has one child"() {
        given:
        jsonLd.getDirectSubclasses("root") >> ["child1"]
        jsonLd.getDirectSubclasses("child1") >> ["child2"]
        jsonLd.getDirectSubclasses("child2") >> []

        jsonLd.getSuperClasses("child1") >> ["root"]
        jsonLd.getSuperClasses("child2") >> ["child1", "root"]
        jsonLd.getSuperClasses("root") >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "root"]],
         ["object": ["@id": "child1"]],
         ["object": ["@id": "child2"]]]     |   [["object": ["@id": "root"],
                                                  "_children": [["object": ["@id": "child1"],
                                                               "_children": [["object": ["@id": "child2"]]]]]]]
    }

    def "One parent, two children"() {
        given:
        jsonLd.getDirectSubclasses("root") >> ["child1", "child2"]
        jsonLd.getDirectSubclasses("child1") >> []
        jsonLd.getDirectSubclasses("child2") >> []

        jsonLd.getSuperClasses("child1") >> ["root"]
        jsonLd.getSuperClasses("child2") >> ["root"]
        jsonLd.getSuperClasses("root") >> []

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
        jsonLd.getDirectSubclasses("absentRoot") >> ["root1", "root2", "root3"]
        jsonLd.getDirectSubclasses("root1") >> []
        jsonLd.getDirectSubclasses("root2") >> []
        jsonLd.getDirectSubclasses("root3") >> []

        jsonLd.getSuperClasses("root1") >> ["absentRoot"]
        jsonLd.getSuperClasses("root2") >> ["absentRoot"]
        jsonLd.getSuperClasses("root3") >> ["absentRoot"]
        jsonLd.getSuperClasses("absentRoot") >> []

        jsonLd.getSubClasses("absentRoot") >> ["root1", "root2", "root3"]
        jsonLd.getSubClasses("root1") >> []
        jsonLd.getSubClasses("root2") >> []
        jsonLd.getSubClasses("root3") >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "root1"]],
         ["object": ["@id": "root2"]],
         ["object": ["@id": "root3"]]]          |    [["totalItems" : 0, "view": ["@id" : "fake"], "object": ["@id": "absentRoot"], "_children": [["object": ["@id": "root1"]],
                                                      ["object": ["@id": "root2"]],
                                                      ["object": ["@id": "root3"]]]]]
    }


    def "Root with one intermediate observation before one child"() {
        given:
        jsonLd.getDirectSubclasses("root") >> ["intermediate"]
        jsonLd.getDirectSubclasses("intermediate") >> ["child"]
        jsonLd.getDirectSubclasses("child") >> []

        jsonLd.getSuperClasses("child") >> ["intermediate", "root"]
        jsonLd.getSuperClasses("root") >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        // TODO: don't depend on exact form of fake observation
        where:
        observations                           | sorted
        [["object": ["@id": "root"]],
         ["object": ["@id": "child"]]]         |   [["object": ["@id": "root"],
                                                     "_children": [["totalItems" : 0, "view": ["@id" : "fake"], "object": ["@id": "intermediate"],
                                                                    "_children": [["object": ["@id": "child"]]]]]]]
    }

    def "Absent root, two children"() {
        given:
        jsonLd.getDirectSubclasses("root") >> ["child1", "child2"]
        jsonLd.getDirectSubclasses("child1") >> []
        jsonLd.getDirectSubclasses("child2") >> []

        jsonLd.getSuperClasses("child1") >> ["root"]
        jsonLd.getSuperClasses("child2") >> ["root"]
        jsonLd.getSuperClasses("root") >> []

        jsonLd.getSubClasses("root") >> ["child1", "child2"]
        jsonLd.getSubClasses("child1") >> []
        jsonLd.getSubClasses("child2") >> []

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "child1"]],
         ["object": ["@id": "child2"]]]        |    [["totalItems" : 0, "view": ["@id" : "fake"], "object": ["@id": "root"],
                                                      "_children" : [["object": ["@id": "child1"]],
                                                                     ["object": ["@id": "child2"]]]]]
    }
}
