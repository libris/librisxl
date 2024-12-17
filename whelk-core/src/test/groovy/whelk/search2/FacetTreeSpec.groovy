package whelk.search2

import spock.lang.Specification
import whelk.JsonLd

class FacetTreeSpec extends Specification {

    JsonLd jsonLd

    void setup() {
        jsonLd = GroovyMock(JsonLd.class)
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
        jsonLd.isSubClassOf("child", "parent") >> {
            true
        }
        jsonLd.isSubClassOf("parent", "child") >> {
            false
        }

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                                                  | sorted
        [["object": ["@id": "parent"]],
         ["object": ["@id": "child"]]]             |    [["object": ["@id": "parent"], "children": [["object": ["@id": "child"]]]]]
    }

    def "Sort one parent with two children"() {
        given:
        jsonLd.isSubClassOf("child1", "parent") >> {
            true
        }
        jsonLd.isSubClassOf("child2", "parent") >> {
            true
        }
        jsonLd.isSubClassOf("parent", "child1") >> {
            false
        }
        jsonLd.isSubClassOf("parent", "child2") >> {
            false
        }

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "parent"]],
         ["object": ["@id": "child1"]],
         ["object": ["@id": "child2"]]]     |   [["object": ["@id": "parent"],
                                                  "children": [["object": ["@id": "child1"]],
                                                               ["object": ["@id": "child2"]]]]]
    }

    def "Sort one parent with one child that has one child"() {
        given:
        jsonLd.isSubClassOf("child1", "parent") >> {
            true
        }
        jsonLd.isSubClassOf("child2", "parent") >> {
            false
        }
        jsonLd.isSubClassOf("child2", "child1") >> {
            true
        }
        jsonLd.isSubClassOf("parent", "child1") >> {
            false
        }
        jsonLd.isSubClassOf("parent", "child2") >> {
            false
        }

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "parent"]],
         ["object": ["@id": "child1"]],
         ["object": ["@id": "child2"]]]     |   [["object": ["@id": "parent"],
                                                  "children": [["object": ["@id": "child1"],
                                                               "children": [["object": ["@id": "child2"]]]]]]]
    }

    def "One parent, two children"() {
        given:
        jsonLd.isSubClassOf("child1", "root") >> {
            true
        }
        jsonLd.isSubClassOf("child2", "root") >> {
            true
        }
        jsonLd.isSubClassOf("root", "child1") >> {
            false
        }
        jsonLd.isSubClassOf("root", "child2") >> {
            false
        }
        jsonLd.isSubClassOf("child1", "child2") >> {
            false
        }
        jsonLd.isSubClassOf("child2", "child1") >> {
            false
        }

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                           | sorted
        [["object": ["@id": "root"]],
         ["object": ["@id": "child1"]],
         ["object": ["@id": "child2"]]]        |    [["object": ["@id": "root"], "children" : [["object": ["@id": "child1"]],
                                                                                                ["object": ["@id": "child2"]]]]]
    }

    def "Three root nodes"() {
        given:
        jsonLd.isSubClassOf(_, _) >> false

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
        jsonLd.isSubClassOf(_, _) >> true

        expect:
        def tree = new FacetTree(jsonLd)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                                            | sorted
        [["object": ["@id": "A"]], ["object": ["@id": "A"]]]    |   [["object": ["@id": "A"]], ["object": ["@id": "A"]]]
    }
}
