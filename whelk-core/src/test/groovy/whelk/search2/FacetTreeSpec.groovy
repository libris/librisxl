package whelk.search2

import spock.lang.Specification

class FacetTreeSpec extends Specification {

    Disambiguate disambiguate

    void setup() {
        disambiguate = GroovyMock(Disambiguate.class)
    }

    def "Single observation should return list with one observation"() {
        expect:
        def tree = new FacetTree(disambiguate)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                    | sorted
        [["object": ["@id": "parent"]]] | [["object": ["@id": "parent"]]]
    }

    def "Sort one parent and one child"() {
        given:
        disambiguate.isSubclassOf("child", "parent") >> {
            true
        }
        disambiguate.isSubclassOf("parent", "child") >> {
            false
        }

        expect:
        def tree = new FacetTree(disambiguate)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                                                  | sorted
        [["object": ["@id": "parent"]],
         ["object": ["@id": "child"]]]             |    [["object": ["@id": "parent"], "children": [["object": ["@id": "child"]]]]]
    }

    def "Sort one parent with two children"() {
        given:
        disambiguate.isSubclassOf("child1", "parent") >> {
            true
        }
        disambiguate.isSubclassOf("child2", "parent") >> {
            true
        }
        disambiguate.isSubclassOf("parent", "child1") >> {
            false
        }
        disambiguate.isSubclassOf("parent", "child2") >> {
            false
        }

        expect:
        def tree = new FacetTree(disambiguate)
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
        disambiguate.isSubclassOf("child1", "parent") >> {
            true
        }
        disambiguate.isSubclassOf("child2", "parent") >> {
            false
        }
        disambiguate.isSubclassOf("child2", "child1") >> {
            true
        }
        disambiguate.isSubclassOf("parent", "child1") >> {
            false
        }
        disambiguate.isSubclassOf("parent", "child2") >> {
            false
        }

        expect:
        def tree = new FacetTree(disambiguate)
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
        disambiguate.isSubclassOf("child1", "root") >> {
            true
        }
        disambiguate.isSubclassOf("child2", "root") >> {
            true
        }
        disambiguate.isSubclassOf("root", "child1") >> {
            false
        }
        disambiguate.isSubclassOf("root", "child2") >> {
            false
        }
        disambiguate.isSubclassOf("child1", "child2") >> {
            false
        }
        disambiguate.isSubclassOf("child2", "child1") >> {
            false
        }

        expect:
        def tree = new FacetTree(disambiguate)
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
        disambiguate.isSubclassOf(_, _) >> false

        expect:
        def tree = new FacetTree(disambiguate)
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
        disambiguate.isSubclassOf(_, _) >> true

        expect:
        def tree = new FacetTree(disambiguate)
        tree.sortObservationsAsTree(observations) == sorted

        where:
        observations                                            | sorted
        [["object": ["@id": "A"]], ["object": ["@id": "A"]]]    |   [["object": ["@id": "A"]], ["object": ["@id": "A"]]]
    }
}
