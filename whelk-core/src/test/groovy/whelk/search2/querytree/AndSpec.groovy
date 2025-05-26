package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate

class AndSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

    def "contains"() {
        given:
        And and = (And) QueryTreeBuilder.buildTree(_and, disambiguate)
        Node node = QueryTreeBuilder.buildTree(_node, disambiguate)

        expect:
        and.contains(node) == result

        where:
        _and                             | _node                    | result
        'p1:v1 p2:v2'                    | 'p1:v1'                  | true
        'p1:v1 p2:v2 p3:v3'              | 'p1:v1 p2:v2'            | true
        'p1:v1 p2:v2'                    | 'p1:v1 p2:v2 p3:v3'      | false
        'p1:v1 p2:v2 p3:v3'              | '(p1:v1 p2:v2) p3:v3'    | true
        'p1:v1 p2:v2 p3:v3'              | '(p1:v1 OR p2:v2) p3:v3' | false
        '(p1:v1 OR p2:v2) (p1:v1 p2:v2)' | '(p1:v1 OR p2:v2) p1:v1' | true
        '(p1:v1 OR p2:v2) (p1:v1 p2:v2)' | '(p1:v1 OR p3:v3) p1:v1' | false
        '(p1:v1 OR p2:v2) (p1:v1 p2:v2)' | '(p1:v1 OR p2:v2) p3:v3' | false
    }

    def "remove"() {
        given:
        And and = (And) QueryTreeBuilder.buildTree(_and, disambiguate)
        Node node = QueryTreeBuilder.buildTree(_node, disambiguate)

        expect:
        and.remove(node)?.toString() == result

        where:
        _and                           | _node                     | result
        'p1:v1 p2:v2'                  | 'p1:v1'                   | 'p2:v2'
        'p1:v1 p2:v2'                  | 'p3:v3'                   | 'p1:v1 p2:v2'
        'p1:v1 p2:v2 p3:v3'            | 'p3:v3'                   | 'p1:v1 p2:v2'
        'p1:v1 p2:v2 p3:v3'            | 'p1:v1 p2:v2'             | 'p3:v3'
        'p1:v1 p2:v2 p3:v3'            | 'p1:v1 p2:v2 p3:v3'       | null
        'p1:v1 p2:v2 p3:v3'            | 'p1:v1 p2:v2 p3:v3 p4:v4' | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2 p3:v3 p4:v4'      | 'p1:v1 p2:v2 p3:v3'       | 'p4:v4'
        'p1:v1 (p2:v2 OR p3:v3) p4:v4' | 'p1:v1 p2:v2 p3:v3'       | 'p1:v1 (p2:v2 OR p3:v3) p4:v4'
        'p1:v1 (p2:v2 OR p3:v3) p4:v4' | 'p1:v1 (p2:v2 OR p3:v3)'  | 'p4:v4'
    }

    def "add"() {
        given:
        And and = (And) QueryTreeBuilder.buildTree(_and, disambiguate)
        Node node = QueryTreeBuilder.buildTree(_node, disambiguate)

        expect:
        and.add(node).toString() == result

        where:
        _and          | _node                                  | result
        'p1:v1 p2:v2' | 'p3:v3'                                | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2' | 'p2:v2'                                | 'p1:v1 p2:v2'
        'p1:v1 p2:v2' | 'p3:v3 p4:v4'                          | 'p1:v1 p2:v2 p3:v3 p4:v4'
        'p1:v1 p2:v2' | 'p1:v1 p3:v3'                          | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2' | 'p1:v1 p2:v2'                          | 'p1:v1 p2:v2'
        'p1:v1 p2:v2' | '(p1:v1 OR p2:v2) p3:v3'               | 'p1:v1 p2:v2 (p1:v1 OR p2:v2) p3:v3'
        'p1:v1 p2:v2' | 'p1:v1 p2:v2 p3:v3'                    | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2' | '((p1:v1 p2:v2) OR p3:v3) p2:v2 p3:v3' | 'p1:v1 p2:v2 ((p1:v1 p2:v2) OR p3:v3) p3:v3'
    }

    def "replace"() {
        given:
        And and = (And) QueryTreeBuilder.buildTree(_and, disambiguate)
        Node replace = QueryTreeBuilder.buildTree(_replace, disambiguate)
        Node replacement = QueryTreeBuilder.buildTree(_replacement, disambiguate)

        expect:
        and.replace(replace, replacement).toString() == result

        where:
        _and                                 | _replace         | _replacement  | result
        'p1:v1 p2:v2'                        | 'p1:v1'          | 'p3:v3'       | 'p2:v2 p3:v3'
        'p1:v1 p2:v2 p3:v3'                  | 'p2:v2 p3:v3'    | 'p4:v4'       | 'p1:v1 p4:v4'
        'p1:v1 p2:v2 p3:v3'                  | 'p2:v2 p3:v3'    | 'p1:v1'       | 'p1:v1'
        '(p1:v1 OR p2:v2) p3:v3'             | 'p1:v1 OR p2:v2' | 'p1:v1 p2:v2' | 'p3:v3 p1:v1 p2:v2'
        '(p1:v1 OR p2:v2) p3:v3 p4:v4 p5:v5' | 'p4:v4'          | 'p1:v1 p3:v3' | '(p1:v1 OR p2:v2) p3:v3 p5:v5 p1:v1'
        'p1:v1 p2:v2'                        | 'p3:v3'          | 'p4:v4'       | 'p1:v1 p2:v2'
    }

    def "collect ruling types"() {
        expect:
        ((And) QueryTreeBuilder.buildTree(and, disambiguate)).collectRulingTypes() == result

        where:
        and                                     | result
        'type:T1 p1:v1'                         | ["T1"]
        'type:(T1 T2) p1:v1'                    | ["T1", "T2"]
        'p1:v1 p2:v2'                           | []
        'type:(T1 OR T2) p1:v1'                 | ["T1", "T2"]
        '(type:T1 OR p1:v1) (type:T2 OR p2:v2)' | []
    }

    def "invert"() {
        expect:
        ((And) QueryTreeBuilder.buildTree(and, disambiguate)).getInverse().toString() == result

        where:
        and                   | result
        'p1:v1 p2:v2'         | 'NOT p1:v1 OR NOT p2:v2'
        'NOT p1:v1 p2:v2'     | 'p1:v1 OR NOT p2:v2'
        'NOT p1:v1 NOT p2:v2' | 'p1:v1 OR p2:v2'
        'p1:v1 p2>v2'         | 'NOT p1:v1 OR p2<=v2'
    }
}
