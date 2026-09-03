package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate
import whelk.search2.TestData
import whelk.search2.querytree.node.Node

class RdfSubjectTypeSpec extends Specification {
    static Disambiguate disambiguate = TestData.getDisambiguate()

    def "extract subject types from query tree"() {
        given:
        Node tree = QueryTreeBuilder.buildTree(q, disambiguate)
        RdfSubjectType rdfSubjectType = RdfSubjectType.extractFrom(tree)

        expect:
        rdfSubjectType.typeNames() == result

        where:
        q                                       | result
        'type:T1 p1:v1'                         | ["T1"]
        'type:(T1 T2) p1:v1'                    | ["T1"] // TODO?
        '(type:T1 p1:v1) OR (type:T2 p2:v2)'    | ["T1", "T2"]
        'p1:v1 p2:v2'                           | []
        'type:(T1 OR T2) p1:v1'                 | ["T1", "T2"]
        '(type:T1 OR p1:v1) (type:T2 OR p2:v2)' | []
    }
}
