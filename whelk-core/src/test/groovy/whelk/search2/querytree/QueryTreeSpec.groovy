package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate
import whelk.search2.QueryParams

class QueryTreeSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

    def "to search mapping"() {
        given:
        def tree = QueryTreeBuilder.buildTree('something (NOT p3:v3 OR p4:"v:4") includeA', disambiguate)

        expect:
        new QueryTree(tree).toSearchMapping(new QueryParams([:])) == [
                'and': [
                        [
                                'property': ['@id': 'textQuery', '@type': 'DatatypeProperty'],
                                'equals'  : 'something',
                                'up'      : ['@id': '/find?_limit=200&_i=&_q=%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA']
                        ],
                        [
                                'or': [
                                        [
                                                'property' : ['@id': 'p3', '@type': 'ObjectProperty'],
                                                'notEquals': 'v3',
                                                'up'       : ['@id': '/find?_limit=200&_i=something&_q=something+p4:%22v:4%22+includeA'],
                                                '_key'     : 'p3',
                                                '_value'   : 'v3'
                                        ],
                                        [
                                                'property': ['@id': 'p4', '@type': 'ObjectProperty'],
                                                'equals'  : 'v:4',
                                                'up'      : ['@id': '/find?_limit=200&_i=something&_q=something+NOT+p3:v3+includeA'],
                                                '_key'    : 'p4',
                                                '_value'  : 'v:4'
                                        ]
                                ],
                                'up': ['@id': '/find?_limit=200&_i=something&_q=something+includeA']
                        ],
                        [
                                'object': ['prefLabelByLang': [:], '@type': 'Resource'],
                                'value' : 'includeA',
                                'up'    : ['@id': '/find?_limit=200&_i=something&_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29']
                        ]
                ],
                'up' : ['@id': '/find?_limit=200&_i=&_q=*']
        ]
    }

    def "normalize free text on instantiation"() {
        given:
        QueryTree qt = new QueryTree("x y (x OR y) \"a b c\" d \"e:f\" NOT g h i", disambiguate)
        var ft1 = (FreeText) new QueryTree("x y \"a b c\" d \"e:f\" h i", disambiguate).tree
        var ft2 = (FreeText) QueryTreeBuilder.buildTree("x", disambiguate)
        var ft3 = (FreeText) QueryTreeBuilder.buildTree("y", disambiguate)
        var ft4 = (FreeText) QueryTreeBuilder.buildTree("NOT g", disambiguate)

        expect:
        qt.tree == new And([
                ft1,
                new Or([ft2, ft3]),
                ft4
        ])
    }

    def "add node to top level of tree"() {
        given:
        Node add = QueryTreeBuilder.buildTree(_add, disambiguate)
        QueryTree tree = new QueryTree(_tree, disambiguate)

        expect:
        tree.addTopLevelNode(add).toString() == result

        where:
        _tree            | _add    | result
        null             | 'p1:v1' | 'p1:v1'
        'p1:v1'          | 'p2:v2' | 'p1:v1 p2:v2'
        'p1:v1'          | 'p1:v1' | 'p1:v1'
        'p1:v1 OR p2:v2' | 'p2:v2' | '(p1:v1 OR p2:v2) p2:v2'
        'p1:v1 p2:v2'    | 'p1:v1' | 'p1:v1 p2:v2'
        'p1:v1 p2:v2'    | 'p3:v3' | 'p1:v1 p2:v2 p3:v3'
    }

    def "omit node from tree"() {
        given:
        PathValue pv1 = (PathValue) QueryTreeBuilder.buildTree('p1:v1', disambiguate)
        PathValue pv2 = (PathValue) QueryTreeBuilder.buildTree('p2:v2', disambiguate)
        PathValue pv3 = (PathValue) QueryTreeBuilder.buildTree('p3:v3', disambiguate)
        Or or = new Or([pv1, pv2])
        // We don't want a new instance of Or in the tree, hence the flattenChildren set to false
        And and = new And([or, pv3], false)
        QueryTree qt = new QueryTree(and)

        expect:
        qt.omitNode(or).tree == pv3
        qt.omitNode(pv3).tree == or
        qt.omitNode(pv1).tree == new And([pv2, pv3])
        qt.omitNode(pv2).tree == new And([pv1, pv3])
        qt.omitNode(and).tree == null
        // A node's content is irrelevant, the given input must refer to a specific instance in the tree
        qt.omitNode(QueryTreeBuilder.buildTree('p3:v3', disambiguate)).tree == qt.tree
        qt.omitNode(QueryTreeBuilder.buildTree('p1:v1 OR p2:v2', disambiguate)).tree == qt.tree
    }

    def "get top level free text as string"() {
        expect:
        new QueryTree(tree, disambiguate).getFreeTextPart() == result

        where:
        tree                | result
        'x y z p1:v1 p2:v2' | 'x y z'
        'x y z'             | 'x y z'
        'p1:v1'             | ''
        'x OR y'            | ''
        null                | ''
    }

    def "to query string"() {
        expect:
        new QueryTree(tree, disambiguate).toQueryString() == result

        where:
        tree                                   | result
        null                                   | "*"
        "p1:v1"                                | "p1:v1"
        "p2:\"v:2\""                           | "p2:\"v:2\""
        "p5:v5"                                | "p5:v5"
        "_x._y:v1"                             | "_x._y:v1"
        "NOT p1:v1"                            | "NOT p1:v1"
        "p1:v1 OR p3:v3"                       | "p1:v1 OR p3:v3"
        "p1:v1 p2:\"v:2\" p3:v3"               | "p1:v1 p2:\"v:2\" p3:v3"
        "something (p1:v1 OR p3:v3) NOT p4:v4" | "something (p1:v1 OR p3:v3) NOT p4:v4"
        "something p4:v4 includeA"             | "something p4:v4 includeA"
    }
}
