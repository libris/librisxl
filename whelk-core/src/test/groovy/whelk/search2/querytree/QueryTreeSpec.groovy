package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate
import whelk.search2.QueryParams

import static DummyNodes.abf
import static DummyNodes.and
import static DummyNodes.andXY
import static DummyNodes.andXYZ
import static DummyNodes.eq
import static DummyNodes.ft
import static DummyNodes.ft1
import static DummyNodes.gt
import static DummyNodes.lt
import static DummyNodes.neq
import static DummyNodes.notFt
import static DummyNodes.notPathV1
import static DummyNodes.notPathV3
import static DummyNodes.or
import static DummyNodes.orXY
import static DummyNodes.path3
import static DummyNodes.prop1
import static DummyNodes.prop2
import static DummyNodes.pathV
import static DummyNodes.propV
import static DummyNodes.propV1
import static DummyNodes.propV2
import static DummyNodes.propV3
import static DummyNodes.v1
import static DummyNodes.v3
import static DummyNodes.pathV1
import static DummyNodes.pathV2
import static DummyNodes.pathV3

//class QueryTreeSpec extends Specification {
//    Disambiguate disambiguate = TestData.getDisambiguate()
//
//    def "to search mapping"() {
//        given:
//        def tree = QueryTreeBuilder.buildTree('something (NOT p3:v3 OR p4:"v:4") includeA', disambiguate)
//
//        expect:
//        new QueryTree(tree).toSearchMapping(new QueryParams([:])) == [
//                'and': [
//                        [
//                                'property': ['@id': 'textQuery', '@type': 'DatatypeProperty'],
//                                'equals'  : 'something',
//                                'up'      : ['@id': '/find?_limit=200&_i=&_q=%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA']
//                        ],
//                        [
//                                'or': [
//                                        [
//                                                'property' : ['@id': 'p3', '@type': 'ObjectProperty'],
//                                                'notEquals': 'v3',
//                                                'up'       : ['@id': '/find?_limit=200&_i=something&_q=something+p4:%22v:4%22+includeA'],
//                                                '_key'     : 'p3',
//                                                '_value'   : 'v3'
//                                        ],
//                                        [
//                                                'property': ['@id': 'p4', '@type': 'ObjectProperty'],
//                                                'equals'  : 'v:4',
//                                                'up'      : ['@id': '/find?_limit=200&_i=something&_q=something+NOT+p3:v3+includeA'],
//                                                '_key'    : 'p4',
//                                                '_value'  : 'v:4'
//                                        ]
//                                ],
//                                'up': ['@id': '/find?_limit=200&_i=something&_q=something+includeA']
//                        ],
//                        [
//                                'object': ['prefLabelByLang': [:], '@type': 'Resource'],
//                                'value' : 'includeA',
//                                'up'    : ['@id': '/find?_limit=200&_i=something&_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29']
//                        ]
//                ],
//                'up' : ['@id': '/find?_limit=200&_i=&_q=*']
//        ]
//    }
//
//    def "normalize free text"() {
//        given:
//        QueryTree qt = new QueryTree(and([
//                ft('x'),
//                ft('y'),
//                or([ft('x'), ft('y')]),
//                ft('a b c'),
//                ft('d'),
//                ft('e:f'),
//                notFt('g'),
//                ft('h'),
//                ft('i')
//        ]))
//        qt.concatFreeText()
//
//        expect:
//        qt.tree == and([
//                ft('x y \"a b c\" d \"e:f\" h i'),
//                or([ft('x'), ft('y')]),
//                notFt('g')
//        ])
//    }
//
//    def "add node to top level of tree"() {
//        expect:
//        new QueryTree(tree).addTopLevelNode(nodeToAdd).tree == result
//
//        where:
//        tree   | nodeToAdd | result
//        null   | pathV1    | pathV1
//        pathV1 | pathV2    | andXY
//        pathV1 | pathV1    | pathV1
//        orXY   | pathV2    | and([orXY, pathV2])
//        andXY  | pathV1    | andXY
//        andXY  | pathV3    | andXYZ
//    }
//
//    def "exclude node from tree"() {
//        given:
//        // We don't want a new instance of Or(x, y) in the tree, hence the flattenChildren set to false
//        And andInstance = new And([orXY, pathV3], false)
//        QueryTree qt = new QueryTree(andInstance)
//
//        expect:
//        qt.omitNode(orXY).tree == pathV3
//        qt.omitNode(pathV3).tree == orXY
//        qt.omitNode(pathV1).tree == and([pathV2, pathV3])
//        qt.omitNode(pathV2).tree == and([pathV1, pathV3])
//        qt.omitNode(andInstance).tree == null
//        // A node's content is irrelevant, the given input must refer to a specific instance in the tree
//        qt.omitNode(pathV(path3, eq, v3)).tree == qt.tree
//        qt.omitNode(or([pathV1, pathV2])).tree == qt.tree
//    }
//
//    def "remove all top level PathValue nodes having a range operator if their (sole) property matches the input"() {
//        given:
//        QueryTree qt = new QueryTree(and([
//                propV(prop1, lt, v1),
//                propV(prop1, eq, v1),
//                or([propV(prop1, lt, v1), propV(prop1, gt, v1)]),
//                propV(prop1, gt, v1),
//                propV(prop2, gt, v1)
//        ]))
//
//        expect:
//        qt.removeTopLevelPathValueWithRangeIfPropEquals(prop1).tree == and([
//                propV(prop1, eq, v1),
//                or([propV(prop1, lt, v1), propV(prop1, gt, v1)]),
//                propV(prop2, gt, v1)
//        ])
//    }
//
//    def "remove all PathValue nodes from top level if their (sole) property matches the input"() {
//        given:
//        QueryTree qt = new QueryTree(and([
//                propV(prop1, lt, v1),
//                propV(prop1, eq, v1),
//                or([propV(prop1, lt, v1), propV(prop1, gt, v1)]),
//                propV(prop1, gt, v1),
//                propV(prop2, gt, v1)
//        ]))
//
//        expect:
//        qt.removeTopLevelPathValueIfPropEquals(prop1).tree == and([
//                or([propV(prop1, lt, v1), propV(prop1, gt, v1)]),
//                propV(prop2, gt, v1)
//        ])
//    }
//
//    def "get top level free text as string"() {
//        expect:
//        new QueryTree(tree).getFreeTextPart() == result
//
//        where:
//        tree                               | result
//        and([ft('x y z'), pathV1, pathV2]) | 'x y z'
//        ft('x y z')                        | 'x y z'
//        pathV1                             | ''
//        or([ft('x'), ft('y')])             | ''
//        null                               | ''
//    }
//
//    def "to query string"() {
//        expect:
//        new QueryTree(tree).toQueryString() == result
//
//        where:
//        tree                                                                              | result
//        null                                                                              | "*"
//        pathV1                                                                            | "p1:v1"
//        pathV2                                                                            | "p2:\"v:2\""
//        propV1                                                                            | "p1:v1"
//        propV2                                                                            | "p2:\"v:2\""
//        pathV(new Path([new Key.RecognizedKey('x'), new Key.RecognizedKey('y')]), eq, v1) | "x.y:v1"
//        ft1                                                                               | "ft1"
//        notPathV1                                                                         | "NOT p1:v1"
//        orXY                                                                              | "p1:v1 OR p2:\"v:2\""
//        andXYZ                                                                            | "p1:v1 p2:\"v:2\" p3:v3"
//        and([ft1, orXY, notPathV3])                                                       | "ft1 (p1:v1 OR p2:\"v:2\") NOT p3:v3"
//    }
//}
