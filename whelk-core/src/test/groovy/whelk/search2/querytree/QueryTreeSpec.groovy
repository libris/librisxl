package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate

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
import static DummyNodes.v2
import static DummyNodes.v3
import static DummyNodes.pathV1
import static DummyNodes.pathV2
import static DummyNodes.pathV3

import static whelk.search2.Disambiguate.Rdfs.RDF_TYPE


class QueryTreeSpec extends Specification {
    def "to search mapping"() {
        given:
        Disambiguate.freeTextDefinition = ['prefLabel': 'freetext query']
        def tree = and([
                ft1,
                or([propV(prop1, neq, v1), propV2]),
                abf('x', propV3, ['prefLabel': 'filter x'])
        ])

        expect:
        new QueryTree(tree).toSearchMapping([:]) == [
                'and': [
                        [
                                'property': ['prefLabel': 'freetext query'],
                                'equals'  : 'ft1',
                                'up'      : ['@id': '/find?_i=&_q=%28NOT+p1:v1+OR+%22p:2%22:%22v:2%22%29+x']
                        ],
                        [
                                'or': [
                                        [
                                                'property' : ['prefLabel': 'p1'],
                                                'notEquals': 'v1',
                                                'up'       : ['@id': '/find?_i=ft1&_q=ft1+%22p:2%22:%22v:2%22+x']
                                        ],
                                        [
                                                'property': ['prefLabel': 'p2'],
                                                'equals'  : ['prefLabel': 'v2'],
                                                'up'      : ['@id': '/find?_i=ft1&_q=ft1+NOT+p1:v1+x']
                                        ]
                                ],
                                'up': ['@id': '/find?_i=ft1&_q=ft1+x']
                        ],
                        [
                                'object': ['prefLabelByLang': ['prefLabel': 'filter x']],
                                'value' : 'x',
                                'up'    : ['@id': '/find?_i=ft1&_q=ft1+%28NOT+p1:v1+OR+%22p:2%22:%22v:2%22%29']
                        ]
                ],
                'up' : ['@id': '/find?_i=&_q=*']
        ]
    }

    def "normalize free text"() {
        given:
        QueryTree qt = new QueryTree(and([
                ft('x'),
                ft('y'),
                or([ft('x'), ft('y')]),
                ft('a b c'),
                ft('d'),
                ft('e:f'),
                notFt('g'),
                ft('h'),
                ft('i')
        ]))
        qt.normalizeFreeText()

        expect:
        qt.tree == and([
                ft('x y \"a b c\" d \"e:f\" h i'),
                or([ft('x'), ft('y')]),
                notFt('g')
        ])
    }

    def "add node to top level of tree"() {
        expect:
        new QueryTree(tree).addToTopLevel(nodeToAdd).tree == result

        where:
        tree   | nodeToAdd | result
        null   | pathV1    | pathV1
        pathV1 | pathV2    | andXY
        pathV1 | pathV1    | pathV1
        orXY   | pathV2    | and([orXY, pathV2])
        andXY  | pathV1    | andXY
        andXY  | pathV3    | andXYZ
    }

    def "exclude node from tree"() {
        given:
        // We don't want a new instance of Or(x, y) in the tree, hence the flattenChildren set to false
        And andInstance = new And([orXY, pathV3], false)
        QueryTree qt = new QueryTree(andInstance)

        expect:
        qt.excludeFromTree(orXY).tree == pathV3
        qt.excludeFromTree(pathV3).tree == orXY
        qt.excludeFromTree(pathV1).tree == and([pathV2, pathV3])
        qt.excludeFromTree(pathV2).tree == and([pathV1, pathV3])
        qt.excludeFromTree(andInstance).tree == null
        // A node's content is irrelevant, the given input must refer to a specific instance in the tree
        qt.excludeFromTree(pathV(path3, eq, v3)).tree == qt.tree
        qt.excludeFromTree(or([pathV1, pathV2])).tree == qt.tree
    }

    def "remove all top level PropertyValue nodes having a range operator if their property matches the input"() {
        given:
        QueryTree qt = new QueryTree(and([
                propV(prop1, lt, v1),
                propV(prop1, eq, v1),
                or([propV(prop1, lt, v1), propV(prop1, gt, v1)]),
                propV(prop1, gt, v1),
                propV(prop2, gt, v1)
        ]))

        expect:
        qt.removeTopLevelPropValueWithRangeIfPropEquals(prop1).tree == and([
                propV(prop1, eq, v1),
                or([propV(prop1, lt, v1), propV(prop1, gt, v1)]),
                propV(prop2, gt, v1)
        ])
    }

    def "remove all PropertyValue nodes from top level if their property matches the input"() {
        given:
        QueryTree qt = new QueryTree(and([
                propV(prop1, lt, v1),
                propV(prop1, eq, v1),
                or([propV(prop1, lt, v1), propV(prop1, gt, v1)]),
                propV(prop1, gt, v1),
                propV(prop2, gt, v1)
        ]))

        expect:
        qt.removeTopLevelPropValueIfPropEquals(prop1).tree == and([
                or([propV(prop1, lt, v1), propV(prop1, gt, v1)]),
                propV(prop2, gt, v1)
        ])
    }

    def "get top level free text as string"() {
        expect:
        new QueryTree(tree).getTopLevelFreeText() == result

        where:
        tree                               | result
        and([ft('x y z'), pathV1, pathV2]) | 'x y z'
        ft('x y z')                        | 'x y z'
        pathV1                             | ''
        or([ft('x'), ft('y')])             | ''
        null                               | ''
    }

    def "to query string"() {
        expect:
        new QueryTree(tree).toString() == result

        where:
        tree                                | result
        null                                | "*"
        pathV1                              | "p1:v1"
        pathV2                              | "p2:\"v:2\""
        propV1                              | "p1:v1"
        propV2                              | "\"p:2\":\"v:2\""
        pathV(new Path(['x', 'y']), eq, v1) | "x.y:v1"
        ft1                                 | "ft1"
        notPathV1                           | "NOT p1:v1"
        orXY                                | "p1:v1 OR p2:\"v:2\""
        andXYZ                              | "p1:v1 p2:\"v:2\" p3:v3"
        and([ft1, orXY, notPathV3])         | "ft1 (p1:v1 OR p2:\"v:2\") NOT p3:v3"
    }
}
