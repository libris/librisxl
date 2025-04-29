package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.AppParams
import whelk.search2.Disambiguate
import whelk.search2.Filter
import whelk.search2.Query
import whelk.search2.QueryParams

class QueryTreeSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "convert to ES query"() {
        given:
        QueryTree tree = new QueryTree('(NOT p1:v1 OR p2:v2) something', disambiguate)

        expect:
        tree.toEs(jsonLd, TestData.getEsMappings(), ['_str^10'], []) ==
                ['bool': [
                        'must': [
                                [
                                        'simple_query_string': [
                                                'default_operator': 'AND',
                                                'analyze_wildcard': true,
                                                'query'           : 'something',
                                                'fields'          : ['_str^10.0']
                                        ]
                                ],
                                ['bool': [
                                        'should': [
                                                ['bool': [
                                                        'must_not': [
                                                                'simple_query_string': [
                                                                        'default_operator': 'AND',
                                                                        'query'           : 'v1',
                                                                        'fields'          : ['p1']
                                                                ]
                                                        ]
                                                ]],
                                                ['bool': [
                                                        'must': [
                                                                'simple_query_string': [
                                                                        'default_operator': 'AND',
                                                                        'query'           : 'v2',
                                                                        'fields'          : ['p2^400']
                                                                ]
                                                        ]
                                                ]]
                                        ]
                                ]]
                        ]
                ]]
    }

    def "to search mapping"() {
        given:
        def tree = QueryTreeBuilder.buildTree('something (NOT p3:v3 OR p4:"v:4") includeA', disambiguate)

        expect:
        new QueryTree(tree).toSearchMapping(new QueryParams([:])) ==
                [
                        "and": [[
                                        "property": [
                                                "@id"  : "textQuery",
                                                "@type": "DatatypeProperty"
                                        ],
                                        "equals"  : "something",
                                        "up"      : [
                                                "@id": "/find?_limit=200&_q=%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                        ]
                                ], [
                                        "or": [[
                                                       "property" : [
                                                               "@id"  : "p3",
                                                               "@type": "ObjectProperty"
                                                       ],
                                                       "notEquals": "v3",
                                                       "up"       : [
                                                               "@id": "/find?_limit=200&_q=something+p4:%22v:4%22+includeA"
                                                       ],
                                                       "_key"     : "p3",
                                                       "_value"   : "v3"
                                               ], [
                                                       "property": [
                                                               "@id"  : "p4",
                                                               "@type": "ObjectProperty"
                                                       ],
                                                       "equals"  : "v:4",
                                                       "up"      : [
                                                               "@id": "/find?_limit=200&_q=something+NOT+p3:v3+includeA"
                                                       ],
                                                       "_key"    : "p4",
                                                       "_value"  : "v:4"
                                               ]],
                                        "up": [
                                                "@id": "/find?_limit=200&_q=something+includeA"
                                        ]
                                ], [
                                        "object": [
                                                "prefLabelByLang": [:],
                                                "alias"          : "includeA",
                                                "raw"            : "NOT excludeA",
                                                "@type"          : "Resource",
                                                "parsedFilter"   : [
                                                        "not": [
                                                                "object": [
                                                                        "prefLabelByLang": [:],
                                                                        "alias"          : "excludeA",
                                                                        "raw"            : "NOT p1:A",
                                                                        "@type"          : "Resource",
                                                                        "parsedFilter"   : [
                                                                                "property" : [
                                                                                        "@id"  : "p1",
                                                                                        "@type": "DatatypeProperty"
                                                                                ],
                                                                                "notEquals": "A",
                                                                                "up"       : [
                                                                                        "@id": "/find?_limit=200&_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                                                ],
                                                                                "_key"     : "p1",
                                                                                "_value"   : "A"
                                                                        ]
                                                                ],
                                                                "value" : "excludeA",
                                                                "up"    : [
                                                                        "@id": "/find?_limit=200&_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                                ]
                                                        ]
                                                ]
                                        ],
                                        "value" : "includeA",
                                        "up"    : [
                                                "@id": "/find?_limit=200&_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29"
                                        ]
                                ]],
                        "up" : [
                                "@id": "/find?_limit=200&_q=*"
                        ]
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

    def "apply site filters"() {
        given:
        QueryTree queryTree = new QueryTree(origQuery, disambiguate)
        def basicSearchMode = Query.SearchMode.STANDARD_SEARCH
        AppParams.DefaultSiteFilter dsf1 = new AppParams.DefaultSiteFilter(TestData.excludeFilter, [basicSearchMode] as Set)
        AppParams.DefaultSiteFilter dsf2 = new AppParams.DefaultSiteFilter(new Filter("type:T1"), [basicSearchMode] as Set)
        AppParams.DefaultSiteFilter dsf3 = new AppParams.DefaultSiteFilter(new Filter("type:T2"), [] as Set)
        AppParams.OptionalSiteFilter osf = new AppParams.OptionalSiteFilter(TestData.includeFilter, [basicSearchMode] as Set)

        AppParams.SiteFilters siteFilters = new AppParams.SiteFilters([dsf1, dsf2, dsf3], [osf])
        siteFilters.parse(disambiguate)

        queryTree.applySiteFilters(basicSearchMode, siteFilters)

        expect:
        queryTree.toString() == normalizedQuery
        queryTree.getFiltered().toString() == filteredQuery

        where:
        origQuery            | normalizedQuery      | filteredQuery
        "x"                  | "x"                  | "x excludeA type:T1"
        "x type:T2"          | "x type:T2"          | "x type:T2 excludeA"
        "x type:T1"          | "x"                  | "x type:T1 excludeA"
        "x NOT type:T2"      | "x NOT type:T2"      | "x NOT type:T2 excludeA type:T1"
        "x NOT type:T1"      | "x NOT type:T1"      | "x NOT type:T1 excludeA"
        "x type:T1 NOT p1:A" | "x"                  | "x type:T1 excludeA"
        "x excludeA"         | "x"                  | "x excludeA type:T1"
        "x includeA"         | "x includeA"         | "x includeA type:T1"
        "x NOT excludeA"     | "x includeA"         | "x includeA type:T1"
        "x NOT includeA"     | "x"                  | "x excludeA type:T1"
        "x type:T2 includeA" | "x type:T2 includeA" | "x type:T2 includeA"
    }
}
