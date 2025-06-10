package whelk.search2.querytree

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.AppParams
import whelk.search2.Disambiguate
import whelk.search2.EsBoost
import whelk.search2.Filter
import whelk.search2.Query
import whelk.search2.QueryParams
import whelk.search2.SelectedFilters

class QueryTreeSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "back to query string"() {
        expect:
        new QueryTree(input, disambiguate).toQueryString() == back

        where:
        input                                       | back
        null                                        | "*"
        "*"                                         | "*"
        "* p1:x"                                    | "p1:x"
        "x y"                                       | "x y"
        "\"x y\""                                   | "\"x y\""
        "\"x y\" z"                                 | "\"x y\" z"
        "x OR y z"                                  | "x OR y z"
        "x OR \"y z\""                              | "x OR \"y z\""
        "NOT x y"                                   | "NOT x y"
        "NOT (x y)"                                 | "NOT (x y)"
        "NOT (x OR y)"                              | "NOT (x OR y)"
        "p1:x"                                      | "p1:x"
        "p1:\"x y\""                                | "p1:\"x y\""
        "p1:\"x OR y\""                             | "p1:\"x OR y\""
        "p1:(x y)"                                  | "p1:(x y)"
        "p1:(x OR y)"                               | "p1:(x OR y)"
        "NOT p1:(x OR y)"                           | "NOT p1:(x OR y)"
        "NOT p1:(NOT x)"                            | "p1:x"
        "p2:e1"                                     | "p2:e1"
        "p2:(e1 e2)"                                | "p2:e1 p2:e2"
        "p2:(e1 OR e2)"                             | "p2:e1 OR p2:e2"
        "NOT p2:(e1 e2)"                            | "NOT p2:e1 OR NOT p2:e2"
        "NOT p2:(e1 OR e2)"                         | "NOT p2:e1 NOT p2:e2"
        "type:(t1 OR t2)"                           | "type:t1 OR type:t2"
        "p3:x"                                      | "p3:x"
        "p3:\"https://id.kb.se/x\""                 | "p3:\"https://id.kb.se/x\""
        "p3:\"sao:x\""                              | "p3:\"sao:x\""
        "p3:(\"sao:x\" \"sao:y\")"                  | "p3:\"sao:x\" p3:\"sao:y\""
        "p3:(\"x y\" z \"sao:x\" \"sao:y\")"        | "p3:(\"x y\" z) p3:\"sao:x\" p3:\"sao:y\""
        "p3:(x (\"sao:x\" OR \"sao:y\"))"           | "p3:x (p3:\"sao:x\" OR p3:\"sao:y\")"
        "NOT p3:(NOT x)"                            | "p3:x"
        "NOT p3:(x y (\"sao:x\" OR NOT \"sao:y\"))" | "NOT p3:(x y) OR (NOT p3:\"sao:x\" p3:\"sao:y\")"
        "_x._y:z"                                   | "_x._y:z"
        "x p1:y includeA"                           | "x p1:y includeA"
        "p1>1990"                                   | "p1>1990"
        "NOT p1>1990"                               | "p1<=1990"
        "p1=1990"                                   | "p1:1990"
        "p12:1990-01-01"                            | "p12:1990-01-01"
        "NOT p12<=1990-01-01"                       | "p12>1990-01-01"
        "p12:\"1990-01-01T01:01\""                  | "p12:\"1990-01-01T01:01\""
    }

    def "convert to ES query"() {
        given:
        QueryTree tree = new QueryTree('(NOT p1:v1 OR p4:v4) something', disambiguate)
        EsBoost.Config esBoostConfig = EsBoost.Config.newBoostFieldsConfig(["_str^10"])

        expect:
        tree.toEs(jsonLd, TestData.getEsMappings(), esBoostConfig, [], []) ==
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
                                                                        'analyze_wildcard': true,
                                                                        'query'           : 'v1',
                                                                        'fields'          : ['p1^0.0']
                                                                ]
                                                        ]
                                                ]],
                                                [
                                                        'simple_query_string': [
                                                                'default_operator': 'AND',
                                                                'analyze_wildcard': true,
                                                                'query'           : 'v4',
                                                                'fields'          : ['p4._str^400.0']
                                                        ]
                                                ]
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
                                                "@id": "/find?_limit=20&_q=%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                        ]
                                ], [
                                        "or": [[
                                                       "property" : [
                                                               "@id"  : "p3",
                                                               "@type": "ObjectProperty"
                                                       ],
                                                       "notEquals": "v3",
                                                       "up"       : [
                                                               "@id": "/find?_limit=20&_q=something+p4:%22v:4%22+includeA"
                                                       ],
                                                       "_key"     : "p3",
                                                       "_value"   : "v3"
                                               ], [
                                                       "property": [
                                                               "@id"  : "p4",
                                                               "@type": "ObjectProperty"
                                                       ],
                                                       "equals"  : "\"v:4\"",
                                                       "up"      : [
                                                               "@id": "/find?_limit=20&_q=something+NOT+p3:v3+includeA"
                                                       ],
                                                       "_key"    : "p4",
                                                       "_value"  : "\"v:4\""
                                               ]],
                                        "up": [
                                                "@id": "/find?_limit=20&_q=something+includeA"
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
                                                                                        "@id": "/find?_limit=20&_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                                                ],
                                                                                "_key"     : "p1",
                                                                                "_value"   : "A"
                                                                        ]
                                                                ],
                                                                "value" : "excludeA",
                                                                "up"    : [
                                                                        "@id": "/find?_limit=20&_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                                ]
                                                        ]
                                                ]
                                        ],
                                        "value" : "includeA",
                                        "up"    : [
                                                "@id": "/find?_limit=20&_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29"
                                        ]
                                ]],
                        "up" : [
                                "@id": "/find?_limit=20&_q=*"
                        ]
                ]
    }

    def "concat simple free text on instantiation"() {
        given:
        QueryTree qt = new QueryTree("x y (x OR y) \"a b c\" d \"e:f\" NOT g h i", disambiguate)
        var ft1 = (FreeText) new QueryTree("x y \"a b c\" d \"e:f\" h i", disambiguate).tree
        var ft2 = (FreeText) QueryTreeBuilder.buildTree("x OR y", disambiguate)
        var ft3 = (FreeText) QueryTreeBuilder.buildTree("NOT g", disambiguate)

        expect:
        qt.tree == new And([ft1, ft2, ft3])
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

        SelectedFilters selectedFilters = new SelectedFilters(queryTree, siteFilters)

        queryTree.applySiteFilters(basicSearchMode, siteFilters, selectedFilters)

        expect:
        queryTree.getFiltered().toString() == filteredQuery

        where:
        origQuery            | filteredQuery
        "x"                  | "x NOT p1:A type:T1"
        "x type:T2"          | "x type:T2 NOT p1:A"
        "x type:T1"          | "x type:T1 NOT p1:A"
        "x NOT type:T2"      | "x NOT type:T2 NOT p1:A type:T1"
        "x NOT type:T1"      | "x NOT type:T1 NOT p1:A"
        "x type:T1 NOT p1:A" | "x type:T1 NOT p1:A"
        "x excludeA"         | "x excludeA type:T1"
        "x includeA"         | "x includeA type:T1"
        "x NOT excludeA"     | "x NOT excludeA type:T1"
        "x NOT includeA"     | "x excludeA type:T1"
        "x type:T2 includeA" | "x type:T2 includeA"
        "x p1:A"             | "x p1:A type:T1"
    }
}
