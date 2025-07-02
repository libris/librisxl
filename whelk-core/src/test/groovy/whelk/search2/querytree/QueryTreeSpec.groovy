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
        "(x OR y) p1:x"                             | "(x OR y) p1:x"
        "x OR y"                                    | "x OR y"
        "(x OR y) z"                                | "(x OR y) z"
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
                                                "@id": "/find?_q=%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                        ]
                                ], [
                                        "or": [[
                                                       "property" : [
                                                               "@id"  : "p3",
                                                               "@type": "ObjectProperty"
                                                       ],
                                                       "notEquals": "v3",
                                                       "up"       : [
                                                               "@id": "/find?_q=something+p4:%22v:4%22+includeA"
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
                                                               "@id": "/find?_q=something+NOT+p3:v3+includeA"
                                                       ],
                                                       "_key"    : "p4",
                                                       "_value"  : "\"v:4\""
                                               ]],
                                        "up": [
                                                "@id": "/find?_q=something+includeA"
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
                                                                                        "@id": "/find?_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                                                ],
                                                                                "_key"     : "p1",
                                                                                "_value"   : "A"
                                                                        ]
                                                                ],
                                                                "value" : "excludeA",
                                                                "up"    : [
                                                                        "@id": "/find?_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29+includeA"
                                                                ]
                                                        ]
                                                ]
                                        ],
                                        "value" : "includeA",
                                        "up"    : [
                                                "@id": "/find?_q=something+%28NOT+p3:v3+OR+p4:%22v:4%22%29"
                                        ]
                                ]],
                        "up" : [
                                "@id": "/find?_q=*"
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

    def "add node"() {
        given:
        Node nodeToAdd = QueryTreeBuilder.buildTree(add, disambiguate)
        QueryTree qt = new QueryTree(q, disambiguate)

        expect:
        qt.add(nodeToAdd).toQueryString() == result

        where:
        q                | add                                    | result
        null             | 'p1:v1'                                | 'p1:v1'
        'p1:v1'          | 'p2:v2'                                | 'p1:v1 p2:v2'
        'p1:v1'          | 'p1:v1'                                | 'p1:v1'
        'p1:v1 OR p2:v2' | 'p2:v2'                                | '(p1:v1 OR p2:v2) p2:v2'
        'p1:v1 p2:v2'    | 'p1:v1'                                | 'p1:v1 p2:v2'
        'p1:v1 p2:v2'    | 'p3:v3'                                | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2'    | 'p1:v1 p3:v3'                          | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2'    | 'p1:v1 p2:v2'                          | 'p1:v1 p2:v2'
        'p1:v1 p2:v2'    | '((p1:v1 p2:v2) OR p3:v3) p2:v2 p3:v3' | 'p1:v1 p2:v2 ((p1:v1 p2:v2) OR p3:v3) p3:v3'
    }

    def "remove node"() {
        given:
        QueryTree queryTree = new QueryTree(q, disambiguate)
        List<Node> nodesToMatch = remove.collect { QueryTreeBuilder.buildTree(it, disambiguate) }
        List<Node> nodesToRemove = queryTree.allDescendants().filter(nodesToMatch::contains).toList();

        expect:
        queryTree.remove(nodesToRemove).toQueryString() == result

        where:
        q                              | remove                               | result
        'p1:v1 p2:v2'                  | ['p1:v1']                            | 'p2:v2'
        'p1:v1 p2:v2'                  | ['p3:v3']                            | 'p1:v1 p2:v2'
        'p1:v1 p2:v2 p3:v3'            | ['p3:v3']                            | 'p1:v1 p2:v2'
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1 p2:v2 p3:v3']                | '*'
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1 p2:v2']                      | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1', 'p2:v2']                   | 'p3:v3'
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1 p2:v2 p3:v3 p4:v4']          | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2 p3:v3'            | ['p1:v1', 'p2:v2', 'p3:v3', 'p4:v4'] | '*'
        'p1:v1 p2:v2 p3:v3 p4:v4'      | ['p1:v1', 'p2:v2', 'p3:v3']          | 'p4:v4'
        'p1:v1 (p2:v2 OR p3:v3) p4:v4' | ['p1:v1', 'p4:v4']                   | 'p2:v2 OR p3:v3'
        'p1:v1 (p2:v2 OR p3:v3) p4:v4' | ['p1:v1', 'p2:v2 OR p3:v3']          | 'p4:v4'
        'p1:v1 (p2:v2 OR p3:v3) p4:v4' | ['p3:v3']                            | 'p1:v1 p2:v2 p4:v4'
    }

    def "replace node"() {
        given:
        QueryTree queryTree = new QueryTree(q, disambiguate)
        Node nodeToMatch = QueryTreeBuilder.buildTree(replace, disambiguate)
        Node nodeToReplace = queryTree.allDescendants().find(nodeToMatch::equals)
        Node replacementNode = QueryTreeBuilder.buildTree(replacement, disambiguate)

        expect:
        queryTree.replace(nodeToReplace, replacementNode).toQueryString() == result

        where:
        q                              | replace             | replacement      | result
        'p1:v1 p2:v2'                  | 'p2:v2'             | 'p3:v3'          | 'p1:v1 p3:v3'
        'p1:v1 p2:v2'                  | 'p3:v3'             | 'p4:v4'          | 'p1:v1 p2:v2'
        'p1:v1 p2:v2 p3:v3'            | 'p1:v1 p2:v2 p3:v3' | 'x y z'          | 'x y z'
        'p1:v1 p2:v2 p3:v3'            | 'p1:v1 p2:v2'       | 'x y z'          | 'p1:v1 p2:v2 p3:v3'
        'p1:v1 p2:v2 p3:v3'            | 'p2:v2'             | 'p4:v4 OR p5:v5' | 'p1:v1 (p4:v4 OR p5:v5) p3:v3'
        'p1:v1 (p4:v4 OR p5:v5) p3:v3' | 'p5:v5'             | 'p6:v6 p7:v7'    | 'p1:v1 (p4:v4 OR (p6:v6 p7:v7)) p3:v3'
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
