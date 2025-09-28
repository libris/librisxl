package whelk.search2.querytree

import spock.lang.Specification
import whelk.search2.Disambiguate
import whelk.search2.ESSettings

import static whelk.search2.querytree.QueryTreeBuilder.buildTree

class GroupSpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()

    def "flatten children on instantiation"() {
        given:
        var andTree = buildTree("x x p1:v1 (p1:v1 p2:v2 (p2:v2 p3:v3)) (p1:v1 p2:v2) (p1:v1 OR p2:v2)", disambiguate)
        var orTree = buildTree("((x OR p1:v1 OR (p1:v1 OR p2:v2 OR (p2:v2 OR p3:v3))) OR (p1:v1 OR p2:v2)) (p1:v1 p2:v2)", disambiguate)

        expect:
        andTree.toString() == 'x x p1:v1 p2:v2 p3:v3 (p1:v1 OR p2:v2)'
        orTree.toString() == '(x OR p1:v1 OR p2:v2 OR p3:v3) p1:v1 p2:v2'
    }

    def "equality check"() {
        given:
        def and1 = buildTree("p1:v1 p2:v2", disambiguate)
        def and2 = buildTree("p2:v2 p1:v1", disambiguate)
        def or1 = buildTree("p1:v1 OR p2:v2", disambiguate)
        def or2 = buildTree("p2:v2 OR p1:v1", disambiguate)

        expect:
        and1 == and2
        or1 == or2
        and1 != or1
        and2 != or2
        ((Set) [and1]).contains(and2)
        ((Set) [or1]).contains(or2)
        !((Set) [and1]).contains(or1)
        !((Set) [or1]).contains(and1)
    }

    def "reduce by condition"() {
        given:
        Group group = (Group) buildTree(_group, disambiguate)
        Node reduced = group.reduceByCondition { a, b -> (a == b) }

        expect:
        reduced.toString() == result

        where:
        _group                                    | result
        'p1:v1 p2:v2'                             | 'p1:v1 p2:v2'
        'p1:v1 (p1:v1 OR p2:v2)'                  | 'p1:v1'
        'p1:v1 OR (p1:v1 p2:v2)'                  | 'p1:v1'
        'p1:v1 (p1:v1 OR p3:v3)'                  | 'p1:v1'
        'p1:v1 (p2:v2 OR p3:v3)'                  | 'p1:v1 (p2:v2 OR p3:v3)'
        'p1:v1 OR (p2:v2 p3:v3)'                  | 'p1:v1 OR (p2:v2 p3:v3)'
        'p1:v1 (p1:v1 OR p2:v2 OR (p2:v2 p3:v3))' | 'p1:v1'
        'p1:v1 OR (p1:v1 2:v2 (p2:v2 OR p3:v3))'  | 'p1:v1'
        'p1:v1 (p2:v2 OR p3:v3 (p3:v3 OR p4:v4))' | 'p1:v1 (p2:v2 OR p3:v3)'
        'p1:v1 OR (p2:v2 p3:v3 (p3:v3 OR p4:v4))' | 'p1:v1 OR (p2:v2 p3:v3)'
        'p1:v1 (p2:v2 OR p3:v3 OR (p1:v1 p4:v4))' | 'p1:v1 (p2:v2 OR p3:v3 OR (p1:v1 p4:v4))'
        'p1:v1 OR (p2:v2 p3:v3 (p1:v1 OR p4:v4))' | 'p1:v1 OR (p2:v2 p3:v3 (p1:v1 OR p4:v4))'
        'p1:v1 p2:v2 (p2:v2 OR p3:v3)'            | 'p1:v1 p2:v2'
        'p1:v1 OR p2:v2 OR (p2:v2 p3:v3)'         | 'p1:v1 OR p2:v2'
        '(p1:v1 OR p2:v2) (p3:v3 OR p4:v4)'       | '(p1:v1 OR p2:v2) (p3:v3 OR p4:v4)'
        '(p1:v1 p2:v2) OR (p3:v3 p4:v4)'          | '(p1:v1 p2:v2) OR (p3:v3 p4:v4)'
        '(p1:v1 OR p2:v2) (p3:v3 OR p4:v4) p1:v1' | 'p1:v1 (p3:v3 OR p4:v4)'
        '(p1:v1 p2:v2) OR (p3:v3 p4:v4) OR p1:v1' | 'p1:v1 OR (p3:v3 p4:v4)'
        '(p1:v1 OR p2:v2) (p2:v2 OR p3:v3)'       | '(p1:v1 OR p2:v2) (p2:v2 OR p3:v3)'
        '(p1:v1 p2:v2) OR (p1:v1 p2:v2)'          | 'p1:v1 p2:v2'
    }

    def "To ES query: group nested"() {
        given:
        var tree = QueryTreeBuilder.buildTree("p3.p1:x p3.p4:y", disambiguate)
        ESSettings esSettings = new ESSettings(TestData.getEsMappings(), new ESSettings.Boost([:]))

        expect:
        tree.toEs(esSettings) == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "simple_query_string": [
                                                                 "default_operator": "AND",
                                                                 "query"           : "x",
                                                                 "analyze_wildcard": false,
                                                                 "fields"          : ["p3.p1^1.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator": "AND",
                                                                 "query"           : "y",
                                                                 "analyze_wildcard": false,
                                                                 "fields"          : ["p3.p4^1.0"]
                                                         ]
                                                 ]]
                                ]
                        ]
                ]
        ]
    }

    def "To ES query: group nested (negated)"() {
        given:
        var tree = QueryTreeBuilder.buildTree("p3.p1:x p3.p4:y", disambiguate)
        ESSettings esSettings = new ESSettings(TestData.getEsMappings(), new ESSettings.Boost([:]))

        expect:
        tree.toEs(esSettings) == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "simple_query_string": [
                                                                 "default_operator": "AND",
                                                                 "query"           : "x",
                                                                 "analyze_wildcard": false,
                                                                 "fields"          : ["p3.p1^1.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator": "AND",
                                                                 "query"           : "y",
                                                                 "analyze_wildcard": false,
                                                                 "fields"          : ["p3.p4^1.0"]
                                                         ]
                                                 ]]
                                ]
                        ]
                ]
        ]
    }

    def "To ES query: group nested 2"() {
        given:
        var tree = QueryTreeBuilder.buildTree("p3.p1:x p3.p4:y p2:e1", disambiguate)
        ESSettings esSettings = new ESSettings(TestData.getEsMappings(), new ESSettings.Boost([:]))

        expect:
        tree.toEs(esSettings) == [
                "bool": [
                        "must": [[
                                         "nested": [
                                                 "query": [
                                                         "bool": [
                                                                 "must": [[
                                                                                  "simple_query_string": [
                                                                                          "default_operator": "AND",
                                                                                          "query"           : "x",
                                                                                          "analyze_wildcard": false,
                                                                                          "fields"          : ["p3.p1^1.0"]
                                                                                  ]
                                                                          ], [
                                                                                  "simple_query_string": [
                                                                                          "default_operator": "AND",
                                                                                          "query"           : "y",
                                                                                          "analyze_wildcard": false,
                                                                                          "fields"          : ["p3.p4^1.0"]
                                                                                  ]
                                                                          ]]
                                                         ]
                                                 ],
                                                 "path" : "p3"
                                         ]
                                 ], [
                                         "bool": [
                                                 "filter": [
                                                         "term": [
                                                                 "p2": "E1"
                                                         ]
                                                 ]
                                         ]
                                 ]]
                ]
        ]
    }

    def "To ES query: group nested 3"() {
        given:
        var tree = QueryTreeBuilder.buildTree("p3.p1:x (p3.p4:y OR p3.p4:z)", disambiguate)
        ESSettings esSettings = new ESSettings(TestData.getEsMappings(), new ESSettings.Boost([:]))

        expect:
        tree.toEs(esSettings) == [
                "nested": [
                        "path" : "p3",
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "simple_query_string": [
                                                                 "default_operator": "AND",
                                                                 "query"           : "x",
                                                                 "analyze_wildcard": false,
                                                                 "fields"          : ["p3.p1^1.0"]
                                                         ]
                                                 ], [
                                                         "bool": [
                                                                 "should": [[
                                                                                    "simple_query_string": [
                                                                                            "default_operator": "AND",
                                                                                            "query"           : "y",
                                                                                            "analyze_wildcard": false,
                                                                                            "fields"          : ["p3.p4^1.0"]
                                                                                    ]
                                                                            ], [
                                                                                    "simple_query_string": [
                                                                                            "default_operator": "AND",
                                                                                            "query"           : "z",
                                                                                            "analyze_wildcard": false,
                                                                                            "fields"          : ["p3.p4^1.0"]
                                                                                    ]
                                                                            ]]
                                                         ]
                                                 ]]
                                ]
                        ]
                ]
        ]
    }

    def "To ES query: group nested 4"() {
        given:
        var tree = QueryTreeBuilder.buildTree("p3.p1:x p3.p4:y p3.p1:a p3.p4:b", disambiguate)
        ESSettings esSettings = new ESSettings(TestData.getEsMappings(), new ESSettings.Boost([:]))

        expect:
        tree.toEs(esSettings) == [
                "nested": [
                        "query": [
                                "bool": [
                                        "must": [[
                                                         "simple_query_string": [
                                                                 "default_operator": "AND",
                                                                 "query"           : "x",
                                                                 "analyze_wildcard": false,
                                                                 "fields"          : ["p3.p1^1.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator": "AND",
                                                                 "query"           : "y",
                                                                 "analyze_wildcard": false,
                                                                 "fields"          : ["p3.p4^1.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator": "AND",
                                                                 "query"           : "a",
                                                                 "analyze_wildcard": false,
                                                                 "fields"          : ["p3.p1^1.0"]
                                                         ]
                                                 ], [
                                                         "simple_query_string": [
                                                                 "default_operator": "AND",
                                                                 "query"           : "b",
                                                                 "analyze_wildcard": false,
                                                                 "fields"          : ["p3.p4^1.0"]
                                                         ]
                                                 ]]
                                ]
                        ],
                        "path" : "p3"
                ]
        ]
    }
}
