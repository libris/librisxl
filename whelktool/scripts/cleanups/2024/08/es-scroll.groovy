import whelk.search.ESQuery
import whelk.search.ElasticFind

var out = getReportWriter("ids")

/*
var find = new ElasticFind(new ESQuery(getWhelk()))

var query = [
        '@type': ['Person']
]

var t1 = System.currentTimeMillis()
find.findIds(query).forEach { id ->
    out.println(id)
}

var t2 = System.currentTimeMillis()
println("Took ${t2 - t1} ms")
*/


println("Starting")

/*
        Map query = [
                'bool': ['filter': ['bool': ['must': [
                        "simple_query_string": [
                                "query": "Person",
                                "fields": [
                                        "@type"
                                ],
                                "default_operator": "AND"
                        ]
                ]]] ]
        ]

         */

Map esQuery = [
        'bool': ['filter': [
                "bool": [
                        "must": [
                                [
                                        "nested": [
                                                "path": "@reverse.itemOf",
                                                "query": [
                                                        "bool": [
                                                                "must": [
                                                                        [
                                                                                "bool": [
                                                                                        "should": [
                                                                                                [
                                                                                                        "simple_query_string": [
                                                                                                                "query": "https://libris.kb.se/library/S",
                                                                                                                "fields": [
                                                                                                                        "@reverse.itemOf.heldBy.@id"
                                                                                                                ],
                                                                                                                "default_operator": "AND"
                                                                                                        ]
                                                                                                ]
                                                                                        ]
                                                                                ]
                                                                        ]
                                                                ]
                                                        ]
                                                ]
                                        ]
                                ]
                        ]
                ]
        ] ]
]

var t1 = System.currentTimeMillis()

getWhelk().elastic.getIds(esQuery).forEach { id ->
    out.println(id)
}

var t2 = System.currentTimeMillis()
println("Took ${t2 - t1} ms")