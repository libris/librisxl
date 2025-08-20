package whelk.search2

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.querytree.Link
import whelk.search2.querytree.Property
import whelk.search2.querytree.TestData

class ObjectQuerySpec extends Specification {
    JsonLd jsonLd = TestData.getJsonLd()
    ESSettings esMappings = new ESSettings(TestData.getEsMappings(), new ESSettings.Boost([:]))

    def "build agg query with curated predicates"() {
        given:
        Link object = new Link("https://libris.kb.se/fcrtpljz1qp2bdv#it")
        List<Property> predicates = ['p4', 'p6'].collect { new Property(it, jsonLd) }
        Map aggQuery = ObjectQuery.buildPAggQuery(object, predicates, jsonLd, List.of(), esMappings)

        expect:
        aggQuery == [
                "_p": [
                        "filters": [
                                "filters": [
                                        "p4": [
                                                "bool": [
                                                        "filter": [
                                                                "term": [
                                                                        "p4.@id": "https://libris.kb.se/fcrtpljz1qp2bdv#it"
                                                                ]
                                                        ]
                                                ]
                                        ],
                                        "p6": [
                                                "bool": [
                                                        "must": [
                                                                "nested": [
                                                                        "path" : "p3",
                                                                        "query": [
                                                                                "bool": [
                                                                                        "filter": [
                                                                                                "term": [
                                                                                                        "p3.p4.@id": "https://libris.kb.se/fcrtpljz1qp2bdv#it"
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
    }
}
