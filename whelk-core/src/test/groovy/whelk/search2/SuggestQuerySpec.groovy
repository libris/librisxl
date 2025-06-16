package whelk.search2

import spock.lang.Specification
import whelk.JsonLd
import whelk.search2.querytree.QueryTree
import whelk.search2.querytree.TestData

class SuggestQuerySpec extends Specification {
    Disambiguate disambiguate = TestData.getDisambiguate()
    JsonLd jsonLd = TestData.getJsonLd()

    def "get suggest query tree"() {
        given:
        List<String> defaultBaseTypes = ['T1', 'T2', 'T3']
        QueryTree queryTree = new QueryTree(q, disambiguate)

        QueryTree suggestQueryTree = SuggestQuery.getSuggestQueryTree(defaultBaseTypes, queryTree, jsonLd, disambiguate, cursor)

        expect:
        suggestQueryTree.toQueryString() == suggestQueryString

        where:
        q             | cursor | suggestQueryString
        'xyz'         | 3      | 'xyz ("rdf:type":T1 OR "rdf:type":T2 OR "rdf:type":T3)'
        'xyz p13:abc' | 3      | 'xyz p13:abc'
        'xyz p13:abc' | 11     | 'abc "rdf:type":T1 reverseLinks.totalItems>0'
        'xyz p13:abc' | 7      | 'xyz p13:abc'
    }
}
