package whelk.search

import spock.lang.Ignore
import spock.lang.Specification
import whelk.Whelk
import whelk.xlql.Operator
import whelk.xlql.QueryTree
import whelk.xlql.SimpleQueryTree

@Ignore
class XLQLQuerySpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static XLQLQuery xlqlQuery = new XLQLQuery(whelk)

    def "Query tree to ES query: Simple free text string"() {
        given:
        String queryString = "Kalle"
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['should'].every {
            it['simple_query_string']['query'] == "Kalle"
        }
    }

    def "Query tree to ES query: Simple free text phrase"() {
        given:
        String queryString = "\"Kalle Anka\""
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['should'].every {
            it['simple_query_string']['query'] == "\"Kalle Anka\""
        }
    }

    def "Query tree to ES query: Simple free text conjunction"() {
        given:
        String queryString = "Kalle Anka"
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Kalle Anka"
            s['simple_query_string']['default_operator'] == "AND"
        }
    }

    def "Query tree to ES query: Simple free text disjunction"() {
        given:
        String queryString = "Kalle or Anka"
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['should'][0]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Kalle"
        }
        esQuery['bool']['should'][1]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Anka"
        }
    }

    def "Query tree to ES query: Simple free text negation"() {
        given:
        String queryString = "not Kalle"
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['must_not']['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Kalle"
        }
    }

    def "Query tree to ES query: Free text negation"() {
        given:
        String queryString = "Kalle not Anka"
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['must'][0]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Kalle"
        }
        esQuery['bool']['must'][1]['bool']['must_not']['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Anka"
        }
    }

    def "Query tree to ES query: Free text disjunction"() {
        given:
        String queryString = "Bamse or Kalle Anka"
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['should'][0]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Bamse"
        }
        esQuery['bool']['should'][1]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Kalle Anka"
        }
    }

    def "Query tree to ES query: Simple field"() {
        given:
        String queryString = "upphovsuppgift: \"Astrid Lindgren\""
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery == ['bool': ['filter': ['simple_query_string': ['query': '"Astrid Lindgren"', 'fields': ['responsibilityStatement']]]]]

    }

    def "Query tree to ES query: Combined @vocab fields"() {
        given:
        String queryString = "utgivningssätt: Serial and beskrivningsnivå=\"marc:FullLevel\""
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['must'][0]['bool']['filter']['simple_query_string'] == ['query': 'Serial', 'fields': ['issuanceType']]
        esQuery['bool']['must'][1]['bool']['filter']['simple_query_string'] == ['query': 'marc:FullLevel', 'fields': ['meta.encodingLevel']]
    }

    def "Query tree to ES query: Free text + range"() {
        given:
        String queryString = "fåglar and (year >= 2010 or year < 2020)"
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['must'][0]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "fåglar"
        }
        esQuery['bool']['must'][1]['bool']['should'][0]['bool']['filter']['range']['year']['gte'] == '2010'
        esQuery['bool']['must'][1]['bool']['should'][1]['bool']['filter']['range']['year']['lt'] == '2020'
    }

    def "Mapping: Simple free text"() {
        given:
        String queryString = "Kalle"
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'property': whelk.jsonld.vocabIndex['textQuery'],
                'equals'  : 'Kalle',
                'up'      : ['@id': '/find?_q=*']
        ]
    }

    def "Mapping: Simple phrase + limit"() {
        given:
        String queryString = "\"Kalle Anka\""
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        xlqlQuery.toMappings(sqt, ['_limit=20']) == [
                'property': whelk.jsonld.vocabIndex['textQuery'],
                'equals'  : '"Kalle Anka"',
                'up'      : ['@id': '/find?_q=*&_limit=20']
        ]
    }

    def "Mapping: Free text"() {
        given:
        String queryString = "Kalle Anka"
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'property': whelk.jsonld.vocabIndex['textQuery'],
                'equals'  : 'Kalle Anka',
                'up'      : ['@id': '/find?_q=*']
        ]
    }

    def "Mapping: Free text grouping"() {
        given:
        String queryString = "(Kalle and not (Anka or Blomqvist)) or \"Bosse Persson\""
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'or': [
                        [
                                'and': [
                                        [
                                                'property': whelk.jsonld.vocabIndex['textQuery'],
                                                'equals'  : 'Kalle',
                                                'up'      : ['@id': '/find?_q=(NOT Anka AND NOT Blomqvist) OR "Bosse Persson"']
                                        ],
                                        [
                                                'and': [
                                                        [
                                                                'property' : whelk.jsonld.vocabIndex['textQuery'],
                                                                'notEquals': 'Anka',
                                                                'up'       : ['@id': '/find?_q=(Kalle AND NOT Blomqvist) OR "Bosse Persson"']
                                                        ],
                                                        [
                                                                'property' : whelk.jsonld.vocabIndex['textQuery'],
                                                                'notEquals': 'Blomqvist',
                                                                'up'       : ['@id': '/find?_q=(Kalle AND NOT Anka) OR "Bosse Persson"']
                                                        ]
                                                ],
                                                'up' : ['@id': '/find?_q=Kalle OR "Bosse Persson"']
                                        ]
                                ],
                                'up' : ['@id': '/find?_q="Bosse Persson"']
                        ],
                        [
                                'property': whelk.jsonld.vocabIndex['textQuery'],
                                'equals'  : '"Bosse Persson"',
                                'up'      : ['@id': '/find?_q=Kalle AND (NOT Anka AND NOT Blomqvist)']
                        ]
                ],
                'up': ['@id': '/find?_q=*']
        ]
    }

    def "Mapping: Free text + fields"() {
        given:
        String queryString = "Kalle Anka år > 2020 not ämne: Hästar"
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'and': [
                        [
                                'property': whelk.jsonld.vocabIndex['textQuery'],
                                'equals'  : 'Kalle Anka',
                                'up'      : ['@id': '/find?_q=year>2020 AND NOT subject:Hästar']
                        ],
                        [
                                'property'   : whelk.jsonld.vocabIndex['year'],
                                'greaterThan': '2020',
                                'up'         : ['@id': '/find?_q=Kalle Anka AND NOT subject:Hästar']
                        ],
                        [
                                'property' : whelk.jsonld.vocabIndex['subject'],
                                'notEquals': 'Hästar',
                                'up'       : ['@id': '/find?_q=Kalle Anka AND year>2020']
                        ]
                ],
                'up' : ['@id': '/find?_q=*']
        ]
    }

    def "Mapping: Property path"() {
        given:
        String queryString = "instanceOf.subject.@id: \"sao:Fysik\" and instanceOf.subject._str: rymd"
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'and': [
                        [
                                'property': [
                                        'propertyChainAxiom': [
                                                whelk.jsonld.vocabIndex['instanceOf'],
                                                whelk.jsonld.vocabIndex['subject'],
                                        ]
                                ],
                                'equals'  : xlqlQuery.lookUp("sao:Fysik").get(),
                                'up'      : ['@id': '/find?_q=instanceOf.subject._str:rymd']
                        ],
                        [
                                'property': [
                                        'propertyChainAxiom': [
                                                whelk.jsonld.vocabIndex['instanceOf'],
                                                whelk.jsonld.vocabIndex['subject'],
                                        ]
                                ],
                                'equals'  : 'rymd',
                                'up'      : ['@id': '/find?_q=instanceOf.subject.@id:"sao:Fysik"']
                        ]
                ],
                'up' : ['@id': '/find?_q=*']
        ]
    }

    def "quoting in up url"() {
        given:
        String queryString = '"har titel":"x!" or a b "c:d" f "g h i>j" k l'
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)
        Map mappings = xlqlQuery.toMappings(sqt)

        expect:
        mappings['or'][0]['up']['@id'] =='/find?_q=a b "c:d" f "g h i>j" k l'
        mappings['or'][1]['up']['@id'] =='/find?_q=hasTitle:"x!"'
    }
}