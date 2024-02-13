package whelk.search

import spock.lang.Specification
import whelk.Whelk
import whelk.xlql.Operator
import whelk.xlql.QueryTree
import whelk.xlql.SimpleQueryTree

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
        esQuery['bool']['must'][0]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Kalle"
        }
        esQuery['bool']['must'][1]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Anka"
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

    def "Query tree to ES query: Free text grouping"() {
        given:
        String queryString = "Bamse or (Kalle Anka)"
        QueryTree qt = xlqlQuery.getQueryTree(xlqlQuery.getSimpleQueryTree(queryString))
        Map esQuery = xlqlQuery.getEsQuery(qt)

        expect:
        esQuery['bool']['should'][0]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Bamse"
        }
        esQuery['bool']['should'][1]['bool']['must'][0]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Kalle"
        }
        esQuery['bool']['should'][1]['bool']['must'][1]['bool']['should'].every { s ->
            s['simple_query_string']['query'] == "Anka"
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
                'variable' : 'textQuery',
                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                'value'    : 'Kalle',
                'operator' : Operator.EQUALS.termKey,
                'up'       : '/find?_q=*'
        ]
    }

    def "Mapping: Simple phrase"() {
        given:
        String queryString = "\"Kalle Anka\""
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'variable' : 'textQuery',
                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                'value'    : 'Kalle Anka',
                'operator' : Operator.EQUALS.termKey,
                'up'       : '/find?_q=*'
        ]
    }

    def "Mapping: Free text conjunction"() {
        given:
        String queryString = "Kalle Anka"
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'and': [
                        [
                                'variable' : 'textQuery',
                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                'value'    : 'Kalle',
                                'operator' : Operator.EQUALS.termKey,
                                'up'       : '/find?_q=Anka'
                        ],
                        [
                                'variable' : 'textQuery',
                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                'value'    : 'Anka',
                                'operator' : Operator.EQUALS.termKey,
                                'up'       : '/find?_q=Kalle'
                        ]
                ],
                'up' : '/find?_q=*'
        ]
    }

    def "Mapping: Free text grouping + nested"() {
        given:
        String queryString = "(Kalle and not (Anka or Blomqvist)) or Bosse"
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        // TODO: Review necessity of flattenCodes/flattenNegations
        xlqlQuery.toMappings(sqt) == [
                'or': [
                        [
                                'and': [
                                        [
                                                'variable' : 'textQuery',
                                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                                'value'    : 'Kalle',
                                                'operator' : Operator.EQUALS.termKey,
                                                'up'       : '/find?_q=(NOT Anka AND NOT Blomqvist) OR Bosse'
                                        ],
                                        [
                                                'and': [
                                                        [
                                                                'variable' : 'textQuery',
                                                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                                                'value'    : 'Anka',
                                                                'operator' : Operator.NOT_EQUALS.termKey,
                                                                'up'       : '/find?_q=(Kalle AND NOT Blomqvist) OR Bosse'
                                                        ],
                                                        [
                                                                'variable' : 'textQuery',
                                                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                                                'value'    : 'Blomqvist',
                                                                'operator' : Operator.NOT_EQUALS.termKey,
                                                                'up'       : '/find?_q=(Kalle AND NOT Anka) OR Bosse'
                                                        ]
                                                ],
                                                'up' : '/find?_q=Kalle OR Bosse'
                                        ]
                                ],
                                'up' : '/find?_q=Bosse'
                        ],
                        [
                                'variable' : 'textQuery',
                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                'value'    : 'Bosse',
                                'operator' : Operator.EQUALS.termKey,
                                'up'       : '/find?_q=Kalle AND (NOT Anka AND NOT Blomqvist)'
                        ]
                ],
                'up': '/find?_q=*'
        ]
    }

    def "Mapping: Free text phrase + fields"() {
        given:
        String queryString = "\"Kalle Anka\" år > 2020 not ämne: Hästar"
        SimpleQueryTree sqt = xlqlQuery.getSimpleQueryTree(queryString)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'and': [
                        [
                                'variable' : 'textQuery',
                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                'value'    : 'Kalle Anka',
                                'operator' : Operator.EQUALS.termKey,
                                'up'       : '/find?_q=year > 2020 AND NOT subject: Hästar'
                        ],
                        [
                                'variable' : 'year',
                                'predicate': whelk.jsonld.vocabIndex['year'],
                                'value'    : '2020',
                                'operator' : Operator.GREATER_THAN.termKey,
                                'up'       : '/find?_q="Kalle Anka" AND NOT subject: Hästar'
                        ],
                        [
                                'variable' : 'subject',
                                'predicate': whelk.jsonld.vocabIndex['subject'],
                                'value'    : 'Hästar',
                                'operator' : Operator.NOT_EQUALS.termKey,
                                'up'       : '/find?_q="Kalle Anka" AND year > 2020'
                        ]
                ],
                'up' : '/find?_q=*'
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
                                'variable' : 'instanceOf.subject.@id',
                                'predicate': [
                                        'propertyChainAxiom': [
                                                whelk.jsonld.vocabIndex['instanceOf'],
                                                whelk.jsonld.vocabIndex['subject'],
                                        ]
                                ],
                                'value'    : "sao:Fysik",
                                'operator' : Operator.EQUALS.termKey,
                                'up'       : '/find?_q=instanceOf.subject._str: rymd'
                        ],
                        [
                                'variable' : 'instanceOf.subject._str',
                                'predicate': [
                                        'propertyChainAxiom': [
                                                whelk.jsonld.vocabIndex['instanceOf'],
                                                whelk.jsonld.vocabIndex['subject'],
                                        ]
                                ],
                                'value'    : 'rymd',
                                'operator' : Operator.EQUALS.termKey,
                                'up'       : '/find?_q=instanceOf.subject.@id: "sao:Fysik"'
                        ]
                ],
                'up' : '/find?_q=*'
        ]
    }
}