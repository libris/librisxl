package whelk.search

import spock.lang.Specification
import whelk.Whelk
import whelk.xlql.QueryTreeBuilder

class XLQLQuerySpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static XLQLQuery xlqlQuery = new XLQLQuery(whelk)

    // The following assume a static dataset
    def "Simple free text string"() {
        given:
        def classicInput = ['q': ["Kalle"] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ["Kalle"] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple free text phrase"() {
        given:
        def classicInput = ['q': ["\"Kalle Anka\""] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ["\"Kalle Anka\""] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple free text conjunction"() {
        given:
        def classicInput = ['q': ['Kalle Anka'] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ['Kalle Anka'] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple free text disjunction"() {
        given:
        def classicInput = ['q': ["Kalle | Anka"] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ["Kalle or Anka"] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple free text negation"() {
        given:
        def classicInput = ['q': ["-Sverige"] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ["not Sverige"] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        /*
         In this case it makes sense that we get less hits from the xlql query,
         since the xlql query translates to
         !(simpleQuery or boostedSoft or boostedExact)
         rather than
         !simpleQuery or !boostedSoft or !boostedExact
         Thus an improvement.
         */
        xlqlEsResponse['totalHits'] < classicEsResponse['totalHits']
    }

    def "Free text negation"() {
        given:
        def classicInput = ['q': ["Anka -Kalle"] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ["Anka not Kalle"] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Free text grouping 1"() {
        given:
        def classicInput = ['q': ["Bamse | (Kalle Anka)"] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ["Bamse or (Kalle Anka)"] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Free text grouping 2"() {
        given:
        def classicInput = ['q': ["(Kalle|Musse) (Pigg|Anka)"] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ["(Musse or Kalle) and (Pigg or Anka)"] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Free text nested"() {
        given:
        def classicInput = ['q': ["(Kalle (Anka|Blomkvist)) (Disney|Astrid) -Sverige"] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ["(Kalle and (Anka or Blomkvist) not Sverige) and (Disney or Astrid)"] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple field"() {
        given:
        def classicInput = ['issuanceType': ['Serial'] as String[], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ['issuanceType: Serial'] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Combined fields + disambiguation"() {
        given:
        def classicInput = ['issuanceType': ['Serial'] as String[], 'meta.encodingLevel': ['marc:FullLevel'], '_debug': ['esQuery'] as String[]]
        def xlqlInput = ['_q': ['utgivningssätt: Serial and beskrivningsnivå="marc:FullLevel"'] as String[], '_debug': ['esQuery'] as String[]]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Free text + fields + grouping"() {
        given:
        def classicInput = [
                'q'                 : ['fåglar'] as String[],
                'issuanceType'      : ['Serial', 'Monograph'] as String[],
                'meta.encodingLevel': ['marc:FullLevel'],
                '_debug'            : ['esQuery'] as String[]
        ]
        def xlqlInput = [
                '_q'    : ['fåglar and utgivningssätt: (Serial or Monograph) and beskrivningsnivå="marc:FullLevel"'] as String[],
                '_debug': ['esQuery'] as String[]
        ]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Mapping: Simple free text"() {
        given:
        def input = "Kalle"
        def sqt = xlqlQuery.queryTreeBuilder.toSimpleQueryTree(input)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'variable' : 'textQuery',
                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                'value'    : 'Kalle',
                'operator' : QueryTreeBuilder.Operator.EQUALS,
                'up'       : '/find?q=*'
        ]
    }

    def "Mapping: Simple phrase"() {
        given:
        def input = "\"Kalle Anka\""
        def sqt = xlqlQuery.queryTreeBuilder.toSimpleQueryTree(input)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'variable' : 'textQuery',
                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                'value'    : 'Kalle Anka',
                'operator' : QueryTreeBuilder.Operator.EQUALS,
                'up'       : '/find?q=*'
        ]
    }

    def "Mapping: Free text conjunction"() {
        given:
        def input = "Kalle Anka"
        def sqt = xlqlQuery.queryTreeBuilder.toSimpleQueryTree(input)

        expect:
        xlqlQuery.toMappings(sqt) == [
                'and': [
                        [
                                'variable' : 'textQuery',
                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                'value'    : 'Kalle',
                                'operator' : QueryTreeBuilder.Operator.EQUALS,
                                'up'       : '/find?_q=Anka'
                        ],
                        [
                                'variable' : 'textQuery',
                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                'value'    : 'Anka',
                                'operator' : QueryTreeBuilder.Operator.EQUALS,
                                'up'       : '/find?_q=Kalle'
                        ]
                ],
                'up' : '/find?q=*'
        ]
    }

    def "Mapping: Free text grouping + nested"() {
        given:
        def input = "(Kalle and not (Anka or Blomqvist)) or Bosse"
        def sqt = xlqlQuery.queryTreeBuilder.toSimpleQueryTree(input)

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
                                                'operator' : QueryTreeBuilder.Operator.EQUALS,
                                                'up'       : '/find?_q=(NOT Anka AND NOT Blomqvist) OR Bosse'
                                        ],
                                        [
                                                'and': [
                                                        [
                                                                'variable' : 'textQuery',
                                                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                                                'value'    : 'Anka',
                                                                'operator' : QueryTreeBuilder.Operator.NOT_EQUALS,
                                                                'up'       : '/find?_q=(Kalle AND NOT Blomqvist) OR Bosse'
                                                        ],
                                                        [
                                                                'variable' : 'textQuery',
                                                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                                                'value'    : 'Blomqvist',
                                                                'operator' : QueryTreeBuilder.Operator.NOT_EQUALS,
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
                                'operator' : QueryTreeBuilder.Operator.EQUALS,
                                'up'       : '/find?_q=Kalle AND (NOT Anka AND NOT Blomqvist)'
                        ]
                ],
                'up': '/find?q=*'
        ]
    }

    def "Mapping: Free text + fields"() {
        given:
        def input = "Kalle år > 2020 not ämne: Hästar"
        def sqt = xlqlQuery.queryTreeBuilder.toSimpleQueryTree(input)
        def mappings = xlqlQuery.toMappings(sqt)

        expect:
        mappings == [
                'and': [
                        [
                                'variable' : 'textQuery',
                                'predicate': whelk.jsonld.vocabIndex['textQuery'],
                                'value'    : 'Kalle',
                                'operator' : QueryTreeBuilder.Operator.EQUALS,
                                'up'       : '/find?_q=year > 2020 AND NOT subject: Hästar'
                        ],
                        [
                                'variable' : 'year',
                                'predicate': whelk.jsonld.vocabIndex['year'],
                                'value'    : '2020',
                                'operator' : QueryTreeBuilder.Operator.GREATER_THAN,
                                'up'       : '/find?_q=Kalle AND NOT subject: Hästar'
                        ],
                        [
                                'variable' : 'subject',
                                'predicate': whelk.jsonld.vocabIndex['subject'],
                                'value'    : 'Hästar',
                                'operator' : QueryTreeBuilder.Operator.NOT_EQUALS,
                                'up'       : '/find?_q=Kalle AND year > 2020'
                        ]
                ],
                'up' : '/find?q=*'
        ]
    }
}
