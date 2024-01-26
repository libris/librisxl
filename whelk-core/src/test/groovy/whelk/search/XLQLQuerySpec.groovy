package whelk.search

import spock.lang.Specification
import whelk.Whelk

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
}
