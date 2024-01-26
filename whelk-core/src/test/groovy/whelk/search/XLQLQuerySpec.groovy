package whelk.search

import spock.lang.Specification
import whelk.Whelk

class XLQLQuerySpec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static XLQLQuery xlqlQuery = new XLQLQuery(whelk)

    // The following assume a static dataset
    def "Simple free text string"() {
        given:
        def classicInput = ['q': ["Kalle"], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ["Kalle"], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple free text phrase"() {
        given:
        def classicInput = ['q': ["\"Kalle Anka\""], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ["\"Kalle Anka\""], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple free text conjunction"() {
        given:
        def classicInput = ['q': ['Kalle Anka'], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ['Kalle Anka'], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple free text disjunction"() {
        given:
        def classicInput = ['q': ["Kalle | Anka"], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ["Kalle or Anka"], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple free text negation"() {
        given:
        def classicInput = ['q': ["-Sverige"], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ["not Sverige"], '_debug': ['esQuery']]

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
        def classicInput = ['q': ["Anka -Kalle"], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ["Anka not Kalle"], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Free text grouping 1"() {
        given:
        def classicInput = ['q': ["Bamse | (Kalle Anka)"], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ["Bamse or (Kalle Anka)"], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Free text grouping 2"() {
        given:
        def classicInput = ['q': ["(Kalle|Musse) (Pigg|Anka)"], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ["(Musse or Kalle) and (Pigg or Anka)"], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Free text nested"() {
        given:
        def classicInput = ['q': ["(Kalle (Anka|Blomkvist)) (Disney|Astrid) -Sverige"], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ["(Kalle and (Anka or Blomkvist) not Sverige) and (Disney or Astrid)"], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Simple field"() {
        given:
        def classicInput = ['issuanceType': ['Serial'], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ['issuanceType: Serial'], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }

    def "Combined fields + disambiguation"() {
        given:
        def classicInput = ['issuanceType': ['Serial'], 'meta.encodingLevel': ['marc:FullLevel'], '_debug': ['esQuery']]
        def xlqlInput = ['_q': ['utgivningssätt: Serial and beskrivningsnivå="marc:FullLevel"'], '_debug': ['esQuery']]

        Map classicEsResponse = xlqlQuery.esQuery.doQuery(classicInput)
        Map xlqlEsResponse = xlqlQuery.doQuery(xlqlInput)

        expect:
        xlqlEsResponse['totalHits'] > 0
        xlqlEsResponse['totalHits'] == classicEsResponse['totalHits']
        xlqlEsResponse['items'].size() == classicEsResponse['items'].size()
    }
}
