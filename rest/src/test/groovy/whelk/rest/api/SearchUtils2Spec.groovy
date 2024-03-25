package whelk.rest.api

import spock.lang.Ignore
import spock.lang.Specification
import whelk.Whelk

@Ignore
class SearchUtils2Spec extends Specification {
    private static Whelk whelk = Whelk.createLoadedSearchWhelk()
    private static SearchUtils2 searchUtils2 = new SearchUtils2(whelk)
    private static SearchUtils searchUtils = new SearchUtils(whelk)

    // The following tests assume a static dataset
    def "Simple free text string"() {
        given:
        def classicInput = ['q': ["Kalle"] as String[]]
        def xlqlInput = ['_q': ["Kalle"] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Simple free text phrase"() {
        given:
        def classicInput = ['q': ["\"Kalle Anka\""] as String[]]
        def xlqlInput = ['_q': ["\"Kalle Anka\""] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Simple free text conjunction"() {
        given:
        def classicInput = ['q': ['Kalle Anka'] as String[]]
        def xlqlInput = ['_q': ['Kalle Anka'] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Simple free text disjunction"() {
        given:
        def classicInput = ['q': ["Kalle | Anka"] as String[]]
        def xlqlInput = ['_q': ["Kalle or Anka"] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Simple free text negation"() {
        given:
        def classicInput = ['q': ["-Sverige"] as String[]]
        def xlqlInput = ['_q': ["not Sverige"] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        /*
         In this case it makes sense that we get less hits from the xlql query,
         since the xlql query translates to
         !(simpleQuery or boostedSoft or boostedExact)
         rather than
         !simpleQuery or !boostedSoft or !boostedExact
         Thus an improvement.
         */
        searchUtils2Result['totalItems'] < searchUtilsClassicResult['totalItems']
    }

    def "Free text negation"() {
        given:
        def classicInput = ['q': ["Anka -Kalle"] as String[]]
        def xlqlInput = ['_q': ["Anka not Kalle"] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Free text grouping 1"() {
        given:
        def classicInput = ['q': ["Bamse | (Kalle Anka)"] as String[]]
        def xlqlInput = ['_q': ["Bamse or (Kalle Anka)"] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Free text grouping 2"() {
        given:
        def classicInput = ['q': ["(Kalle|Musse) (Pigg|Anka)"] as String[]]
        def xlqlInput = ['_q': ["(Musse or Kalle) and (Pigg or Anka)"] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Free text nested"() {
        given:
        def classicInput = ['q': ["(Kalle (Anka|Blomkvist)) (Disney|Astrid) -Sverige"] as String[]]
        def xlqlInput = ['_q': ["(Kalle and (Anka or Blomkvist) not Sverige) and (Disney or Astrid)"] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Simple field"() {
        given:
        def classicInput = ['issuanceType': ['Serial'] as String[]]
        def xlqlInput = ['_q': ['issuanceType: Serial'] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Combined fields + disambiguation"() {
        given:
        def classicInput = ['issuanceType': ['Serial'] as String[], 'meta.encodingLevel': ['marc:FullLevel']]
        def xlqlInput = ['_q': ['utgivningssätt: Serial and beskrivningsnivå="marc:FullLevel"'] as String[]]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }

    def "Free text + fields + grouping"() {
        given:
        def classicInput = [
                'q'                 : ['fåglar'] as String[],
                'issuanceType'      : ['Serial', 'Monograph'] as String[],
                'meta.encodingLevel': ['marc:FullLevel']
        ]
        def xlqlInput = [
                '_q'    : ['fåglar and utgivningssätt: (Serial or Monograph) and beskrivningsnivå="marc:FullLevel"'] as String[]
        ]

        Map searchUtilsClassicResult = searchUtils.doSearch(classicInput)
        Map searchUtils2Result = searchUtils2.doSearch(xlqlInput)

        expect:
        searchUtils2Result['totalItems'] > 0
        searchUtils2Result['totalItems'] == searchUtilsClassicResult['totalItems']
    }
}
