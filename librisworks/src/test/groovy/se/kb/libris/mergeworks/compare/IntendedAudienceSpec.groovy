package se.kb.libris.mergeworks.compare

import se.kb.libris.mergeworks.Doc
import spock.lang.Specification
import whelk.Document
import whelk.Whelk

class IntendedAudienceSpec extends Specification {
    private static def general = ['@id': 'https://id.kb.se/marc/General']
    private static def adult = ['@id': 'https://id.kb.se/marc/Adult']
    private static def juvenile = ['@id': 'https://id.kb.se/marc/Juvenile']
    private static def blank = ['label': 'x']

    def "is compatible"() {
        expect:
        new IntendedAudience().isCompatible(a, b) == result

        where:
        a          || b                          || result
        [juvenile] || [juvenile]                 || true
        [juvenile] || [general]                  || true
        [juvenile] || []                         || true
        [juvenile] || [blank, general, juvenile] || true
        [juvenile] || [adult]                    || false
        [adult]    || [adult]                    || true
        [adult]    || [general]                  || true
        [adult]    || [juvenile, general]        || false
        [adult]    || [blank]                    || false
        [adult]    || []                         || true
    }

    def "preferred comparison order"() {
        given:
        Whelk whelk = Whelk.createLoadedSearchWhelk()
        def intendedAudience = [[juvenile], [adult], [juvenile], [], [adult], [general]]
        List<Doc> docs = intendedAudience.collect {
            def data = ['@graph': [[], ['instanceOf': ['intendedAudience': it]]]]
            return new Doc(whelk, new Document(data))
        }
        IntendedAudience.preferredComparisonOrder(docs)

        expect:
        docs*.intendedAudience() == [[general], [adult], [], [adult], [juvenile], [juvenile]]
    }
}