package se.kb.libris.mergeworks.compare

import se.kb.libris.mergeworks.Doc

import static se.kb.libris.mergeworks.Util.asList

class IntendedAudience extends StuffSet {
    private static def GENERAL = ['@id': 'https://id.kb.se/marc/General']
    private static def ADULT = ['@id': 'https://id.kb.se/marc/Adult']

    @Override
    boolean isCompatible(Object a, Object b) {
        !a || !b || asList(a) == [GENERAL] || asList(b) == [GENERAL]
                || !(asList(a) + asList(b)).findResults { it == ADULT }.containsAll([true, false])
    }

    static void preferredComparisonOrder(Collection<Doc> docs) {
        docs.sort { Doc d ->
            d.intendedAudience().with {
                it.isEmpty() || it == [GENERAL] || it == [ADULT]
            }
        }.reverse(true)
    }
}
