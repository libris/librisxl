package se.kb.libris.mergeworks.compare


import static se.kb.libris.mergeworks.Util.asList

class IntendedAudience extends StuffSet {
    static def GENERAL = ['@id': 'https://id.kb.se/marc/General']
    static def ADULT = ['@id': 'https://id.kb.se/marc/Adult']

    @Override
    boolean isCompatible(Object a, Object b) {
        !a || !b || asList(a) == [GENERAL] || asList(b) == [GENERAL]
                || !(asList(a) + asList(b)).findResults { it == ADULT }.containsAll([true, false])
    }
}
