package se.kb.libris.mergeworks.compare

import spock.lang.Specification

class GenreFormSpec extends Specification {
    private static def x = ['@id': 'https://id.kb.se/x']
    private static def y = ['@id': 'https://id.kb.se/y']
    private static def z = ['prefLabel': 'z']
    private static def saoGfLatt = ['@id': 'https://id.kb.se/term/saogf/L%C3%A4ttl%C3%A4st']
    private static def marcFiction = ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified']
    private static def marcNotFiction = ['@id': 'https://id.kb.se/marc/NotFictionNotFurtherSpecified']
    private static def marcNovel = ['@id': 'https://id.kb.se/marc/Novel']

    def "is compatible"() {
        expect:
        new GenreForm().isCompatible(a, b) == result

        where:
        a              || b              || result
        [x]            || [y]            || true
        [x]            || [x, y, z]      || true
        [x]            || [y, saoGfLatt] || false
        [x, saoGfLatt] || [y, saoGfLatt] || true
    }

    def "merge"() {
        expect:
        new GenreForm().merge(a, b) as Set == result as Set

        where:
        a                || b                   || result
        [x]              || [y]                 || [x, y]
        [x]              || [x, y, z]           || [x, y, z]
        [x, saoGfLatt]   || [y, saoGfLatt]      || [x, y, saoGfLatt]
        [x, marcFiction] || [marcNotFiction]    || [x, marcFiction]
        [x, marcNovel]   || [marcNotFiction, y] || [x, y, marcNovel]
    }
}
