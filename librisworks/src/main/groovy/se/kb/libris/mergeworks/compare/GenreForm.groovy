package se.kb.libris.mergeworks.compare

import static whelk.JsonLd.ID_KEY

class GenreForm extends StuffSet {
    // When merging, the values in this map are preferred over the keys.
    // E.g. 'https://id.kb.se/marc/Novel' overwrites 'https://id.kb.se/marc/FictionNotFurtherSpecified'
    private static def precedenceRules = [
            ([(ID_KEY): 'https://id.kb.se/marc/NotFictionNotFurtherSpecified']): [
                    [(ID_KEY): 'https://id.kb.se/marc/FictionNotFurtherSpecified'],
                    [(ID_KEY): 'https://id.kb.se/marc/Autobiography'],
                    [(ID_KEY): 'https://id.kb.se/marc/Biography']
            ],
            ([(ID_KEY): 'https://id.kb.se/marc/FictionNotFurtherSpecified'])   : [
                    [(ID_KEY): 'https://id.kb.se/marc/Poetry'],
                    [(ID_KEY): 'https://id.kb.se/marc/Novel']
            ],
    ]

    @Override
    boolean isCompatible(Object a, Object b) {
        def lattLast = {
            it[ID_KEY] == 'https://id.kb.se/term/saogf/L%C3%A4ttl%C3%A4st'
                    || it[ID_KEY] == 'https://id.kb.se/term/barngf/L%C3%A4ttl%C3%A4sta%20b%C3%B6cker'
                    || it['prefLabel'] == 'Lättläst'
        }
        a.any(lattLast) == b.any(lattLast)
    }

    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b)) { gf1, gf2 ->
            if (precedes(gf1, gf2)) {
                gf1
            } else if (precedes(gf2, gf1)) {
                gf2
            }
        }
    }

    boolean precedes(a, b) {
        precedenceRules[b]?.any { it == a || precedes(a, it) }
    }
}