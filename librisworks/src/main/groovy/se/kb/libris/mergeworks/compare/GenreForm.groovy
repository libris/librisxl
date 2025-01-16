package se.kb.libris.mergeworks.compare

import whelk.datatool.util.DocumentComparator

//FIXME
class GenreForm extends StuffSet {
    private static final DocumentComparator c = new DocumentComparator()

    // Terms that will be merged (values precede keys)
    private static def norm = [
            (['@id': 'https://id.kb.se/marc/NotFictionNotFurtherSpecified']): [
                    ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'],
                    ['@id': 'https://id.kb.se/marc/Autobiography'],
                    ['@id': 'https://id.kb.se/marc/Biography']
            ],
            (['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'])   : [
                    ['@id': 'https://id.kb.se/marc/Poetry'],
                    ['@id': 'https://id.kb.se/marc/Novel']
            ],
    ]

    @Override
    boolean isCompatible(Object a, Object b) {
        def lattLast = {
            it['@id'] == 'https://id.kb.se/term/saogf/L%C3%A4ttl%C3%A4st'
                    || it['@id'] == 'https://id.kb.se/term/barngf/L%C3%A4ttl%C3%A4sta%20b%C3%B6cker'
                    || it['prefLabel'] == 'Lättläst'
        }
        a.any(lattLast) == b.any(lattLast)
    }

    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b)) { gf1, gf2 ->
            if (n(gf1, gf2)) {
                gf2
            } else if (n(gf2, gf1)) {
                gf1
            }
        }
    }

    boolean n(a, b) {
        norm[a]?.any { it == b || n(it, b) }
    }
}