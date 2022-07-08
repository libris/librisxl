package datatool.scripts.mergeworks.compare

import datatool.util.DocumentComparator

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
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b).findAll { it.'@id' }) { gf1, gf2 ->
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