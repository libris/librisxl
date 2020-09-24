package datatool.scripts.mergeworks.compare

import datatool.util.DocumentComparator

//FIXME
class GenreForm extends StuffSet {
    private static final DocumentComparator c = new DocumentComparator()

    // Terms that will be merged, result is the one to right
    private static Map norm = [
            ['@id': 'https://id.kb.se/marc/NotFictionNotFurtherSpecified'] : ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'],
            ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'] : ['@id': 'https://id.kb.se/marc/Novel'],
            ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'] : ['@id': 'https://id.kb.se/marc/Poetry'],
    ]

    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b)) { gf1, gf2 ->
            if (cmp(norm[gf1], gf2)) {
                gf2
            }
            else if (cmp(norm[gf2], gf1)) {
                gf1
            }
        }
    }

    boolean cmp(a, b) {
        a && b && c.isEqual(a, b)
    }
}