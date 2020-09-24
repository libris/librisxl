package datatool.scripts.mergeworks.compare

import datatool.util.DocumentComparator

//FIXME
class GenreForm extends StuffSet {
    private static final DocumentComparator c = new DocumentComparator()

    // Terms that will be merged, result is the one to right
    private static def norm = [
            [['@id': 'https://id.kb.se/marc/NotFictionNotFurtherSpecified'], ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified']],
            [['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'], ['@id': 'https://id.kb.se/marc/Novel']],
            [['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'], ['@id': 'https://id.kb.se/marc/Poetry']],
    ]

    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b)) { gf1, gf2 ->
            if (nor(gf1, gf2)) {
                gf2
            }
            else if (nor(gf2, gf1)) {
                gf1
            }
        }
    }

    boolean nor(a, b) {
        norm.any {
            it[0] == a && it[1] == b
        }
    }


}