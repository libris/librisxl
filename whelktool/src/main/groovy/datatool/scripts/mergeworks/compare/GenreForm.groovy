package datatool.scripts.mergeworks.compare

class GenreForm extends StuffSet {
    private static Map norm = [
            ['@id': 'https://id.kb.se/marc/NotFictionNotFurtherSpecified'] : ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'],
            ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'] : ['@id': 'https://id.kb.se/marc/Novel'],
            ['@id': 'https://id.kb.se/marc/FictionNotFurtherSpecified'] : ['@id': 'https://id.kb.se/marc/Novel'],
    ]

    @Override
    Object merge(Object a, Object b) {
        return mergeCompatibleElements(super.merge(a, b)) { gf1, gf2 ->
            if (norm[gf1] == gf2) {
                repl(gf2)
            }
            else if (norm[gf2] == gf1) {
                repl(gf1)
            }
        }
    }

    Object repl(Object o) {
        def r = norm.getOrDefault(o, o)
        o != r ? repl(r) : o
    }
}