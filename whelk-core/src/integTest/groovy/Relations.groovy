

import whelk.Whelk
import whelk.util.WhelkFactory

class Relations {
    static final String SKÖN = 'https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur'
    static final String FANTASY = 'https://id.kb.se/term/saogf/Fantasy'
    static final String URBAN_FANTASY = 'https://id.kb.se/term/saogf/Urban%20fantasy'
    static final String ÄVENTYR = 'https://id.kb.se/term/gmgpc%2F%2Fswe/%C3%84ventyrsserier'

    static void main(args) {
        Whelk whelk = WhelkFactory.getSingletonWhelk()

        assert whelk.getRelations().isImpliedBy(SKÖN, URBAN_FANTASY)
        assert whelk.getRelations().isImpliedBy(SKÖN, ÄVENTYR)

        def fiction = whelk.getRelations().followReverseBroader(SKÖN)
        assert fiction.size() == 102
        assert FANTASY in fiction
        assert URBAN_FANTASY in fiction
        assert ÄVENTYR in fiction

        assert whelk.getRelations().getBy(FANTASY, ['broadMatch', 'broader']).asList() == [SKÖN]

        def s =  whelk.getRelations().getByReverse(SKÖN, ['broadMatch', 'broader'])
        assert FANTASY in s
        assert !(URBAN_FANTASY in s)
    }

}
