

import whelk.Whelk
import whelk.util.WhelkFactory

class Relations {
    static void main(args) {
        Whelk whelk = WhelkFactory.getSingletonWhelk()

        assert whelk.isImpliedBy('https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur', 'https://id.kb.se/term/saogf/Urban%20fantasy')
        assert whelk.isImpliedBy('https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur', 'https://id.kb.se/term/gmgpc%2F%2Fswe/%C3%84ventyrsserier')

        def fiction = whelk.findInverseBroaderRelations('https://id.kb.se/term/saogf/Sk%C3%B6nlitteratur')
        assert fiction.size() == 102
        assert 'https://id.kb.se/term/saogf/Urban%20fantasy' in fiction
        assert 'https://id.kb.se/term/gmgpc%2F%2Fswe/%C3%84ventyrsserier' in fiction

        println("OK")
    }

}
