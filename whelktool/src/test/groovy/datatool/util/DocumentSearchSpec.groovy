package datatool.util

import datatool.util.DocumentSearch.Remove
import datatool.util.DocumentSearch.Replace
import spock.lang.Specification

import static DocumentSearch.NOP

class DocumentSearchSpec extends Specification {

    def "replace"() {
        given:
        def o = [a: [b: [c: [0, 1, [d: 0]]]]]
        new DocumentSearch().traverse(o, { path, value ->
            (path && path.last() == 'd') ? new Replace(1) : NOP
        })

        expect:
        o == [a: [b: [c: [0, 1, [d: 1]]]]]
    }

    def "remove"() {
        given:
        def o = [a: [b: [c: 'q']]]
        boolean modified = new DocumentSearch().traverse(o, { path, value ->
            value == 'q' ? new Remove() : NOP
        })

        expect:
        modified == true
        o == ['a': ['b': [:]]]
    }

    def "remove from list"() {
        given:
        def o = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        boolean modified = new DocumentSearch().traverse(o, { path, value ->
            if (path) {
                value % 2 == 0 ? new Remove() : new Replace(value * 3)
            }
        })

        expect:
        modified == true
        o == [3, 9, 15, 21, 27]
    }

    def "no op is nop"() {
        given:
        def o = [a: [b: [c: 'q']]]
        boolean modified = new DocumentSearch().traverse(o, { path, value ->  })

        expect:
        modified == false
        o == [a: [b: [c: 'q']]]
    }

}
