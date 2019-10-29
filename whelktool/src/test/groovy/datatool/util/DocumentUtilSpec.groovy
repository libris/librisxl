package datatool.util

import datatool.util.DocumentUtil.Remove
import datatool.util.DocumentUtil.Replace
import spock.lang.Specification

import static DocumentUtil.NOP

class DocumentUtilSpec extends Specification {

    def "replace"() {
        given:
        def o = [a: [b: [c: [0, 1, [d: 0]]]]]
        DocumentUtil.traverse(o, { path, value ->
            (path && path.last() == 'd') ? new Replace(1) : NOP
        })

        expect:
        o == [a: [b: [c: [0, 1, [d: 1]]]]]
    }

    def "remove"() {
        given:
        def o = [a: [b: [c: 'q']]]
        boolean modified = DocumentUtil.traverse(o, { path, value ->
            value == 'q' ? new Remove() : NOP
        })

        expect:
        modified == true
        o == ['a': ['b': [:]]]
    }

    def "remove from list"() {
        given:
        def o = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        boolean modified = DocumentUtil.traverse(o, { path, value ->
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
        boolean modified = DocumentUtil.traverse(o, { path, value ->  })

        expect:
        modified == false
        o == [a: [b: [c: 'q']]]
    }

}
