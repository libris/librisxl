package datatool.util

import datatool.util.DocumentUtil.Remove
import datatool.util.DocumentUtil.Replace
import spock.lang.Specification

import static DocumentUtil.NOP

class DocumentUtilSpec extends Specification {

    def "replace"() {
        given:
        def o = [a: [b: [c: [0, 1, [d: 0]]]]]
        DocumentUtil.traverse(o, { value, path ->
            (path && path.last() == 'd') ? new Replace(1) : NOP
        })

        expect:
        o == [a: [b: [c: [0, 1, [d: 1]]]]]
    }

    def "remove"() {
        given:
        def o = [a: [b: [c: 'q', d: 'r']]]
        boolean modified = DocumentUtil.traverse(o, { value, path ->
            value == 'q' ? new Remove() : NOP
        })

        expect:
        modified == true
        o == ['a': ['b': [d: 'r']]]
    }

    def "remove from list"() {
        given:
        def o = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        boolean modified = DocumentUtil.traverse(o, { value, path ->
            if (path) {
                value % 2 == 0 ? new Remove() : new Replace(value * 3)
            }
        })

        expect:
        modified == true
        o == [3, 9, 15, 21, 27]
    }

    def "removing last element removes parent"() {
        given:
        def o = [b: [a: 2, c: 2, b: [[x: [2, 2]], 2]]]
        boolean modified = DocumentUtil.traverse(o, { value, path ->
            if (value == 2 || (path && path.last() == 'x')) {
                new Remove()
            }
        })

        expect:
        modified == true
        o == [:]
    }

    def "no op is nop"() {
        given:
        def o = [a: [b: [c: 'q']]]
        boolean modified = DocumentUtil.traverse(o, { value, path -> })

        expect:
        modified == false
        o == [a: [b: [c: 'q']]]
    }

    def findKey() {
        given:
        def data = [
                a: [b: [c: 'q']],
                r: [s: [t: [a: [q: 2]]]],
                l: [[], [a: 2]]
        ]

        def visited = []
        def values = []
        DocumentUtil.findKey(data, 'a', { value, path ->
            values << value
            visited << path
            return NOP
        })

        expect:
        values == [
                [b: [c: 'q']],
                [q: 2],
                2
        ]
        visited == [
                ['a'],
                ['r', 's', 't', 'a'],
                ['l', 1, 'a']
        ]
    }

    def "link"() {
        given:
        def data = [
                [key: [
                        [x: 3],
                        [x: 1],
                        [x: 2],
                        [x: 3],
                        [x: 4],
                ]],
                [key: [x: 1]],
                [key: [x: 2]]
        ]

        DocumentUtil.findKey(data, 'key', DocumentUtil.link({ blankNode, existingLinks ->
            switch (blankNode['x']) {
                case 1:
                    return [['@id': 7]]
                case 2:
                    return [['@id': 8], ['@id': 9]]
                case 3:
                    return []
                default:
                    return null
            }
        }))

        expect:
        data == [
                [key: [
                        [x: 3],
                        ['@id': 7],
                        ['@id': 8],
                        ['@id': 9],
                        [x: 3],
                        [x: 4]]
                ],
                [key: ['@id': 7]],
                [key: [['@id': 8], ['@id': 9]]]
        ]
    }
}
