package whelk.util

import spock.lang.Specification
import whelk.util.DocumentUtil.Remove
import whelk.util.DocumentUtil.Replace

import static whelk.util.DocumentUtil.NOP
import static whelk.util.DocumentUtil.getAtPath

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

    def "removing null values"() {
        given:
        def o = [b: [a: 2, c: null, b: [[x: [2, null, 3]], 2]]]
        boolean modified = DocumentUtil.traverse(o, { value, path ->
            if (value == null) {
                new Remove()
            }
        })

        expect:
        modified == true
        o == [b: [a: 2, b: [[x: [2, 3]], 2]]]
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
            visited << path.collect()
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

    def findKeys() {
        given:
        def data = [
                a: [b: [c: 'q']],
                r: [s: [t: [a: [q: 2]]]],
                l: [[], [a: 2]]
        ]

        def visited = []
        def values = []
        DocumentUtil.findKey(data, ['a', 's', 'q'], { value, path ->
            values << value
            visited << path.collect()
            return NOP
        })

        expect:
        values == [
                [b: [c: 'q']],
                [t: [a: [q: 2]]],
                [q: 2],
                2,
                2
        ]
        visited == [
                ['a'],
                ['r', 's'],
                ['r', 's', 't', 'a'],
                ['r', 's', 't', 'a', 'q'],
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
                [key: 'str'],
                [key: [x: 2]],
        ]

        DocumentUtil.findKey(data, 'key', DocumentUtil.link(
                new DocumentUtil.Linker() {
                    @Override
                    List<Map> link(Map blankNode, List existingLinks) {
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
                    }

                    @Override
                    List<Map> link(String blank, List existingLinks) {
                        return [['@id': 's']]
                    }
                }
        ))

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
                [key: ['@id': 's']],
                [key: [['@id': 8], ['@id': 9]]]
        ]
    }

    // Example enum
    enum E {
        a,
        b,
        x
    }

    def "get at path"() {
        given:


        def data = [
                a:  [ 
                        [b: [
                                [x: 1],
                                [x: 2],
                                [x: 3],
                                [x: 4],
                                [x: 5],
                        ]],
                        [b: [x: 88]],
                        [b: 'str'],
                        [b: [x: 99]],
                        [b: [
                                [x: 6],
                                [x: 7],
                                [x: 8],
                        ]],
                        999
                    ]
                ]

        expect:
        getAtPath(data, path, defaultTo) == expected

        where:
        path                      | defaultTo || expected
        ['b']                     | 'default' || 'default'
        [1]                       | 'default' || 'default'
        ['*']                     | 'default' || []
        ['a', 4, 'b', 1, 'x']     | 'default' || 7
        [E.a, 4, E.b, 1, E.x]     | 'default' || 7
        ['a', '*', 'b', 1, 'x']   | 'default' || [2, 7]
        ['a', '*', 'b', 3, 'x']   | 'default' || [4]
        ['a', '*', 'b', '*', 'x'] | 'default' || [1, 2, 3, 4, 5, 6, 7, 8]
        ['a', '*', 'b', 'x']      | 'default' || [88, 99]
        // maybe a bit counter-intuitive but using '*' flattens all lists
        ['a', '*', 'b']           | 'default' || [[x: 1], [x: 2], [x: 3], [x: 4], [x: 5], [x: 88], 'str', [x: 99], [x: 6], [x: 7], [x: 8]]
    }

    def "get at empty path"() {
        given:
        def data = [a: [b: 1]]

        expect:
        getAtPath(data, []) == data
    }

    def "get at path default defaultTo"() {
        given:
        def data = [a: [b: 1]]

        expect:
        getAtPath(data, ['c']) == null
    }
}
