package whelk.history


import spock.lang.Specification
import whelk.Document
import whelk.JsonLd
import whelk.util.Jackson
import whelk.util.JsonLdSpec

class HistorySpec extends Specification {
    def "array(set) order does not matter"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': [
                                ['@id': 'id1'],
                                ['b': 'x']
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': [
                                ['b': 'x'],
                                ['@id': 'id1'],
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        !changeSets[1].removedPaths
        !changeSets[1].addedPaths
    }

    def "simple value modified"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': 'x']
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': 'y']
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].removedPaths == [["@graph", 1, "a"]] as Set
        changeSets[1].addedPaths == [["@graph", 1, "a"]] as Set
    }

    def "simple value added"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        [:]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': 'x']
                ]]
        ])

        expect:
        changeSets.size() == 2
        !changeSets[1].removedPaths
        changeSets[1].addedPaths == [["@graph", 1, "a"]] as Set
    }

    def "simple value removed"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': 'x']
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        [:]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].removedPaths == [["@graph", 1, "a"]] as Set
        !changeSets[1].addedPaths
    }

    def "nested value modified"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': [
                                ['b': 'x']
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': [
                                ['b': 'y']
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].removedPaths == [["@graph", 1, "a", 0, "b"]] as Set
        changeSets[1].addedPaths == [["@graph", 1, "a", 0, "b"]] as Set
    }

    def "nested value added"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': 
                                 ['c': [
                                         ['a': 'x']
                                 ]]
                        ]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i':
                                 ['c': [
                                         ['a': 'x', 
                                          'b': 'y']
                                 ]]
                        ]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i", "c", 0, "b"]] as Set
        !changeSets[1].removedPaths
    }

    def "nested value modified 2"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i':
                                 ['c': [
                                         ['a': 'x',
                                          'b': 'y']
                                 ]]
                        ]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i':
                                 ['c': [
                                         ['a': 'x',
                                          'b': 'y2']
                                 ]]
                        ]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i", "c", 0, "b"]] as Set
        changeSets[1].removedPaths == [["@graph", 1, "i", "c", 0, "b"]] as Set
    }

    def "nested value removed"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i':
                                 ['c': [
                                         ['a': 'x',
                                          'b': 'y']
                                 ]]
                        ]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i':
                                 ['c': [
                                         ['a': 'x']
                                 ]]
                        ]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].removedPaths == [["@graph", 1, "i", "c", 0, "b"]] as Set
        !changeSets[1].addedPaths
    }

    def "even more nested value removed"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': [
                                 ['b': [
                                         ['c': [
                                                 ['d': 'x',
                                                  'e': 'y']
                                         ]]
                                 ]]
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': [
                                 ['b': [
                                         ['c': [
                                                 ['d': 'x']
                                         ]]
                                 ]]
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].removedPaths == [["@graph", 1, "a", 0, "b", 0, "c", 0, "e"]] as Set
        !changeSets[1].addedPaths
    }

    def "array middle remove"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['a': 'x', 'b': 'y'],
                                ['@id': 'b'],
                                ['@id': 'c'],
                                ['@id': 'd'],
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'b'],
                                ['@id': 'd'],
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        !changeSets[1].addedPaths
        changeSets[1].removedPaths == [["@graph", 1, "i", 0], ["@graph", 1, "i", 2]] as Set
    }

    def "completely changed"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['p': ['a': 'x', 'b': 'y']]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['p': ['c': 'z', 'd': 'å']]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "p"]] as Set
        changeSets[1].removedPaths == [["@graph", 1, "p"]] as Set
    }

    def "array middle add"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'b'],
                                ['@id': 'd'],
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'a'],
                                ['@id': 'b'],
                                ['@id': 'c'],
                                ['@id': 'd'],
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i", 0], ["@graph", 1, "i", 2]] as Set
        !changeSets[1].removedPaths
    }

    def "array swap and add"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'a'],
                                ['@id': 'b'],
                                ['@id': 'c'],
                                ['@id': 'd'],
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'a'],
                                ['@id': 'd'],
                                ['@id': 'c'],
                                ['@id': 'E'],
                                ['@id': 'b'],
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i", 3]] as Set
        !changeSets[1].removedPaths
    }

    def "array swap"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'a'],
                                ['@id': 'b'],
                                ['@id': 'c'],
                                ['@id': 'd'],
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'a'],
                                ['@id': 'd'],
                                ['@id': 'c'],
                                ['@id': 'b'],
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        !changeSets[1].addedPaths
        !changeSets[1].removedPaths
    }

    def "array middle add local"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'b'],
                                ['@id': 'd'],
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'b'],
                                ['a': 'x', 'b': 'y'],
                                ['@id': 'd'],
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i", 1]] as Set
        !changeSets[1].removedPaths
    }
    
    def "array swap and edit"() {
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['a': 'x', 'b': 'y'],
                                ['c': 'z', 'd': 'å'],
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['c': 'z', 'd': 'å2'],
                                ['a': 'x', 'b': 'y2'],
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i", 0], ["@graph", 1, "i", 1]] as Set
        changeSets[1].removedPaths == [["@graph", 1, "i", 0], ["@graph", 1, "i", 1]] as Set
    }
    
    def "array edit and remove"() {
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['a': 'x', 'b': 'y'],
                                ['c': 'z', 'd': 'å'],
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['c': 'z', 'd': 'å2'],
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i", 0]] as Set
        changeSets[1].removedPaths == [["@graph", 1, "i", 0], ["@graph", 1, "i", 1]] as Set
    }

    
    def "link in array"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['a': 'x'],
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'b'],
                        ]]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i", 0]] as Set
        changeSets[1].removedPaths == [["@graph", 1, "i", 0]] as Set
    }
    
    def "link"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': ['a': 'x']]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': ['@id': 'b']]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i"]] as Set
        changeSets[1].removedPaths == [["@graph", 1, "i"]] as Set
    }
    
    def "unlink"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': ['@id': 'b']]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': ['a': 'x']]
                ]]
        ])
        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i"]] as Set
        changeSets[1].removedPaths == [["@graph", 1, "i"]] as Set
    }

    def "unlink in array"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['@id': 'b'],
                        ]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['i': [
                                ['a': 'x'],
                        ]]
                ]]
        ])
        
        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 1, "i", 0]] as Set
        changeSets[1].removedPaths == [["@graph", 1, "i", 0]] as Set
    }

    def "modified only"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        [
                                '@id': 'ID',
                                'modified': '2022-02-02T12:00:00Z',
                        ],
                        [:]
                ]],
                ['@graph': [
                        [
                                '@id': 'ID',
                                'modified': '2022-02-03T12:00:00Z',
                        ],
                        [:]
                ]]
        ])
        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [["@graph", 0, "modified"]] as Set
        changeSets[1].removedPaths == [["@graph", 0, "modified"]] as Set
    }

    def "string in array"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-03T12:00:00Z'],
                        ['i': ['c': ['l': ["a", "b", "c"]]]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-03T12:00:00Z'],
                        ['i': ['c': ['l': ["a", "B", "c"]]]]
                ]]
        ])
        
        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [['@graph', 1, 'i', 'c', 'l', 1]] as Set
        changeSets[1].removedPaths == [['@graph', 1, 'i', 'c', 'l', 1]] as Set
    }

    def "single string in array"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-03T12:00:00Z'],
                        ['i': ['c': ['l': ["a"]]]]
                ]],
                ['@graph': [
                        ['modified': '2022-02-03T12:00:00Z'],
                        ['i': ['c': ['l': ["A"]]]]
                ]]
        ])
        
        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [['@graph', 1, 'i', 'c', 'l', 0]] as Set
        changeSets[1].removedPaths == [['@graph', 1, 'i', 'c', 'l', 0]] as Set
    }

    def "language container"() {
        given:
        def changeSets = diff([
                ['@graph': [
                        ['modified': '2022-02-03T12:00:00Z'],
                        ['fooByLang': ['a': 'A']]
                ]],
                ['@graph': [
                        ['modified': '2022-02-03T12:00:00Z'],
                        ['fooByLang': ['b': 'B']]
                ]]
        ])

        expect:
        changeSets.size() == 2
        changeSets[1].addedPaths == [['@graph', 1, 'fooByLang', 'b']] as Set
        changeSets[1].removedPaths == [['@graph', 1, 'fooByLang', 'a']] as Set
    }

    def "Owner changes deep in structure"() {
        given:
        def ld = new JsonLd(JsonLdSpec.CONTEXT_DATA, [:], JsonLdSpec.VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'xl',
                 'data':
                        ['@graph': [
                                ['modified': '2022-02-01T12:00:00Z'],
                                ['a': [
                                        ['b': 'x']
                                ]]
                        ]]
                ],
                ['changedBy': 'sigel2',
                 'changedIn': 'xl',
                 'data':
                        ['@graph': [
                                ['modified': '2022-02-02T12:00:00Z'],
                                ['a': [
                                        ['b': 'y']
                                ]]
                        ]]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }

        def history = new History(versions, ld)
        expect:
        history.getOwnership(["@graph", 1, "a"]).m_manualEditor == "sigel1"
        history.getOwnership(["@graph", 1, "a", 0, "b"]).m_manualEditor == "sigel2"
    }

    def "Owner changes deep in structure2"() {
        given:
        def ld = new JsonLd(JsonLdSpec.CONTEXT_DATA, [:], JsonLdSpec.VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'xl',
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-01T12:00:00Z'],
                                 ['a': [
                                         ['b': 'x']
                                 ]]
                         ]]
                ],
                ['changedBy': 'sigel2',
                 'changedIn': 'xl',
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-02T12:00:00Z'],
                                 ['a': [
                                         ['b': 'x'],
                                         ['b': 'y']
                                 ]]
                         ]]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }

        def history = new History(versions, ld)
        expect:
        history.getOwnership(["@graph", 1, "a", 0, "b"]).m_manualEditor == "sigel1"
        history.getOwnership(["@graph", 1, "a", 1, "b"]).m_manualEditor == "sigel2"
    }

    def "Owner changes deep in structure3"() {
        given:
        def ld = new JsonLd(JsonLdSpec.CONTEXT_DATA, [:], JsonLdSpec.VOCAB_DATA)
        def versions = [
                ['changedBy': 'sigel1',
                 'changedIn': 'xl',
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-01T12:00:00Z'],
                                 ['a': [
                                         ['b': 'x'],
                                         ['b': 'y']
                                 ]]
                         ]]
                ],
                ['changedBy': 'sigel2',
                 'changedIn': 'xl',
                 'data':
                         ['@graph': [
                                 ['modified': '2022-02-02T12:00:00Z'],
                                 ['a': [
                                         ['b': 'x']
                                 ]]
                         ]]
                ]
        ].collect { change ->
            new DocumentVersion(new Document(change.data), change.changedBy, change.changedIn)
        }

        def history = new History(versions, ld)
        expect:
        history.getOwnership(["@graph", 1, "a", 0, "b"]).m_manualEditor == "sigel2"
        history.getOwnership(["@graph", 1, "a", 1, "b"]).m_manualEditor == "sigel2" // Doesn't exist anymore, (should revert to list owner).
    }

    def diff(List<Map> versions) {
        def display = [
                "@context": [
                        "fooByLang": ["@id": "foo", "@container": "@language"]
                ],
        ]
        
        def ld = new JsonLd(JsonLdSpec.CONTEXT_DATA, display, JsonLdSpec.VOCAB_DATA)
        def v = versions.collect { data ->
            new DocumentVersion(new Document(data), '', '')
        }

        def history = new History(v, ld)
        List<Map> changeSets = history.m_changeSetsMap['changeSets']
        return changeSets
    }
    
    def printJson(def c) {
        println(groovy.json.JsonOutput.prettyPrint(Jackson.mapper().writeValueAsString(c)))
    }

}
