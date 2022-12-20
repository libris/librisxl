package whelk.history

import spock.lang.Specification
import whelk.Document
import whelk.JsonLd
import whelk.util.Jackson
import whelk.util.JsonLdSpec

class HistorySpec extends Specification {
    def "array(set) order does not matter"() {
        given:
        def ld = new JsonLd(JsonLdSpec.CONTEXT_DATA, [:], JsonLdSpec.VOCAB_DATA)
        def versions = [
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
        ].collect { data ->
            new DocumentVersion(new Document(data), '', '')
        }

        def history = new History(versions, ld)
        List<Map> changeSets = history.m_changeSetsMap['changeSets']
        expect:
        changeSets.size() == 2
        !changeSets[1].removedPaths
        !changeSets[1].addedPaths
    }

    def "simple value modified"() {
        given:
        def ld = new JsonLd(JsonLdSpec.CONTEXT_DATA, [:], JsonLdSpec.VOCAB_DATA)
        def versions = [
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': 'x']
                ]],
                ['@graph': [
                        ['modified': '2022-02-02T12:00:00Z'],
                        ['a': 'y']
                ]]
        ].collect { data ->
            new DocumentVersion(new Document(data), '', '')
        }

        def history = new History(versions, ld)
        List<Map> changeSets = history.m_changeSetsMap['changeSets']
        expect:
        changeSets.size() == 2
        changeSets[1].removedPaths == [["@graph", 1, "a"]] as Set
        changeSets[1].addedPaths == [["@graph", 1, "a"]] as Set
    }

    def "nested value modified"() {
        given:
        def ld = new JsonLd(JsonLdSpec.CONTEXT_DATA, [:], JsonLdSpec.VOCAB_DATA)
        def versions = [
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
        ].collect { data ->
            new DocumentVersion(new Document(data), '', '')
        }

        def history = new History(versions, ld)
        List<Map> changeSets = history.m_changeSetsMap['changeSets']

        //printJson(changeSets)

        expect:
        changeSets.size() == 2
        changeSets[1].removedPaths == [["@graph", 1, "a", 0, "b"]] as Set
        changeSets[1].addedPaths == [["@graph", 1, "a", 0, "b"]] as Set
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

    def printJson(def c) {
        println(groovy.json.JsonOutput.prettyPrint(Jackson.mapper().writeValueAsString(c)))
    }

}
