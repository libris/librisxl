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
        def versions =[
                ['@graph': [
                        ['modified':'2022-02-02T12:00:00Z'],
                        ['a':[
                                ['@id': 'id1'],
                                ['b': 'x']
                        ]]
                ]],
                ['@graph': [
                        ['modified':'2022-02-02T12:00:00Z'],
                        ['a':[
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
        def versions =[
                ['@graph': [
                        ['modified':'2022-02-02T12:00:00Z'],
                        ['a': 'x']
                ]],
                ['@graph': [
                        ['modified':'2022-02-02T12:00:00Z'],
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
        def versions =[
                ['@graph': [
                        ['modified':'2022-02-02T12:00:00Z'],
                        ['a':[
                                ['b': 'x']
                        ]]
                ]],
                ['@graph': [
                        ['modified':'2022-02-02T12:00:00Z'],
                        ['a':[
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

    def printJson(def c) {
        println(groovy.json.JsonOutput.prettyPrint(Jackson.mapper().writeValueAsString(c)))
    }

}
