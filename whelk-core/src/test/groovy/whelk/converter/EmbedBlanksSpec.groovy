package whelk.converter

import spock.lang.Specification

class EmbedBlanksSpec extends Specification {

    def "embed blanks"() {
        given:
        def embedded = ["@graph": [
            ["@id": "x", "item": ["@id": "_:b1"]],
            ["@id": "y", "item": ["@id": "_:b2"]],
            ["@id": "_:b1", "name": "1"],
            ["@id": "_:b2", "name": "2"],
        ]]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == ["@graph": [
            ["@id": "x", "item": ["name": "1"]],
            ["@id": "y", "item": ["name": "2"]]
        ]]
    }

    def "embed nested blanks"() {
        given:
        def embedded = ["@graph": [
            ["@id": "x", "item": [["@id": "_:b1"]]],
            ["@id": "_:b1", "item": [["@id": "_:b2"]]],
            ["@id": "_:b2", "name": "2"],
        ]]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == ["@graph": [
            ["@id": "x", "item": [["item": [["name": "2"]]]]],
        ]]
    }

    def "don't embed multiply referenced blanks"() {
        given:
        def embedded = ["@graph": [
            ["@id": "x", "item": ["@id": "_:z"]],
            ["@id": "y", "item": ["@id": "_:z"]],
            ["@id": "_:z", "name": "z"]
        ]]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == ["@graph": [
            ["@id": "x", "item": ["@id": "_:z"]],
            ["@id": "y", "item": ["@id": "_:z"]],
            ["@id": "_:z", "name": "z"]
        ]]
    }

    def "no references"() {
        given:
        def embedded = ["@graph": [
            ["@id": "_:x", "name": "x"],
            ["@id": "_:y", "name": "y"]
        ]]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == ["@graph": [
            ["@id": "_:x", "name": "x"],
            ["@id": "_:y", "name": "y"]
        ]]
    }

    def "just references"() {
        given:
        def embedded = ["@graph": [
            ["@id": "_:x"],
            ["@id": "_:y"]
        ]]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == ["@graph": [
            ["@id": "_:x"],
            ["@id": "_:y"]
        ]]
    }

    def "cyclic references"() {
        given:
        def embedded = ["@graph": [
            ["@id": "_:x", "item": ["@id": "_:y"]],
            ["@id": "_:y", "item": ["@id": "_:x"]]
        ]]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == ["@graph": [
            ["@id": "_:x", "item": ["@id": "_:y"]],
            ["@id": "_:y", "item": ["@id": "_:x"]]
        ]]
    }

    def "nested cyclic references"() {
        given:
        def embedded = ["@graph": [
            ["@id": "_:x", "item": [["@id": "_:b0"], ["@id": "_:y"]]],
            ["@id": "_:y", "item": [["@id": "_:z"]]],
            ["@id": "_:z", "item": [["@id": "_:x"]]]
        ]]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == ["@graph": [
            ["@id": "_:x", "item": [["@id": "_:b0"], ["@id": "_:y"]]],
            ["@id": "_:y", "item": [["@id": "_:z"]]],
            ["@id": "_:z", "item": [["@id": "_:x"]]]
        ]]
    }

    def "indirect cyclic references"() {
        given:
        def embedded = ["@graph": [
            ["@id": "x", "item": ["@id": "_:b0"]],
            ["@id": "_:b0", "item": ["@id": "_:b1"]],
            ["@id": "_:b1", "item": ["@id": "_:b2"]],
            ["@id": "_:b2", "item": ["@id": "_:b1"]]
        ]]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == ["@graph": [
            ["@id": "x", "item": ["item": ["@id": "_:b1"]]],
            ["@id": "_:b1", "item": ["@id": "_:b2"]],
            ["@id": "_:b2", "item": ["@id": "_:b1"]]
        ]]
    }

    def "empty data"() {
        given:
        def embedded = [:]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == [:]
    }

    def "empty graph"() {
        given:
        def embedded = ["@graph": []]
        EmbedBlanks.embedBlanks(embedded)
        expect:
        embedded == ["@graph": []]
    }

}
