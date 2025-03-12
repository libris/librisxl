package whelk.diff

import spock.lang.Specification

class DiffSpec extends Specification {

    def "simple value replace"() {
        given:
        def before = "{\"a\":\"b\"}"
        def after = "{\"a\":\"c\"}"

        def result = Diff.diff(before, after);

        expect:
        result == [
                [
                        "op":"replace",
                        "path": "/a",
                        "value": "c"
                ]
        ]
    }

    def "simple property add"() {
        given:
        def before = "{\"a\":\"b\"}"
        def after = "{\"a\":\"b\", \"c\":\"d\"}"

        def result = Diff.diff(before, after);

        expect:
        result == [
                [
                        "op":"add",
                        "path": "/c",
                        "value": "d"
                ]
        ]
    }

    def "simple property remove"() {
        given:
        def before = "{\"a\":\"b\", \"c\":\"d\"}"
        def after = "{\"a\":\"b\"}"


        def result = Diff.diff(before, after);

        expect:
        result == [
                [
                        "op":"remove",
                        "path": "/c"
                ]
        ]
    }

    def "replace type"() {
        given:
        def before = "{\"a\":\"b\"}"
        def after = "{\"a\":[\"b\"]}"

        def result = Diff.diff(before, after);

        expect:
        result == [
                [
                        "op":"replace",
                        "path": "/a",
                        "value": ["b"]
                ]
        ]
    }

    def "add to list"() {
        given:
        def before = "{\"a\":[\"b\"]}"
        def after = "{\"a\":[\"b\", \"c\"]}"

        def result = Diff.diff(before, after);

        expect:
        result == [
                [
                        "op":"add",
                        "path": "/a/1",
                        "value": "c"
                ]
        ]
    }

    def "remove from list"() {
        given:
        def before = "{\"a\":[\"b\", \"c\"]}"
        def after = "{\"a\":[\"b\"]}"

        def result = Diff.diff(before, after);

        expect:
        result == [
                [
                        "op":"remove",
                        "path": "/a/1"
                ]
        ]
    }

    def "add to list at escaped path"() {
        given:
        def before = "{\"/something~\":[\"b\"]}"
        def after = "{\"/something~\":[\"b\", \"c\"]}"

        def result = Diff.diff(before, after);

        expect:
        result == [
                [
                        "op":"add",
                        "path": "/~1something~0/1",
                        "value": "c"
                ]
        ]
    }
}
