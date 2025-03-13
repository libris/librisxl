package whelk.diff

import spock.lang.Specification

class PatchSpec extends Specification {

    def "simple test"() {
        given:
        def patch = [
                [
                "op":"test",
                "path": "/a",
                "value": "c"
                ]
        ]
        def before = ["a": "c"]
        def after = ["a": "c"]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "negative test"() {
        given:
        def patch = [
                [
                        "op":"test",
                        "path": "/a",
                        "value": "c"
                ]
        ]
        def before = ["a": "d"]

        def result = Patch.patch(before, patch)

        expect:
        result == null
    }

    def "simple add"() {
        given:
        def patch = [
                [
                        "op":"add",
                        "path": "/a",
                        "value": "c"
                ]
        ]
        def before = [:]
        def after = ["a": "c"]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "deep add"() {
        given:
        def patch = [
                [
                        "op":"add",
                        "path": "/a/1/b",
                        "value": "c"
                ]
        ]
        def before = ["a":["e1", [:]]]
        def after = ["a":["e1", ["b":"c"]]]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "root add"() {
        given:
        def patch = [
                [
                        "op":"add",
                        "path": "",
                        "value": ["b":"something else"]
                ]
        ]
        def before = ["a":"something"]
        def after = ["b":"something else"]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "simple remove"() {
        given:
        def patch = [
                [
                        "op":"remove",
                        "path": "/a"
                ]
        ]
        def before = ["a": "c"]
        def after = [:]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "list remove"() {
        given:
        def patch = [
                [
                        "op":"remove",
                        "path": "/a/1"
                ]
        ]
        def before = ["a": ["b", "c"]]
        def after = ["a": ["b"]]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "deep remove"() {
        given:
        def patch = [
                [
                        "op":"remove",
                        "path": "/a/2/c"
                ]
        ]
        def before = ["a": ["a", "b", ["c":"d", "e":"f"]]]
        def after = ["a": ["a", "b", ["e":"f"]]]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "root remove"() {
        given:
        def patch = [
                [
                        "op":"remove",
                        "path": ""
                ]
        ]
        def before = ["a": ["a", "b", ["c":"d", "e":"f"]]]
        def after = [:]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "simple replace"() {
        given:
        def patch = [
                [
                        "op":"replace",
                        "path": "/a",
                        "value": "c"
                ]
        ]
        def before = ["a": "b"]
        def after = ["a": "c"]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "deep replace"() {
        given:
        def patch = [
                [
                        "op":"replace",
                        "path": "/a/0/b",
                        "value": "f"
                ]
        ]
        def before = ["a": [["b":"c"], ["d":"e"]]]
        def after = ["a": [["b":"f"], ["d":"e"]]]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "root replace"() {
        given:
        def patch = [
                [
                        "op":"replace",
                        "path": "",
                        "value": ["g":"h"]
                ]
        ]
        def before = ["a": [["b":"c"], ["d":"e"]]]
        def after = ["g":"h"]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "replace with empty-string address"() {
        given:
        def patch = [
                [
                        "op":"replace",
                        "path": "/",
                        "value": "b"
                ]
        ]
        def before = ["": "a"]
        def after = ["": "b"]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "simple copy"() {
        given:
        def patch = [
                [
                        "op":"copy",
                        "from": "/a",
                        "to": "/c"
                ]
        ]
        def before = ["a": "b", "c": "d"]
        def after = ["a": "b", "c": "b"]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "copy to root"() {
        given:
        def patch = [
                [
                        "op":"copy",
                        "from": "/a",
                        "to": ""
                ]
        ]
        def before = ["a": ["b":"c"]]
        def after = ["b":"c"]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

    def "simple move"() {
        given:
        def patch = [
                [
                        "op":"move",
                        "from": "/a",
                        "to": "/c"
                ]
        ]
        def before = ["a": "b", "c": "d"]
        def after = ["c": "b"]

        def result = Patch.patch(before, patch)

        expect:
        after.equals(result)
    }

}
