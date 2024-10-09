package whelk.datatool.form

import spock.lang.Specification

import static whelk.util.Jackson.mapper

class TransformSpec extends Specification {
    static List<Map> specs = TransformSpec.class.getClassLoader()
            .getResourceAsStream('whelk/datatool/form/specs.json')
            .with { mapper.readValue((InputStream) it, Map)['specs'] }

    def "collect changed paths"() {
        given:
        def transform = new Transform(spec["matchForm"], spec["targetForm"])
        def addedPaths = spec["addedPaths"]
        def removedPaths = spec["removedPaths"]

        expect:
        transform.addedPaths == addedPaths
        transform.removedPaths == removedPaths

        where:
        spec << specs.findAll { (it["addedPaths"] || it["removedPaths"]) && !it['shouldFailWithException'] }
    }

    def "is equal"() {
        given:
        def a = ["p": ["x": "y"]]
        def b = ["p": ["@type": "t1", "x": "y"]]
        def c = ["p": ["@type": "t2", "x": "y"]]

        expect:
        Transform.isEqual(a, b)
        Transform.isEqual(b, a)
        Transform.isEqual(a, c)
        !Transform.isEqual(b, c)
        Transform.isEqual(["p": [["a":"b"], a]], ["p": [a, ["a":"b"]]])
        Transform.isEqual(["p": [["a":"b"], a]], ["p": [b, ["a":"b"]]])
        !Transform.isEqual(["p": [["a":"b"], c]], ["p": [b, ["a":"b"]]])
    }
}
