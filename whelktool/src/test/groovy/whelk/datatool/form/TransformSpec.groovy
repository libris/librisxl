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
        spec << specs.findAll { (it["addedPaths"] || it["removedPaths"]) && !it['shouldFailWithException'] }.drop(7)
    }
}
