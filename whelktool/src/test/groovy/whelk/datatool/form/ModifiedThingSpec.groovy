package whelk.datatool.form

import spock.lang.Specification

import static whelk.util.Jackson.mapper

class ModifiedThingSpec extends Specification {
    static List<Map> specs = FormDiffSpec.class.getClassLoader()
    // TODO: Add lots of test cases to modify-specs.json
            .getResourceAsStream('whelk/datatool/form/modify-specs.json')
//            .getResourceAsStream('whelk/datatool/form/form-bulk-change-specs.json')
            .with { mapper.readValue((InputStream) it, Map)['specs'] }
    static repeatable = ['r1', 'r2'] as Set

    def "pass"() {
        given:
        def formDiff = new FormDiff(spec["matchForm"], spec["targetForm"])
        def before = spec["before"]
        def after = spec["after"]

        expect:
        new ModifiedThing(before, formDiff, repeatable).after == after

        where:
        spec << specs.findAll { !it['shouldFailWithException'] }
    }

    def "fail with exception"() {
        given:
        def formDiff = new FormDiff(spec["matchForm"], spec["targetForm"])
        def before = spec["before"]

        when:
        new ModifiedThing(before, formDiff, repeatable)

        then:
        thrown Exception

        where:
        spec << specs.findAll { it['shouldFailWithException'] }
    }
}
