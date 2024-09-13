package whelk.datatool.form

import spock.lang.Specification

import static whelk.util.Jackson.mapper

class ModifiedThingSpec extends Specification {
    static List<Map> specs = FormDiffSpec.class.getClassLoader()
            // TODO: Add lots of test cases to modify-specs.json
            .getResourceAsStream('whelk/datatool/form/modify-specs.json')
//            .getResourceAsStream('whelk/datatool/form/form-bulk-change-specs.json')
            .with { mapper.readValue((InputStream) it, Map)['specs'] }

    def "test"() {
        given:
        def formDiff = new FormDiff(spec["matchForm"], spec["targetForm"])
        def before = spec["before"]
        def after = spec["after"]

        expect:
        new ModifiedThing(before, formDiff, [] as Set).after == after

        where:
        spec << specs
    }
}
