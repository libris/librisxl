package whelk.datatool.form

import spock.lang.Specification

import static whelk.util.Jackson.mapper

class ModifiedThingSpec extends Specification {
    static List<Map> specs = ModifiedThingSpec.class.getClassLoader()
            .getResourceAsStream('whelk/datatool/form/specs.json')
            .with { mapper.readValue((InputStream) it, Map)['specs'] }
    static repeatable = ['r1', 'r2'] as Set

    def "pass"() {
        given:
        def transform = new Transform(spec["bulk:matchForm"], spec["bulk:targetForm"])
        def before = spec["before"]
        def after = spec["after"]

        expect:
        new ModifiedThing(before, transform, repeatable).after == after

        where:
        spec << specs.findAll { it["before"] && !it['shouldFailWithException'] }
    }

    def "fail with exception"() {
        given:
        def transform = new Transform(spec["bulk:matchForm"], spec["bulk:targetForm"])
        def before = spec["before"]

        when:
        new ModifiedThing(before, transform, repeatable)

        then:
        thrown Exception

        where:
        spec << specs.findAll { it["before"] && it['shouldFailWithException'] }
    }
}
