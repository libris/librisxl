import spock.lang.Specification
import whelk.BulkChange
import whelk.util.DocumentComparator

import static whelk.util.Jackson.mapper

class BulkChangeSpec extends Specification {
    static DocumentComparator comparator = new DocumentComparator()
    static List<Map> specs = BulkChangeSpec.class.getClassLoader()
            .getResourceAsStream('bulk-change-specs.json')
            .with { mapper.readValue(it, Map)['specs'] }

    def "modify"() {
        given:
        def test = spec['test']
        def before = test['before']
        def after = test['after']

        when:
        def modified = BulkChange.modify(before, spec, comparator)

        then:
        [modified, after].transpose().each { result, expected ->
            assert comparator.isEqual(result, expected)
        }

        where:
        spec << specs
    }
}
