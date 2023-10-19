import spock.lang.Specification
import whelk.BulkChange
import whelk.Whelk
import whelk.util.DocumentComparator

import static whelk.util.Jackson.mapper

class BulkChangeSpec extends Specification {
    static DocumentComparator comparator = new DocumentComparator()
    static List<Map> specs = BulkChangeSpec.class.getClassLoader()
            .getResourceAsStream('bulk-change-specs.json')
            .with { mapper.readValue(it, Map)['specs'] }
    static Whelk whelk = Whelk.createLoadedSearchWhelk()

    def "modify"() {
        given:
        def test = spec['test']
        def before = test['before']
        def after = test['after']
        def modified = BulkChange.modify(before, spec, comparator, whelk.jsonld.getRepeatableTerms())

        expect:
        [modified, after].transpose().each { result, expected ->
            assert comparator.isEqual(result, expected)
        }

        where:
        spec << specs
    }
}
