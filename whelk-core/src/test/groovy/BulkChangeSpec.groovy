import spock.lang.Specification
import whelk.BulkChange
import whelk.Document

import static whelk.util.Jackson.mapper

class BulkChangeSpec extends Specification {
    static List<Map> specs = BulkChangeSpec.class.getClassLoader()
            .getResourceAsStream('bulk-change-specs.json')
            .with { mapper.readValue(it, Map)['specs'] }
    static BulkChange bulkChange2 = new BulkChange("", [:], ['contribution'] as Set)

    def "get changed paths"() {
        given:
        Map matchForm = spec['matchForm']
        Map targetForm = spec['targetForm']
        List expectedAdded = spec['addedPaths']
        List expectedRemoved = spec['removedPaths']

        expect:
        BulkChange.collectRemovedPaths(matchForm, targetForm) == expectedRemoved
        BulkChange.collectAddedPaths(matchForm, targetForm) == expectedAdded

        where:
        spec << specs
    }

    def "modify (from matchForm to targetForm)"() {
        given:
        Map before = (Map) Document.deepCopy(spec['matchForm'])
        Map after = (Map) Document.deepCopy(spec['targetForm'])
        BulkChange.clearImplicitIds(before)
        BulkChange.clearImplicitIds(after)
        Map formSpec = (Map) Document.deepCopy(spec.subMap(['matchForm', 'targetForm']))

        expect:
        bulkChange2.modify(before, formSpec) == after

        where:
        spec << specs
    }
}
