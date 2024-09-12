package datatool.bulkchange

import spock.lang.Specification
import whelk.Document

import static whelk.util.Jackson.mapper

class FormBulkChangeSpec extends Specification {
    static List<Map> specs = FormBulkChangeSpec.class.getClassLoader()
            .getResourceAsStream('datatool/bulkchange/form-bulk-change-specs.json')
            .with { mapper.readValue((InputStream) it, Map)['specs'] }
    static FormBulkChange bulkChange = new FormBulkChange("", [:], [] as Set)

    def "get changed paths"() {
        given:
        Map matchForm = spec['matchForm']
        Map targetForm = spec['targetForm']
        List expectedAdded = spec['addedPaths']
        List expectedRemoved = spec['removedPaths']

        expect:
        FormBulkChange.collectRemovedPaths(matchForm, targetForm) == expectedRemoved
        FormBulkChange.collectAddedPaths(matchForm, targetForm) == expectedAdded

        where:
        spec << specs
    }

    def "modify (from matchForm to targetForm)"() {
        given:
        Map before = (Map) Document.deepCopy(spec['matchForm'])
        Map after = (Map) Document.deepCopy(spec['targetForm'])
        FormBulkChange.clearImplicitIds(before)
        FormBulkChange.clearImplicitIds(after)
        Map formSpec = (Map) Document.deepCopy(spec.subMap(['matchForm', 'targetForm']))

        expect:
        bulkChange.modify(before, formSpec) == after

        where:
        spec << specs
    }
}
