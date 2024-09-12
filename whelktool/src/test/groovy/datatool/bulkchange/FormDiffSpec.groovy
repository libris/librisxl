package datatool.bulkchange

import spock.lang.Specification
import whelk.Document

import static whelk.util.Jackson.mapper

class FormDiffSpec extends Specification {
    static List<Map> specs = FormDiffSpec.class.getClassLoader()
            .getResourceAsStream('datatool/bulkchange/form-bulk-change-specs.json')
            .with { mapper.readValue((InputStream) it, Map)['specs'] }

    def "get changed paths"() {
        given:
        FormDiff formDiff = new FormDiff(spec['matchForm'], spec['targetForm'])
        List expectedAdded = spec['addedPaths']
        List expectedRemoved = spec['removedPaths']

        expect:
        formDiff.removedPaths == expectedRemoved
        formDiff.addedPaths == expectedAdded

        where:
        spec << specs
    }
}
