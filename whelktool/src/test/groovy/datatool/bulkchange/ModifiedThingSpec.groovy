package datatool.bulkchange

import spock.lang.Specification
import whelk.Document

import static whelk.util.Jackson.mapper

class ModifiedThingSpec extends Specification {
    static List<Map> specs = FormDiffSpec.class.getClassLoader()
            .getResourceAsStream('datatool/bulkchange/form-bulk-change-specs.json')
            .with { mapper.readValue((InputStream) it, Map)['specs'] }

    def "modify (from matchForm to targetForm)"() {
        given:
        Map before = spec['matchForm']
        Map after = spec['targetForm']
        FormDiff formDiff = new FormDiff(spec['matchForm'], spec['targetForm'])
//        Map formSpec = (Map) Document.deepCopy(spec.subMap(['matchForm', 'targetForm']))

        expect:
        new ModifiedThing(before, formDiff, [] as Set).after == after

        where:
        spec << specs
    }
}
