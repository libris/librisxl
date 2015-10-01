package whelk.plugin

import whelk.Whelk
import whelk.importer.ImportResult

import spock.lang.Specification
import whelk.importer.OaiPmhImporter
import whelk.servlet.ScheduledJob


class ScheduledOperatorSpec extends Specification {

    def "should update state on successful import"() {
        setup:
        def ds = "test"
        def is = [:]

        and:
        def imports = ["2001-01-01T00:00:00+00", null, "2002-02-02T00:00:00+00"]
        def importer = GroovyMock(OaiPmhImporter)
        importer.serviceUrl >> "http://example.org/service"
        importer.doImport(_, _, _, _, _, _) >>> imports.collect {
            new ImportResult(
                numberOfDocuments: it? 1 : 0,
                numberOfDeleted: 0,
                lastRecordDatestamp:
                    it? Date.parse(ScheduledJob.DATE_FORMAT, it) : null)
        }

        when: "first import"
        def sjob = new ScheduledJob(importer, ds, null, is)
        sjob.run()
        then:
        is[ds].lastImport == imports[0]

        and: "none in this one"
        sjob.run()
        then:
        is[ds].lastImport == imports[0]

        and: "something new"
        sjob.run()
        then:
        is[ds].lastImport == imports[2]

        and: "nothing new"
        sjob.run()
        then:
        is[ds].lastImport == imports[2]
    }

}
