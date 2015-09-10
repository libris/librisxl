package whelk.plugin

import whelk.StandardWhelk
import whelk.result.ImportResult

import spock.lang.Specification


class ScheduledOperatorSpec extends Specification {

    def "should update state on successful import"() {
        setup:
        def ds = "test"
        def whelk = new StandardWhelk()

        and:
        def imports = ["2001-01-01T00:00:00Z", null, "2002-02-02T00:00:00Z"]
        def importer = GroovyMock(Importer)
        importer.serviceUrl >> "http://example.org/service"
        importer.doImport(_, _, _, _, _, _) >>> imports.collect {
            new ImportResult(
                numberOfDocuments: it? 1 : 0,
                numberOfDeleted: 0,
                lastRecordDatestamp:
                    it? Date.parse(ScheduledJob.DATE_FORMAT, it) : null)
        }

        when: "first import"
        def sjob = new ScheduledJob("id", importer, ds, whelk)
        sjob.run()
        then:
        whelk.state[ds].lastImport == imports[0]
        !whelk.state[ds].lock

        and: "none in this one"
        sjob.run()
        then:
        whelk.state[ds].lastImport == imports[0]

        and: "something new"
        sjob.run()
        then:
        whelk.state[ds].lastImport == imports[2]

        and: "nothing new"
        sjob.run()
        then:
        whelk.state[ds].lastImport == imports[2]
    }

}
