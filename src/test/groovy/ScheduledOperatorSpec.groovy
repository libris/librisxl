package whelk.plugin

import whelk.StandardWhelk
import whelk.result.ImportResult

import spock.lang.Specification


class ScheduledOperatorSpec extends Specification {

    def "should update state on successful import"() {
        setup:
        def ds = "test"
        def whelk = Mock(StandardWhelk)
        def state = [:]
        whelk.acquireLock(ds) >> true
        whelk.getState() >> state
        whelk.updateState(_, _) >> { key, newState -> state[key] = newState }

        and:
        def imports = ["2001-01-01T00:00:00Z", "2002-02-02T00:00:00Z"]
        def importer = GroovyMock(Importer)
        importer.serviceUrl >> "http://example.org/service"
        importer.doImport(_, _, _, _, _, _) >>> imports.collect {
            new ImportResult(
                numberOfDocuments: 1,
                numberOfDeleted: 1,
                lastRecordDatestamp: Date.parse(ScheduledJob.DATE_FORMAT, it))
        }

        when:
        def sjob = new ScheduledJob("id", importer, ds, whelk)
        sjob.run()
        then:
        state[ds].lastImport == imports[0]

        and: "again"
        sjob.run()
        then:
        state[ds].lastImport == imports[1]

        and: "yet again"
        sjob.run()
        then:
        state[ds].lastImport == imports[1]
    }

}
