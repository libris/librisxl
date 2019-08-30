package whelk.plugin

import spock.lang.Ignore
import spock.lang.Specification
import whelk.harvester.HarvestResult
import whelk.harvester.OaiPmhHarvester
import whelk.servlet.ScheduledJob

@Ignore
class ScheduledOperatorSpec extends Specification {

    def "should update state on successful import"() {
        setup:
        def ds = "test"
        def is = [:]

        and:
        def imports = ["2001-01-01T00:00:00Z", null, "2002-02-02T00:00:00Z"]
        def importer = GroovyMock(OaiPmhHarvester)
        importer.serviceUrl >> "http://example.org/service"
        importer.harvest(_, _, _, _, _, _, _) >>> imports.collect {
            new HarvestResult(
                numberOfDocuments: it? 1 : 0,
                numberOfDocumentsDeleted: 0,
                lastRecordDatestamp:
                    it? Date.parse(ScheduledJob.DATE_FORMAT, it) : null)
        }

        when: "first import"
        def sjob = new ScheduledJob(importer, ds, null)
        sjob.run()
        then:
        Date.parse(ScheduledJob.DATE_FORMAT, is[ds].lastImport) == Date.parse(ScheduledJob.DATE_FORMAT, imports[0])

        and: "none in this one"
        sjob.run()
        then:
        Date.parse(ScheduledJob.DATE_FORMAT, is[ds].lastImport) == Date.parse(ScheduledJob.DATE_FORMAT, imports[0])

        and: "something new"
        sjob.run()
        then:
        Date.parse(ScheduledJob.DATE_FORMAT, is[ds].lastImport) == Date.parse(ScheduledJob.DATE_FORMAT, imports[2])

        and: "nothing new"
        sjob.run()
        then:
        Date.parse(ScheduledJob.DATE_FORMAT, is[ds].lastImport) == Date.parse(ScheduledJob.DATE_FORMAT, imports[2])
    }

}
