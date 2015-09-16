package whelk.scheduler

import groovy.util.logging.Slf4j as Log
import whelk.Document
import whelk.component.PostgreSQLComponent

import java.util.concurrent.*

import whelk.importer.OaiPmhImporter


@Log
class ScheduledOperator {

    String description = "Scheduled operator runner."




    PostgreSQLComponent storage

    ScheduledOperator(OaiPmhImporter opi, PostgreSQLComponent pg) {
        oaiPmhImporter = opi
        ses = Executors.newScheduledThreadPool(3)
        storage = pg
    }



    void testJob() {
        log.info("Test job!")
        def j = new ScheduledJob("test", getPlugin("oaipmhimporter"), "bib")
        j.run()
        log.info("Test complete!")
    }
}

