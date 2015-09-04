package whelk.scheduler

import groovy.util.logging.Slf4j as Log
import whelk.Document
import whelk.component.PostgreSQLComponent

import java.util.concurrent.*

import whelk.importer.OaiPmhImporter


@Log
class ScheduledOperator {

    String description = "Scheduled operator runner."

    int scheduleDelaySeconds = 10
    int scheduleIntervalSeconds = 30

    ScheduledExecutorService ses
    OaiPmhImporter oaiPmhImporter
    PostgreSQLComponent storage

    ScheduledOperator(OaiPmhImporter opi, PostgreSQLComponent pg) {
        oaiPmhImporter = opi
        ses = Executors.newScheduledThreadPool(3)
        storage = pg
    }

    void start() {
        for (dataset in ["auth", "bib", "hold"]) {
            log.info("Setting up schedule for $dataset")
            def job = new ScheduledJob(oaiPmhImporter, dataset, storage)
            try {
                ses.scheduleWithFixedDelay(job, scheduleDelaySeconds, scheduleIntervalSeconds, TimeUnit.SECONDS)
            } catch (RejectedExecutionException ree) {
                log.error("execution failed", ree)
            }
        }
        log.info("scheduler started")
    }

    void testJob() {
        log.info("Test job!")
        def j = new ScheduledJob("test", getPlugin("oaipmhimporter"), "bib")
        j.run()
        log.info("Test complete!")
    }
}

@Log
class ScheduledJob implements Runnable {

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"

    String dataset
    OaiPmhImporter importer
    PostgreSQLComponent storage

    ScheduledJob(OaiPmhImporter imp, String ds, PostgreSQLComponent pg) {
        this.importer = imp
        this.dataset = ds
        this.storage = pg
    }


    void run() {
        assert dataset

        def whelkState = storage.load("/sys/whelk.state")?.data ?: [:]
        log.info("Current whelkstate: $whelkState")
        try {
            String lastImport = whelkState.get("lastImport")
            Date currentSince
            Date nextSince = new Date()
            if (lastImport) {
                log.trace("Parsing $lastImport as date")
                currentSince = Date.parse(DATE_FORMAT, lastImport)
                nextSince = new Date(currentSince.getTime())
                nextSince.set(second: currentSince[Calendar.SECOND] + 1)
                log.trace("Next since (upped by 1 second): $nextSince")
            } else {
                def lastWeeksDate = nextSince[Calendar.DATE] - 7
                nextSince.set(date: lastWeeksDate)
                currentSince = nextSince
                log.info("Whelk has no state for last import from $dataset. Setting last week (${nextSince})")
            }
            log.debug("Executing OAIPMH import for $dataset since $nextSince from ${importer.serviceUrl}")
            whelkState.put("status", "RUNNING")
            whelkState.put("importOperator", dataset)
            whelkState.remove("lastImportOperator")
            def result = importer.doImport(dataset, null, -1, true, true, nextSince)

            int totalCount = result.numberOfDocuments
            if (result.numberOfDocuments > 0 || result.numberOfDeleted > 0 || result.numberOfDocumentsSkipped > 0) {
                log.info("Imported ${result.numberOfDocuments} documents and deleted ${result.numberOfDeleted} for $dataset. Last record has datestamp: ${result.lastRecordDatestamp.format(DATE_FORMAT)}")
                whelkState.put("lastImportNrImported", result.numberOfDocuments)
                whelkState.put("lastImportNrDeleted", result.numberOfDeleted)
                whelkState.put("lastImportNrSkipped", result.numberOfDocumentsSkipped)
                whelkState.put("lastImport", result.lastRecordDatestamp.format(DATE_FORMAT))
            } else {
                log.debug("Imported ${result.numberOfDocuments} document for $dataset.")
                whelkState.put("lastImport", currentSince.format(DATE_FORMAT))
            }
            whelkState.remove("importOperator")
            whelkState.put("status", "IDLE")
            whelkState.put("lastRunNrImported", result.numberOfDocuments)
            whelkState.put("lastRun", new Date().format(DATE_FORMAT))

        } catch (Exception e) {
            log.error("Something failed: ${e.message}", e)
        } finally {
            storage.store(new Document("/sys/whelk.state", whelkState))
        }
    }

}
