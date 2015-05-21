package whelk.plugin

import groovy.util.logging.Slf4j as Log

import java.util.concurrent.*

import whelk.Whelk

@Log
class ScheduledOperator extends BasicPlugin {

    String description = "Scheduled operator runner."

    ScheduledExecutorService ses

    Map configuration

    ScheduledOperator(Map settings) {
        this.configuration = settings
    }

    void bootstrap() {
        if (System.getProperty("whelk.mode", "") != "ops") {
            ses = Executors.newScheduledThreadPool(configuration.size())
            configuration.each { task, conf ->
                log.debug("Setting up schedule for $task : $conf")
                def imp = getPlugin(conf.importer)
                assert imp
                imp.serviceUrl = conf.url
                def job = new ScheduledJob(task, imp, conf.dataset, whelk)
                try {
                    ses.scheduleWithFixedDelay(job, 30, conf.interval, TimeUnit.SECONDS)
                    log.info("${task} will start in 30 seconds.")
                } catch (RejectedExecutionException ree) {
                    log.error("execution failed", ree)
                }
            }
        } else {
            log.info("Whelk ${this.whelk.id} is started in operations mode. No tasks scheduled.")
        }
    }

    void testJob() {
        log.info("Test job!")
        def j = new ScheduledJob("test", getPlugin("oaipmhimporter"), "bib", whelk)
        j.run()
        log.info("Test complete!")
    }
}

@Log
class ScheduledJob implements Runnable {

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

    String id, dataset
    Importer importer
    Whelk whelk

    ScheduledJob(String id, whelk.plugin.Importer imp, String ds, Whelk w) {
        this.id = id
        this.importer = imp
        this.dataset = ds
        this.whelk = w
    }


    void run() {
        assert dataset

        if (whelk.acquireLock(dataset)) {
            log.trace("Lock acquired for $dataset")
        } else {
            log.trace("[${this.id}] Whelk is busy for dataset $dataset")
            return
        }

        try {
            log.trace("all state: ${whelk.state}")
            def whelkState = whelk.state.get(dataset) ?: [:]
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
            whelkState.put("importOperator", this.id)
            whelkState.remove("lastImportOperator")
            whelk.updateState(dataset, whelkState)
            def result = importer.doImport(dataset, null, -1, true, true, nextSince)

            int totalCount = result.numberOfDocuments
            if (result.numberOfDocuments > 0 || result.numberOfDeleted > 0) {
                log.info("Imported ${result.numberOfDocuments} documents and deleted ${result.numberOfDeleted} for $dataset.")
                whelkState.put("lastImportNrImported", result.numberOfDocuments)
                whelkState.put("lastImportNrDeleted", result.numberOfDeleted)
                whelkState.put("lastImport", result.lastRecordDatestamp.format(DATE_FORMAT))
            } else {
                log.debug("Imported ${result.numberOfDocuments} document for $dataset.")
                whelkState.put("lastImport", currentSince.format(DATE_FORMAT))
            }
            whelkState.remove("importOperator")
            whelkState.put("status", "IDLE")
            whelkState.put("lastRunNrImported", result.numberOfDocuments)
            whelkState.put("lastRun", new Date().format(DATE_FORMAT))
            whelk.updateState(dataset, whelkState)

        } catch (Exception e) {
            log.error("Something failed: ${e.message}", e)
        } finally {
            whelk.releaseLock(dataset)
            log.trace("Lock released for $dataset")
        }
    }

}
