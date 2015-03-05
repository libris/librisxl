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
        ses = Executors.newScheduledThreadPool(configuration.size())
        configuration.each { task, conf ->
            log.debug("Setting up schedule for $task : $conf")
            def imp = getPlugin(conf.importer)
            assert imp
            imp.serviceUrl = conf.url
            def job = new ScheduledJob(task, imp, conf.dataset, whelk)
            try {
                ses.scheduleWithFixedDelay(job, 30, conf.interval, TimeUnit.SECONDS)
                log.info("${job.id} will start in 30 seconds.")
            } catch (RejectedExecutionException ree) {
                log.error("execution failed", ree)
            }
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
            log.debug("Lock acquired for $dataset")
        } else {
            log.debug("[${this.id}] Whelk is busy for dataset $dataset")
            return
        }

        try {
            log.trace("all state: ${whelk.state}")
            def whelkState = whelk.state.get(dataset) ?: [:]
            String dString = whelkState.get("lastImport")
            Date since
            if (dString) {
                log.trace("Parsing $dString as date")
                since = Date.parse("yyyy-MM-dd'T'HH:mm:ss'Z'", dString)
            } else {
                since = new Date()
                def lastWeeksDate = since[Calendar.DATE] - 7
                since.set(date: lastWeeksDate)
                log.info("Whelk has no state for last import from $dataset. Setting last week (${since})")
            }
            log.debug("Executing OAIPMH import for $dataset since $since from ${importer.serviceUrl}")
            whelkState.put("status", "RUNNING")
            whelkState.put("importOperator", this.id)
            whelkState.remove("lastImportOperator")
            whelk.updateState(dataset, whelkState)
            Date dateStamp = new Date()
            int totalCount = importer.doImport(dataset, null, -1, true, true, since)
            if (totalCount > 0) {
                log.info("Imported $totalCount document for $dataset.")
            } else {
                log.debug("Imported $totalCount document for $dataset.")
            }
            whelkState.remove("importOperator")
            whelkState.put("status", "IDLE")
            whelkState.put("lastImport", dateStamp.format("yyyy-MM-dd'T'HH:mm:ss'Z'"))
            whelkState.put("lastRunNrImported", totalCount)
            whelk.updateState(dataset, whelkState)

        } catch (Exception e) {
            log.error("Something failed: ${e.message}", e)
        } finally {
            whelk.releaseLock(dataset)
            log.debug("Lock released for $dataset")
        }
    }

}
