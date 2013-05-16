package se.kb.libris.whelks

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.imports.*
import se.kb.libris.whelks.plugin.*

import java.util.concurrent.*


/*
 * StandardWhelk (formerly WhelkImpl/BasicWhelk) has moved to StandardWhelk.groovy.
 * CombinedWhelk has moved to CombinedWhelk.groovy.
 * WhelkOperator has moved to WhelkOperator.groovy.
 */

@Log
class ReindexOnStartupWhelk extends StandardWhelk {

    ReindexOnStartupWhelk(String pfx) {
        super(pfx)
    }

    @Override
    public void init() {
        log.info("Indexing whelk ${this.prefix}.")
        reindex()
        log.info("Whelk reindexed.")
    }
}


@Log
class ResourceWhelk extends StandardWhelk {

    ResourceWhelk(String prefix) {
        super(prefix)
    }

    URI store(Document doc) {
        doc = sanityCheck(doc)

        for (storage in storages) {
            storage.store(doc, this.id)
        }

        return doc.identifier
    }
}

@Log
class ThreadedReindexingWhelk extends StandardWhelk {
    ThreadedReindexingWhelk(String pfx) {super(pfx)}

    @Override
    void reindex(fromStorage=null) {
        int counter = 0
        Storage scomp
        if (fromStorage) {
            scomp = components.find { it instanceof Storage && it.id == fromStorage }
        } else {
            scomp = components.find { it instanceof Storage }
        }
        Index icomp = components.find { it instanceof Index }
        def ifcs = plugins.findAll { it instanceof IndexFormatConverter }

        log.info("Using $scomp as storage source for reindex.")

        long startTime = System.currentTimeMillis()
        List<Document> docs = new ArrayList<Document>()
        try {
            for (Document doc : scomp.getAll(this.prefix)) {
                counter++
                docs.add(doc)
                if (counter % History.BATCH_SIZE == 0) {
                    long ts = System.currentTimeMillis()
                    log.info "(" + ((ts - startTime)/History.BATCH_SIZE) + ") New batch, indexing document with id: ${doc.identifier}. Velocity: " + (counter/((ts - startTime)/History.BATCH_SIZE)) + " documents per second. $counter total sofar."
                    def idocs = new ArrayList<Document>(docs)
                    executor.execute(new Runnable() {
                        public void run() {
                            for (ifc in ifcs) {
                                idocs = ifc.convertBulk(idocs)
                            }
                            log.debug("Current pool size: " + executor.getPoolSize() + " current active count " + executor.getActiveCount())
                            log.info("Indexing "+idocs.size()+" documents ... "+"Whelk prefix: "+this.getPrefix())
                            icomp.index(idocs, this.getPrefix())
                        }
                    })
                    docs.clear()
                }
            }
            if (docs.size() > 0) {
                log.info "Indexing remaining " + docs.size() + " documents."
                for (ifc in ifcs) {
                    log.trace("Running converter $ifc")
                    docs = ifc.convertBulk(docs)
                }
                if (icomp == null) {
                    log.warn("No index components configured for ${this.prefix} whelk.")
                    counter = 0
                } else {
                    log.info "Indexing remaining " + docs.size() + " documents."
                    icomp?.index(docs, this.prefix)
                }
            }
        } finally {
            executor.shutdown()
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow()
            }
        }
        log.debug "Reindexed $counter documents"
    }

    public ExecutorService newScalingThreadPoolExecutor(int min, int max, long keepAliveTime) {
        se.kb.libris.conch.ScalingQueue queue = new se.kb.libris.conch.ScalingQueue()
        java.util.concurrent.ThreadPoolExecutor executor = new se.kb.libris.conch.ScalingThreadPoolExecutor(min, max, keepAliveTime, TimeUnit.SECONDS, queue)
        executor.setRejectedExecutionHandler(new se.kb.libris.conch.ForceQueuePolicy())
        queue.setThreadPoolExecutor(executor)
        return executor
    }
}
