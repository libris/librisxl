package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.map.SerializationConfig.Feature

import java.util.concurrent.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.importers.*
import se.kb.libris.whelks.plugin.*

import se.kb.libris.conch.Tools

@Log
class ReindexOperator extends AbstractOperator {
    String oid = "reindex"

    // Unique for this operator
    List<String> selectedComponents = null
    String startAt = null
    String fromStorage = null
    ExecutorService gstoreQueue
    ExecutorService indexQueue
    Semaphore gstoreAvailable
    Semaphore indexAvailable

    boolean showSpinner = false

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        log.debug("parameters: $parameters")
        if (parameters.selectedComponents) {
            this.selectedComponents = parameters.get("selectedComponents").first().split(",") as List<String>
        }
        this.fromStorage = parameters.get("fromStorage", null)?.first()
        this.showSpinner = parameters.get("showSpinner", false)
    }

    void doRun(long startTime) {
        String indexName = whelk.id

        log.info("Starting reindexing.")

        if (!dataset && doIndexing) {
            log.debug("Requesting new index for ${whelk.index.id}.")
            indexName = whelk.index.createNewCurrentIndex(whelk.id)
        }
        if (fromStorage) {
            log.info("Rebuilding storage from $fromStorage")
        }
        for (doc in whelk.loadAll(dataset, null, fromStorage)) {
            log.trace("Loaded doc ${doc.identifier} with type ${doc.contentType}")
            if (!doc.entry.deleted) {
                if (fromStorage) {
                    try {
                        for (strg in whelk.storages) {
                            if (strg.id != fromStorage) {
                                strg.add(doc)
                            }
                        }
                    } catch (WhelkAddException wae) {
                        log.trace("Expected exception ${wae.message}")
                    }
                }
                whelk.storage.notifyCamel(doc)
                count++
            } else {
                log.warn("Document ${doc.identifier} is deleted. Don't try to add it.")
            }
            runningTime = System.currentTimeMillis() - startTime
            if (showSpinner) {
                def velocityMsg = "Current velocity: ${count/(runningTime/1000)}."
                Tools.printSpinner("Rebuilding from ${fromStorage}. ${count} documents rebuilt sofar. $velocityMsg", count)
            }
            if (cancelled) {
                break
            }
        }
        log.info("Reindexed $count documents in ${((System.currentTimeMillis() - startTime)/1000)} seconds.")
        // TODO: This will happen before indexing has taken place. Find a way to do this at the end of the queue
        whelk.index.reMapAliases(whelk.id)
    }

    @Override
    Map getStatus() {
        def map = super.getStatus()
        if (hasRun) {
            if (fromStorage) {
                map.get("lastrun")?.put("fromStorage", fromStorage)
            }
            if (selectedComponents) {
                map.get("lastrun")?.put("selectedComponents", selectedComponents)
            }
        } else {
            if (fromStorage) {
                map.put("fromStorage", fromStorage)
            }
            if (selectedComponents) {
                map.put("selectedComponents", selectedComponents)
            }
        }
        return map
    }
}

@Log
class RebuildMetaIndexOperator extends AbstractOperator {
    String oid = "rebuild"
    boolean sequenceOnly = false

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        log.debug("parameters: $parameters")
    }
    void doRun(long startTime) {
        whelk.storage.rebuildIndex()
    }
}
}
