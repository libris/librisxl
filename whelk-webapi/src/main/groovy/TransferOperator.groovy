package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.map.SerializationConfig.Feature

import java.util.concurrent.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.component.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.importers.*
import se.kb.libris.whelks.plugin.*

import se.kb.libris.conch.Tools

@Log
class TransferOperator extends AbstractOperator {
    String oid = "transfer"

    // Unique for this operator
    String fromStorage = null
    String toStorage = null

    boolean showSpinner = false

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        this.fromStorage = parameters.get("fromStorage", null)
        this.toStorage = parameters.get("toStorage", null)
        this.showSpinner = parameters.get("showSpinner", false)
        assert fromStorage
        assert toStorage
    }

    void doRun(long startTime) {
        log.info("Transferring data from $fromStorage to $toStorage")
        Storage targetStorage = whelk.getStorages().find { it.id == toStorage }
        targetStorage.versioning = false
        for (doc in whelk.loadAll(dataset, null, fromStorage)) {
            log.trace("Storing doc ${doc.identifier} with type ${doc.contentType}")
            if (!doc.entry.deleted) {
                if (targetStorage.store(doc)) {
                    count++
                }
            } else {
                log.warn("Document ${doc.identifier} is deleted. Don't try to add it.")
            }
            /*
            if (++count % 1000 == 0) { // Bulk index 1000 docs at a time
                doTheIndexing(docs)
                docs = []
            }
            */
            runningTime = System.currentTimeMillis() - startTime
            if (showSpinner) {
                def velocityMsg = "Current velocity: ${count/(runningTime/1000)}."
                Tools.printSpinner("Transferring from ${fromStorage} to ${toStorage}. ${count} documents transferred sofar. $velocityMsg", count)
            }
            if (cancelled) {
                break
            }
        }
        /*
        log.debug("Went through all documents. Processing remainder.")
        if (docs.size() > 0) {
            log.trace("Reindexing remaining ${docs.size()} documents")
            try {
                whelk.addToGraphStore(docs, selectedComponents)
            } catch (WhelkAddException wae) {
                //errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed adding identifiers to graphstore: ${wae.failedIdentifiers as String}")
            }
            try {
                whelk.addToIndex(docs, selectedComponents)
            } catch (WhelkAddException wae) {
                //errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed adding identifiers to graphstore: ${wae.failedIdentifiers as String}")
            }
        }
        */
        log.info("Transferred $count documents in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds." as String)
        /*
        if (!dataset) {
            for (index in whelk.indexes) {
                if (!selectedComponents || index in selectedComponents) {
                    if (cancelled) {
                        log.info("Process cancelled, resetting currentIndex")
                        index.currentIndex = index.getRealIndexFor(index.elasticIndex)
                    } else {
                        index.reMapAliases()
                    }
                }
            }
        }
        */
        operatorState=OperatorState.FINISHING
        this.whelk.flush()
    }

    /*
    void doTheIndexing(final List docs) {
        queue.execute({
            try {
                whelk.addToGraphStore(docs, selectedComponents)
            } catch (WhelkAddException wae) {
                //errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed adding identifiers to graphstore: ${wae.failedIdentifiers}")
            }
            try {
                whelk.addToIndex(docs, selectedComponents)
            } catch (WhelkAddException wae) {
                //errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed indexing identifiers: ${wae.failedIdentifiers}")
            } catch (PluginConfigurationException pce) {
                log.error("System badly configured", pce)
                throw pce
            }
        } as Runnable)
    }
    */

    @Override
    Map getStatus() {
        def map = super.getStatus()
        if (hasRun) {
            if (fromStorage) {
                map.get("lastrun").put("fromStorage", fromStorage)
                map.get("lastrun").put("toStorage", toStorage)
            }
        } else {
            if (fromStorage) {
                map.put("fromStorage", fromStorage)
                map.put("toStorage", toStorage)
            }
        }
        return map
    }
}
