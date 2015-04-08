package whelk.api

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.map.SerializationConfig.Feature

import java.util.concurrent.*

import whelk.*
import whelk.exception.*
import whelk.importer.*
import whelk.plugin.*

import whelk.util.Tools

@Log
class ReindexOperator extends AbstractOperator {
    String oid = "reindex"

    // Unique for this operator
    List<String> selectedComponents = null
    String startAt = null
    String fromStorage = null
    String toIndex = null

    boolean showSpinner = false
    int reindexcount = 0

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        log.debug("parameters: $parameters")
        if (parameters.selectedComponents) {
            this.selectedComponents = parameters.get("selectedComponents").split(",") as List<String>
        }
        this.fromStorage = parameters.get("fromStorage")
        this.toIndex = parameters.get("toIndex")
        this.showSpinner = parameters.get("showSpinner", false)
    }

    int getCount() { reindexcount }

    void doRun() {
        if (whelk.state.locked) {
            log.info("Whelk is currently busy. Don't do anything.")
            return
        }
        try {
            whelk.acquireLock(dataset)
            String indexName = toIndex ?: whelk.index.getIndexName()
            reindexcount = 0

            log.info("Starting reindexing into $indexName.")

            if (fromStorage) {
                log.info("Loading data from $fromStorage")
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
                        whelk.notifyCamel(doc, Whelk.BULK_ADD_OPERATION, ["index":indexName])
                        reindexcount++
                    } else {
                        log.debug("Document ${doc.identifier} is deleted. Don't try to add it.")
                        whelk.notifyCamel(doc.identifier, doc.dataset, Whelk.REMOVE_OPERATION, ["index":indexName])
                    }
                if (cancelled) {
                    break
                }
            }
        } finally {
            whelk.releaseLock(dataset)
        }
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
    String storageId = null

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        this.storageId = parameters.get("storage", null)?.first()
        log.debug("parameters: $parameters")
    }
    int getCount() { 0 }
    void doRun() {
        if (storageId) {
            def store = whelk.storages.find { it.id == storageId }
            if (store) {
                log.info("Rebuilding meta index for storage ${storageId}.")
                store.rebuildIndex()
            } else {
                log.warn("No storage found with id $storageId")
            }
        } else {
            log.info("Rebuilding meta index for default storage.")
            whelk.storage.rebuildIndex()
        }
    }
}
