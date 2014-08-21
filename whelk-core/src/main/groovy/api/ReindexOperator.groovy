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

    boolean doIndexing = true
    boolean doGraphing = true

    int indexingSemaphores = 50
    int graphstoreSemaphores = 1000
    int indexBatchSize = 1000
    int graphBatchSize = 1000

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        log.info("parameters: $parameters")
        if (parameters.selectedComponents) {
            this.selectedComponents = parameters.get("selectedComponents").first().split(",") as List<String>
        }
        this.fromStorage = parameters.get("fromStorage", null)?.first()
        this.showSpinner = parameters.get("showSpinner", false)
        this.indexingSemaphores = parameters.get("indexingQueueSize", 50)
        this.graphstoreSemaphores = parameters.get("graphstoreQueueSize", 1000)
        this.indexBatchSize = parameters.get("indexBatchSize", 1000)
        this.graphBatchSize = parameters.get("graphBatchSize", 1000)
        this.doIndexing = (whelk.index != null && whelk.index.id != parameters.get("disableComponent")?.first())
        this.doGraphing = (whelk.graphStore != null && whelk.graphStore.id != parameters.get("disableComponent")?.first())
    }

    void doRun(long startTime) {
        List<Document> indexdocs = []
        List<Document> graphdocs = []
        /*
        gstoreQueue = Executors.newSingleThreadExecutor()
        */
        indexQueue = Executors.newFixedThreadPool(3)
        gstoreAvailable = new Semaphore(graphstoreSemaphores)
        indexAvailable = new Semaphore(indexingSemaphores)
        String indexName = whelk.id

        long newestDocumentTimestamp = 0L

        log.info("Starting reindexing.")
        if (doIndexing) {
            log.info("Index batch size: $indexBatchSize")
            log.info("Index queue size: $indexingSemaphores")
        }
        if (doGraphing) {
            log.info("Graph batch size: $indexBatchSize")
            log.info("Graph queue size: $graphstoreSemaphores")
        }

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
                if (doc.timestamp > newestDocumentTimestamp) {
                    newestDocumentTimestamp = doc.timestamp
                }
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
                /*
                indexdocs << doc
                graphdocs << doc
                */
                whelk.storage.notifyCamel(doc)
            } else {
                log.warn("Document ${doc.identifier} is deleted. Don't try to add it.")
            }
            /*
            if (++count % indexBatchSize == 0) { // Bulk index 1000 docs at a time
                doTheIndexing(indexdocs, indexName)
                indexdocs = []
            }
            if (count % graphBatchSize == 0) {
                doGraphIndexing(graphdocs)
                graphdocs = []
            }
            */
            runningTime = System.currentTimeMillis() - startTime
            if (showSpinner) {
                def velocityMsg = "Current velocity: ${count/(runningTime/1000)}."
                Tools.printSpinner("Rebuilding from ${fromStorage}. ${count} documents rebuilt sofar. $velocityMsg", count)
            }
            if (cancelled) {
                break
            }
        }
        /*
        log.debug("Went through all documents. Processing remainder.")
        if (graphdocs.size() > 0 && whelk.graphStore && doGraphing) {
            log.trace("Reindexing remaining ${graphdocs.size()} documents")
            try {
                whelk.graphStore.bulkAdd(graphdocs, graphdocs.first().contentType)
            } catch (WhelkAddException wae) {
                //errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed adding identifiers to graphstore: ${wae.failedIdentifiers as String}")
            }
        }
        if (indexdocs.size() > 0 && whelk.index && doIndexing) {
            try {
                def preparedDocs = whelk.index.prepareDocs(indexdocs, indexdocs.first().contentType)
                whelk.index.addDocuments(preparedDocs, indexName)
            } catch (WhelkAddException wae) {
                //errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed adding identifiers to graphstore: ${wae.failedIdentifiers as String}")
            }
        }
        */
        log.info("Reindexed $count documents in ${((System.currentTimeMillis() - startTime)/1000)} seconds.")
        if (doIndexing && !dataset && !cancelled) {
            indexQueue.execute({
                whelk.index.reMapAliases(whelk.id)
            })
        }
        operatorState=OperatorState.FINISHING
        /*
        indexQueue.execute({
            this.whelk.flush()
            //this.whelk.index.setState(whelk.index.LAST_UPDATED, newestDocumentTimestamp)
        } as Runnable)
        */
        indexQueue.shutdown()
        /*
        gstoreQueue.execute({
            this.whelk.graphstore.setState(whelk.graphstore.LAST_UPDATED, newestDocumentTimestamp)
        } as Runnable)
        gstoreQueue.shutdown()
        */
    }

    void doTheIndexing(final List docs, String indexName) {
        if (doIndexing && whelk.index) {
            if (indexAvailable.availablePermits() < 10) {
                log.info("Trying to acquire semaphore for indexing. ${indexAvailable.availablePermits()} available.")
            }
            indexAvailable.acquire()
            log.debug("Acquired.")
            indexQueue.execute({
                try {
                    // Bypass component.bulkAdd() to be able to index into indexName
                    def preparedDocs = whelk.index.prepareDocs(docs, docs.first().contentType)
                    whelk.index.addDocuments(preparedDocs, indexName)
                    whelk.index.setState(whelk.index.LAST_UPDATED, new Date().getTime())
                } catch (WhelkAddException wae) {
                    log.warn("Failed indexing identifiers: ${wae.failedIdentifiers}")
                } catch (PluginConfigurationException pce) {
                    log.error("System badly configured", pce)
                throw pce
                } finally {
                    indexAvailable.release()
                log.debug("Released indexing semaphore. ${indexAvailable.availablePermits()} available.")
                }
            } as Runnable)
        }
    }

    void doGraphIndexing(final List docs) {
        if (doGraphing && whelk.graphStore) {
            log.info("Trying to acquire semaphore for graphstore. ${gstoreAvailable.availablePermits()} available.")
            gstoreAvailable.acquire()
            gstoreQueue.execute({
                try {
                    whelk.graphStore.bulkAdd(docs, docs.first().contentType)
                } catch (WhelkAddException wae) {
                    log.warn("Failed adding identifiers to graphstore: ${wae.failedIdentifiers}")
                } finally {
                    gstoreAvailable.release()
                    log.info("Released graphstore semaphore. ${gstoreAvailable.availablePermits()} available.")
                }
            } as Runnable)
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

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        log.info("parameters: $parameters")
        sequenceOnly = (parameters.get("sequenceOnly", ["false"]).first() == "true")
    }
    void doRun(long startTime) {
        if (sequenceOnly) {
            whelk.storage.updateSequenceNumbers()
        } else {
            whelk.storage.rebuildIndex()
        }
    }
}

@Log
class PingCatchupOperator extends AbstractOperator {
    String oid = "ping"

    long pingSince = 0L

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        log.info("parameters: $parameters")
        pingSince = new Long(parameters.get("since", ["0"]).first()).longValue()
    }

    void doRun(long startTime) {
        log.info("Pinging with lastupdated = $pingSince")
        log.info("Whelk is $whelk, Storage is ${whelk.storage}, Listener is: ${whelk.storage.listener}")
        def listener = whelk.storage.listener
        listener.registerUpdate(whelk.storage.id, pingSince, true)
    }
}
