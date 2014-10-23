package whelk.api

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.*
import org.codehaus.jackson.map.SerializationConfig.Feature

import java.util.concurrent.*

import whelk.*
import whelk.component.*
import whelk.exception.*
import whelk.importer.*
import whelk.plugin.*

import whelk.util.Tools

@Log
@Deprecated
class TransferOperator extends AbstractOperator implements Plugin {
    String oid = "transfer"

    // Unique for this operator
    String fromStorage = null
    String toStorage = null

    boolean showSpinner = false
    ExecutorService queue

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        this.fromStorage = parameters.get("fromStorage", null)?.first()
        this.toStorage = parameters.get("toStorage", null)?.first()
        this.showSpinner = parameters.get("showSpinner", false)
        assert fromStorage
        assert toStorage
    }

    void doRun(long startTime) {
        Storage targetStorage = whelk.getStorages().find { it.id == toStorage }
        Storage sourceStorage = whelk.getStorages().find { it.id == fromStorage }
        assert targetStorage, sourceStorage
        queue = Executors.newSingleThreadExecutor()
        List docs = []
        log.info("Transferring data from $fromStorage to $toStorage")
        boolean versioningOriginalSetting = targetStorage.versioning
        targetStorage.versioning = false

        FormatConverter fc = plugins.find {
            it instanceof FormatConverter &&
            it.requiredContentType == sourceStorage.contentTypes.first() &&
            it.resultContentType == targetStorage.contentTypes.first()
        }
        Filter filter = plugins.find {
            it instanceof Filter && ((!fc && it.requiredContentType == sourceStorage.contentTypes.first()) ||
            (!fc || fc.resultContentType == it.requiredContentType))
        }
        if (fc) {
            log.info("Using formatconverter ${fc.id}")
        }
        if (filter) {
            log.info("Using filter ${filter.id}")
        }

        for (doc in sourceStorage.getAll(dataset)) {
            log.trace("Storing doc ${doc.identifier} with type ${doc.contentType}")
            if (doc && !doc.entry.deleted) {
                try {
                    if (fc) {
                        if (filter) {
                            docs << filter.filter(fc.convert(doc))
                        } else {
                            docs << fc.convert(doc)
                        }
                    } else {
                        if (filter) {
                            docs << filter.filter(doc)
                        } else {
                            docs << doc
                        }
                    }
                } catch (Exception e) {
                    log.error("Failed to transmogrify document ${doc?.identifier}")
                    throw e
                }
            } else {
                log.warn("Document ${doc?.identifier} is deleted. Don't try to add it.")
            }
            runningTime = System.currentTimeMillis() - startTime
            if (count++ % 20000 == 0) {
                addDocs(docs, targetStorage)
                docs = []
            }
            if (showSpinner) {
                def velocityMsg = "Current velocity: ${count/(runningTime/1000)}."
                Tools.printSpinner("Transferring from ${fromStorage} to ${toStorage}. ${count} documents transferred sofar. $velocityMsg", count)
            }
            if (cancelled) {
                break
            }
        }

        if (docs.size() > 0) {
            queue.execute({
                addDocs(docs, targetStorage)
            } as Runnable)
        }
        log.info("Transferred $count documents in ${((System.currentTimeMillis() - startTime)/1000)} seconds." as String)
        targetStorage.versioning = versioningOriginalSetting
        operatorState=OperatorState.FINISHING
        log.debug("Shutting down queue")
        queue.shutdown()
    }

    private addDocs(final List<Document> docs, Storage targetStorage) {
        log.info("Saving batch")
        queue.execute({
            targetStorage.bulkStore(docs)
        } as Runnable)
    }

    @Override
    Map getStatus() {
        def map = super.getStatus()
        if (hasRun) {
            if (fromStorage) {
                map.get("lastrun", [:]).put("fromStorage", fromStorage)
                map.get("lastrun", [:]).put("toStorage", toStorage)
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
