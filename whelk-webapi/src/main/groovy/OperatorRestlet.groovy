package se.kb.libris.whelks.api

import groovy.util.logging.Slf4j as Log

import org.restlet.*
import org.restlet.data.*
import org.restlet.resource.*
import org.restlet.representation.*

import org.codehaus.jackson.map.*
import org.codehaus.jackson.map.SerializationConfig.Feature

import java.util.concurrent.*

import se.kb.libris.whelks.*
import se.kb.libris.whelks.exception.*
import se.kb.libris.whelks.importers.*
import se.kb.libris.whelks.plugin.*

@Log
class OperatorRestlet extends BasicWhelkAPI implements RestAPI {

    def pathEnd = "_operations"
    def varPath = false

    String description = "Web based whelk operator"
    String id = "operations"
    ObjectMapper mapper = new ObjectMapper()


    static final OPERATIONS_HTML_FILE = "operator.html"

    static BenchmarkOperator benchmarkOperator = new BenchmarkOperator()
    static ReindexOperator reindexOperator = new ReindexOperator()
    static ImportOperator importOperator = new ImportOperator()

    static Map operators = ["reindex":reindexOperator,
                            "import":importOperator,
                            "benchmark":benchmarkOperator]

    void doHandle(Request request, Response response) {
        def req = [:]
        if (request.method == Method.POST) {
            req = new Form(request.getEntity()).getValuesMap()
        } else {
            req = request.getResourceRef().getQueryAsForm().getValuesMap()
        }
        if (!isBusy() && operators.containsKey(req.operation)) {
            log.debug("Starting ${req.operation} operation")
            def op = operators[req.operation]
            op.whelk = this.whelk
            op.setParameters(req)
            new Thread(op).start()
            response.setStatus(Status.REDIRECTION_SEE_OTHER, "Operation ${req.operation} started.")
            response.setLocationRef(request.getRootRef().toString()+"/"+pathEnd)
        }
        if (isBusy() && operators.containsKey(req.cancel)) {
            log.debug("Cancelling operation ${req.cancel}")
            operators[req.cancel].cancel()
        }

        if (isBusy() || req.status || request.getResourceRef().getQuery() == "status") {
            def opMap = [:]
            operators.each { k,v ->
                opMap[k] = v.status
            }
            // Make output pretty
            mapper.enable(Feature.INDENT_OUTPUT)
            def page = mapper.writeValueAsString(["operations":opMap])

            response.setEntity(page, MediaType.APPLICATION_JSON)
        } else {
            response.setEntity(loadPage(), MediaType.TEXT_HTML)
        }
    }

    boolean isBusy() {
        boolean busy = operators.values().any { it.operatorState == OperatorState.RUNNING }
        return busy
    }

    String loadPage() {
        try {
            return getClass().classLoader.getResourceAsStream(OPERATIONS_HTML_FILE).text
        } catch (NullPointerException npe) {
            throw new WhelkRuntimeException("Couldn't find file $OPERATIONS_HTML_FILE.")
        }
    }
}

@Log
class ImportOperator extends AbstractOperator {
    String oid = "import"

    String importerPlugin = null
    String serviceUrl = null
    int numToImport = -1
    Date since = null
    boolean picky = true

    Importer importer = null

    long startTime

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        this.importerPlugin = parameters.get("importer", null)
        this.serviceUrl = parameters.get("url", null)
        this.numToImport = parameters.get("nums", -1) as int
        if (parameters.get("since", null)) {
            this.since = Date.parse('yyyy-MM-dd', parameters.get("since"))
        }
    }

    void doRun(long startTime) {
        this.startTime = startTime
        assert dataset
        if (importerPlugin) {
            importer = whelk.getImporter(importerPlugin)
        } else {
            def importers = whelk.getImporters()
            if (importers.size() > 1) {
                throw new WhelkRuntimeException("Multiple importers available for ${whelk.id}, you need to specify one with the 'importer' parameter.")
            } else {
                try {
                    importer = importers[0]
                } catch (IndexOutOfBoundsException e) { }
            }
        }
        if (!importer) {
            throw new WhelkRuntimeException("Couldn't find any importers working for ${whelk.id}.")
        }
        log.debug("Importer name: ${importer.getClass().getName()}")
        if (importer.getClass().getName() == "se.kb.libris.whelks.importers.OAIPMHImporter") {
            importer.serviceUrl = serviceUrl
            log.info("Import from OAIPMH")
            count = importer.doImport(dataset, numToImport, true, picky, since)
        } else {
            if (!serviceUrl) {
                throw new WhelkRuntimeException("URL is required for import.")
            }
            count = importer.doImport(dataset, numToImport, true, picky, new URL(serviceUrl))
        }
        runningTime = System.currentTimeMillis() - startTime
        /*
        long elapsed = ((System.currentTimeMillis() - startTime) / 1000)
        if (nrimports > 0 && elapsed > 0) {
            println "Imported $nrimports documents in $elapsed seconds. That's " + (nrimports / elapsed) + " documents per second."
        } else {
            println "Nothing imported ..."
        }
        */
    }

    @Override
    Map getStatus() {
        runningTime = System.currentTimeMillis() - startTime
        count = (importer ? importer.nrImported : 0)
        def status = super.getStatus()
        if (importer?.errorMessages) {
            if (operatorState == OperatorState.IDLE) {
                status.get("lastrun").put("errors", errorMessages)
            } else {
                status['errors'] = errorMessages
            }
        }
        return status
    }

    @Override
    void cancel() {
        this.importer.cancel()
    }
}

@Log
class ReindexOperator extends AbstractOperator {
    String oid = "reindex"

    // Unique for this operator
    List<String> selectedComponents = null
    String startAt = null
    String fromStorage = null
    ExecutorService queue

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        if (parameters.selectedComponents) {
            this.selectedComponents = parameters.get("selectedComponents").split(",") as List<String>
        }
        this.fromStorage = parameters.get("fromStorage", null)
    }

    void doRun(long startTime) {
        List<Document> docs = []
        boolean indexing = !startAt
        queue = Executors.newSingleThreadExecutor()
        def futures = [].asSynchronized()
        if (!dataset) {
            for (index in whelk.indexes) {
                if (!selectedComponents || index in selectedComponents) {
                    log.debug("Requesting new index for ${index.id}.")
                    index.createNewCurrentIndex()
                }
            }
        }
        for (doc in whelk.loadAll(dataset, fromStorage)) {
            if (startAt && doc.identifier == startAt) {
                log.info("Found document with identifier ${startAt}. Starting to index ...")
                    indexing = true
            }
            if (indexing) {
                log.trace("Adding doc ${doc.identifier} with type ${doc.contentType}")
                    if (fromStorage) {
                        log.trace("Rebuilding storage from $fromStorage")
                        try {
                            docs << whelk.addToStorage(doc, fromStorage)
                        } catch (WhelkAddException wae) {
                            log.trace("Expected exception ${wae.message}")
                        }
                    } else {
                        docs << doc
                    }
                if (++count % 1000 == 0) { // Bulk index 1000 docs at a time
                    doTheIndexing(futures, docs)
                    docs = []
                    runningTime = System.currentTimeMillis() - startTime
                }
            }
            if (cancelled) {
                break
            }
        }
        log.debug("Went through all documents. Processing remainder.")
        if (docs.size() > 0) {
            log.trace("Reindexing remaining ${docs.size()} documents")
            try {
                whelk.addToGraphStore(docs, selectedComponents)
            } catch (WhelkAddException wae) {
                errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed adding identifiers to graphstore: ${wae.failedIdentifiers as String}")
            }
            try {
                whelk.addToIndex(docs, selectedComponents)
            } catch (WhelkAddException wae) {
                errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed adding identifiers to graphstore: ${wae.failedIdentifiers as String}")
            }
        }
        log.info("Reindexed $count documents in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds." as String)
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
        operatorState=OperatorState.FINISHING
        boolean cleanResult = true
        log.debug("Number of futures: ${futures.size()}")
        for (f in futures) {
            def b = f.get()
            log.debug("Collecting results from threads ... ($b)")
            cleanResult = cleanResult && f.get()
        }
        log.info("Reindexing completed cleanly: $cleanResult")
        queue.shutdown()
    }

    void doTheIndexing(List futures, final List docs) {
        futures << queue.submit({
            try {
                whelk.addToGraphStore(docs, selectedComponents)
            } catch (WhelkAddException wae) {
                errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed adding identifiers to graphstore: ${wae.failedIdentifiers}")
                return false
            }
            try {
                whelk.addToIndex(docs, selectedComponents)
            } catch (WhelkAddException wae) {
                errorMessages << new String(wae.message + " (" + wae.failedIdentifiers + ")")
                log.warn("Failed indexing identifiers: ${wae.failedIdentifiers}")
                return false
            } catch (PluginConfigurationException pce) {
                log.error("System badly configured", pce)
                throw pce
            }
            return true
        } as Callable)
    }

    @Override
    Map getStatus() {
        def map = super.getStatus()
        if (hasRun) {
            if (fromStorage) {
                map.get("lastrun").put("fromStorage", fromStorage)
            }
            if (selectedComponents) {
                map.get("lastrun").put("selectedComponents", selectedComponents)
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
class BenchmarkOperator extends AbstractOperator {

    String oid = "benchmark"
    Date since = null

    @Override
    void setParameters(Map parameters) {
        super.setParameters(parameters)
        if (parameters.get("since", null)) {
            this.since = Date.parse("yyyy-MM-dd'T'hh:mm", parameters.get("since"))
            log.info("Since: $since")
        }
    }

    @Override
    void doRun(long startTime) {
        def docs = []
        for (doc in whelk.loadAll(dataset, since)) {
            docs << doc
            if (++count % 1000 == 0) {
                // Update runningtime every 1000 docs
                runningTime = System.currentTimeMillis() - startTime

                log.debug("Retrieved ${docs.size()} documents from $whelk ... ($count total). Time elapsed: ${runningTime/1000}. Current velocity: ${count/(runningTime/1000)} documents / second.")
                docs = []
            }
            if (cancelled) {
                break
            }
        }
        runningTime = System.currentTimeMillis() - startTime
        log.debug("$count documents read. Total time elapsed: ${runningTime/1000} seconds.")
    }
}

abstract class AbstractOperator implements Runnable {
    abstract String getOid()
    String dataset
    int count = 0
    long runningTime = 0
    OperatorState operatorState = OperatorState.IDLE
    Whelk whelk
    boolean hasRun = false
    boolean cancelled = false
    List<String> errorMessages

    void setParameters(Map parameters) {
        this.dataset = parameters.get("dataset", null)
    }

    @Override
    void run() {
        assert whelk
        try {
            log.debug("Starting reindex operation")
            operatorState=OperatorState.RUNNING
            cancelled = false
            count = 0
            runningTime = 0
            errorMessages = []
            doRun(System.currentTimeMillis())
        } finally {
            operatorState=OperatorState.IDLE
            hasRun = true
        }
    }

    abstract void doRun(long startTime);

    Map getStatus() {
        double rt = (runningTime > 0 ? runningTime/1000 : 0.0)
        float velocity = 0.0
        if (rt > 0) {
            velocity = count/rt
        }
        if (operatorState == OperatorState.IDLE) {
            def map = ["state":operatorState]
            if (hasRun) {
                map.put("lastrun", ["dataset":dataset,"velocity":(velocity > 0 ? velocity : "unlimited"),"count":count,"runningTime":rt])
                if (errorMessages) {
                    map.get("lastrun").put("errors",errorMessages)
                }
                if (cancelled) {
                    map.get("lastrun").put("cancelled", true)
                }
            }
            return map
        } else {
            return ["state":operatorState,"dataset":dataset,"velocity":velocity,"count":count,"runningTime":rt, "errors":errorMessages]
        }
    }

    void cancel() {
        this.cancelled = true
    }
}


enum OperatorState {
    IDLE, RUNNING, FINISHING
}
