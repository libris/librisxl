package whelk.datatool

import com.google.common.util.concurrent.MoreExecutors
import org.apache.logging.log4j.Logger
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl
import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd
import whelk.JsonLdValidator
import whelk.Whelk
import whelk.datatool.form.MatchForm
import whelk.datatool.util.IdLoader
import whelk.exception.StaleUpdateException
import whelk.exception.WhelkException
import whelk.meta.WhelkConstants
import whelk.search.ESQuery
import whelk.search.ElasticFind
import whelk.util.DocumentUtil
import whelk.util.LegacyIntegrationTools
import whelk.util.Statistics

import javax.script.Bindings
import javax.script.CompiledScript
import javax.script.ScriptEngineManager
import javax.script.SimpleBindings
import java.time.ZonedDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import static java.util.concurrent.TimeUnit.SECONDS
import static whelk.util.Jackson.mapper

class WhelkTool {
    static final int DEFAULT_BATCH_SIZE = 500
    public static final int DEFAULT_FETCH_SIZE = 100
    static final int DEFAULT_STATS_NUM_IDS = 3
    public static final String MAIN_LOG_NAME = "MAIN.txt"
    public static final String ERROR_LOG_NAME = "ERRORS.txt"
    public static final String FAILED_LOG_NAME = "FAILED.txt"
    public static final String MODIFIED_LOG_NAME = "MODIFIED.txt"
    public static final String CREATED_LOG_NAME = "CREATED.txt"
    public static final String DELETED_LOG_NAME = "DELETED.txt"

    Whelk whelk
    IdLoader idLoader

    private Script script
    private Bindings bindings

    private boolean hasStoredScriptJob

    String changedIn = "xl"
    String defaultChangedBy

    File reportsDir
    PrintWriter mainLog
    PrintWriter errorLog

    PrintWriter failedLog
    PrintWriter modifiedLog
    PrintWriter createdLog
    PrintWriter deletedLog
    ConcurrentHashMap<String, PrintWriter> reports = new ConcurrentHashMap<>()
    Closure finalizeLogs
    Logger logger

    Counter counter = new Counter()

    boolean skipIndex
    boolean dryRun
    boolean noThreads = true
    int numThreads = -1
    boolean stepWise
    volatile int limit = -1

    boolean recordChanges
    int recordingLimit = -1

    private String chosenAnswer = 'y'

    boolean allowLoud
    boolean allowIdRemoval

    enum ValidationMode {
        ON,
        OFF,
        SKIP_AND_LOG
    }

    ValidationMode jsonLdValidation = ValidationMode.ON
    ValidationMode inDatasetValidation = ValidationMode.ON

    Throwable errorDetected

    private def jsonWriter = mapper.writerWithDefaultPrettyPrinter()

    ElasticFind elasticFind
    Statistics statistics
    JsonLdValidator validator
    volatile boolean finished

    private Queue<RecordedChange> recordedChanges = new ConcurrentLinkedQueue<>()

    private ScheduledExecutorService timedLogger = MoreExecutors.getExitingScheduledExecutorService(
            Executors.newScheduledThreadPool(1))

    WhelkTool(Whelk whelk, Script script, File reportsDir = null, int statsNumIds) {
        if (whelk == null) {
            try {
                whelk = Whelk.createLoadedSearchWhelk()
            } catch (NullPointerException e) {
                whelk = Whelk.createLoadedCoreWhelk()
            }
        }
        this.whelk = whelk
        this.idLoader = new IdLoader(whelk.storage)
        this.validator = JsonLdValidator.from(whelk.jsonld)
        this.script = script
        this.defaultChangedBy = script.scriptJobUri
        this.reportsDir = reportsDir
        reportsDir.mkdirs()
        mainLog = new PrintWriter(new File(reportsDir, MAIN_LOG_NAME))
        def errorLogFile = new File(reportsDir, ERROR_LOG_NAME)
        errorLog = new PrintWriter(errorLogFile)
        def failedLogFile = new File(reportsDir, FAILED_LOG_NAME)
        failedLog = new PrintWriter(failedLogFile)
        def modifiedLogFile = new File(reportsDir, MODIFIED_LOG_NAME)
        modifiedLog = new PrintWriter(modifiedLogFile)
        def createdLogFile = new File(reportsDir, CREATED_LOG_NAME)
        createdLog = new PrintWriter(createdLogFile)
        def deletedLogFile = new File(reportsDir, DELETED_LOG_NAME)
        deletedLog = new PrintWriter(deletedLogFile)

        try {
            elasticFind = new ElasticFind(new ESQuery(whelk))
        }
        catch (Exception e) {
            log "Could not initialize elasticsearch: " + e
        }
        statistics = new Statistics(statsNumIds)

        var finishLogs = {
            if (!statistics.isEmpty()) {
                new PrintWriter(new File(reportsDir, "STATISTICS.txt")).withCloseable {
                    statistics.print(0, it)
                }
            }

            [modifiedLogFile, createdLogFile, deletedLogFile, errorLogFile, failedLogFile].each { if (it.length() == 0) it.delete() }
        }

        var lock = new Object()
        var isFinalized = new AtomicBoolean()
        finalizeLogs = {
            if (!isFinalized.get()) {
                synchronized (lock) {
                    if (!isFinalized.get()) {
                        isFinalized.set(true)
                        finishLogs()
                    }
                }
            }
        }
    }

    boolean isFinished() {
        return finished
    }

    boolean getUseThreads() { !noThreads && !stepWise }

    String findCanonicalId(String id) {
        return whelk.storage.getMainId(id)
    }

    Map load(String id) {
        return whelk.storage.loadDocumentByMainId(findCanonicalId(id))?.data
    }

    void selectByIds(Collection<String> ids, Closure process,
                     int batchSize = DEFAULT_BATCH_SIZE, boolean silent = false) {
        if (!silent) {
            log "Select by ${ids.size()} IDs"
        }
        def idItems = idLoader.collectXlShortIds(ids)
        if (idItems.isEmpty()) {
            return
        }
        doSelectBySqlWhere("id = ANY(?) AND deleted = false", process, batchSize, [1: idItems])
    }

    // Use this for Voyager ids (which are unique only within respective marc collection)
    void selectByIdsAndCollection(Collection<String> ids, String collection, Closure process,
                                  int batchSize = DEFAULT_BATCH_SIZE, boolean silent = false) {
        if (!silent) {
            log "Select by ${ids.size()} IDs in collection $collection"
        }

        def idItems = idLoader.collectXlShortIds(ids, collection)
        if (idItems.isEmpty()) {
            return
        }

        doSelectBySqlWhere("id = ANY(?) AND collection = ? AND deleted = false", process,
                batchSize, [1: idItems, 2: collection])
    }

    void selectByForm(MatchForm matchForm, Closure process,
                      int batchSize = DEFAULT_BATCH_SIZE, boolean silent = false) {
        if (!silent) {
            log "Select by form"
        }

        var ids = matchForm.getIdSelection()
        if (ids.isEmpty()) {
            var pathToIds = matchForm.getIdListsForPaths()
            ids = pathToIds.isEmpty()
                    ? whelk.sparqlQueryClient.queryIdsByPattern(matchForm.getSparqlPattern(whelk.jsonld.context))
                    : idLoader.loadIdsByLinksAtPaths(pathToIds)
        }

        selectByIds(ids, process, batchSize, silent)
    }

    DocumentItem create(Map data) {
        Document doc = new Document(data)
        doc.deepReplaceId(Document.BASE_URI.toString() + IdGenerator.generate())
        DocumentItem item = new DocumentItem(
                number: counter.createdCount,
                doc: doc,
                whelk: whelk,
                logFailed: this.&logFailed
        )
        item.existsInStorage = false
        return item
    }

    void selectBySqlWhere(Map params, String whereClause, Closure process) {
        selectBySqlWhere(whereClause,
                params.batchSize ?: DEFAULT_BATCH_SIZE, params.silent,
                process)
    }

    Iterable<String> queryIds(Map<String, List<String>> parameters) {
        if (!elasticFind)
            throw new IllegalStateException("no connection to elasticsearch")

        return elasticFind.findIds(parameters)
    }

    Iterable<Map> queryDocs(Map<String, List<String>> parameters) {
        if (!elasticFind)
            throw new IllegalStateException("no connection to elasticsearch")

        return elasticFind.find(parameters)
    }

    void selectBySqlWhere(String whereClause,
                          int batchSize = DEFAULT_BATCH_SIZE, boolean silent = false,
                          Closure process) {
        if (!silent)
            log "Select by SQL"
        doSelectBySqlWhere(whereClause, process, batchSize)
    }

    private void doSelectBySqlWhere(String whereClause, Closure process,
                                    int batchSize = DEFAULT_BATCH_SIZE, Map<Integer, Object> params = [:]) {
        def query = """
            SELECT id, data, created, modified, deleted
            FROM lddb
            WHERE $whereClause
            """

        def conn = whelk.storage.getOuterConnection()
        def stmt
        def rs
        try {
            conn.setAutoCommit(false)
            stmt = conn.prepareStatement(query)
            params.each { i, obj ->
                if (obj instanceof Collection) {
                    stmt.setArray(i, conn.createArrayOf("TEXT", obj as String[]))
                } else if (obj instanceof String) {
                    stmt.setString(i, obj)
                }
            }
            stmt.setFetchSize(DEFAULT_FETCH_SIZE)
            rs = stmt.executeQuery()
            select(whelk.storage.iterateDocuments(rs), process, batchSize)
        } finally {
            rs?.close()
            stmt?.close()
            conn?.close()
        }
    }

    void selectByCollection(String collection, Closure process,
                            int batchSize = DEFAULT_BATCH_SIZE, boolean silent = false) {
        if (!silent)
            log "Select by collection: ${collection}"
        select(whelk.storage.loadAll(collection), process, batchSize)
    }

    void selectFromIterable(Iterable<DocumentItem> docs, Closure process,
                            int batchSize = DEFAULT_BATCH_SIZE, boolean silent = false) {
        if (!silent)
            log "Processing from in-memory collection: ${docs.size()} items."

        boolean newItems = true
        select(docs.collect { it -> it.doc }, process, batchSize, newItems)
    }

    private void select(Iterable<Document> selection, Closure process,
                        int batchSize = DEFAULT_BATCH_SIZE, boolean newItems = false) throws Exception {
        int batchCount = 0
        Batch batch = new Batch(number: ++batchCount)

        def executorService = useThreads && !isWorkerThread() ? createExecutorService() : null
        def catchUnexpected = { Runnable r -> internalExceptionHandler(r, executorService) }

        def loggerFuture = !stepWise ? setupTimedLogger(counter) : null

        for (Document doc : selection) {
            if (doc.deleted) {
                continue
            }
            counter.countRead()
            if (limit > -1 && counter.readCount > limit) {
                break
            }
            DocumentItem item = new DocumentItem(
                    number: counter.readCount,
                    doc: doc,
                    whelk: whelk,
                    preUpdateChecksum: doc.getChecksum(whelk.jsonld),
                    logFailed: this.&logFailed
            )
            item.existsInStorage = !newItems
            batch.items << item
            if (batch.items.size() == batchSize) {
                def batchToProcess = batch
                if (executorService) {
                    executorService.submit catchUnexpected {
                        if (!processBatch(process, batchToProcess, counter)) {
                            executorService.shutdownNow()
                        }
                    }
                } else {
                    if (!processBatch(process, batchToProcess, counter)) {
                        log "Aborted selection: ${counter.summary}. Done in ${counter.elapsedSeconds} s."
                        return
                    }
                }
                batch = new Batch(number: ++batchCount)
            }
        }

        if (executorService) {
            executorService.submit catchUnexpected {
                processBatch(process, batch, counter)
            }
            executorService.shutdown()
        } else {
            processBatch(process, batch, counter)
        }

        if (!executorService || executorService.awaitTermination(8, TimeUnit.HOURS)) {
            log "Processed selection: ${counter.summary}. Done in ${counter.elapsedSeconds} s."
            log()
        }
        loggerFuture?.cancel(true)

        if (errorDetected) {
            log "Error detected, refusing further processing."
            throw new Exception()
        }
    }

    private def createExecutorService() {
        int poolSize = numThreads > 1 ? numThreads : defaultNumThreads()
        def linkedBlockingDeque = new LinkedBlockingDeque<Runnable>((int) (poolSize * 1.5))

        def executorService = new ThreadPoolExecutor(poolSize, poolSize,
                1, TimeUnit.DAYS,
                linkedBlockingDeque, new ThreadPoolExecutor.CallerRunsPolicy())

        executorService.setThreadFactory(new ThreadFactory() {
            ThreadGroup group = new ThreadGroup(WhelkConstants.BATCH_THREAD_GROUP)

            @Override
            Thread newThread(Runnable runnable) {
                return new Thread(group, runnable)
            }
        })

        return executorService
    }

    private Runnable internalExceptionHandler(Runnable r, ExecutorService executorService) {
        return {
            try {
                r.run()
            } catch (Exception e) {
                String msg = "*** THIS IS A BUG IN WHELKTOOL ***"
                log msg
                log "Error: $e"
                executorService.shutdownNow()
                errorLog.println msg
                errorLog.println "Error: "
                e.printStackTrace errorLog
                errorLog.println "-" * 20
                errorLog.flush()
                errorDetected = e
            }
        }
    }

    private static int defaultNumThreads() {
        Runtime.getRuntime().availableProcessors() * 4
    }

    private boolean isWorkerThread() {
        return Thread.currentThread().getThreadGroup().getName().contains(WhelkConstants.BATCH_THREAD_GROUP)
    }

    private ScheduledFuture<?> setupTimedLogger(Counter counter) {
        timedLogger.scheduleAtFixedRate({
            if (counter.timeSinceLastSummarySeconds() > 4) {
                repeat "$counter.summary"
            }
        }, 5, 5, SECONDS)
    }

    /**
     * @return true to continue, false to break.
     */
    private boolean processBatch(Closure process, Batch batch, def counter) {
        boolean doContinue = true
        for (DocumentItem item : batch.items) {
            var globals = new HashSet(bindings.keySet())

            if (!useThreads) {
                repeat "Processing $item.number: ${item.doc.id} ($counter.summary)"
            }

            try {
                doContinue = doProcess(process, item, counter)
            } catch (Throwable err) {
                log "Error occurred when processing <$item.doc.completeId>: $err"
                errorLog.println "Stopped at document <$item.doc.completeId>"
                errorLog.println "Process status: $counter.summary"
                errorLog.println "Error:"
                err.printStackTrace errorLog
                errorLog.println "-" * 20
                errorLog.flush()
                errorDetected = err
                return false
            }

            var newGlobals = bindings.keySet() - globals
            if (newGlobals) {
                String msg = "FORBIDDEN - new bindings detected: ${newGlobals}"
                log(msg)
                log("Adding new global bindings during record processing is forbidden (since they share state across threads).")
                log("Aborting.")
                errorDetected = new Exception(msg)
                return false
            }

            if (!doContinue) {
                break
            }
        }
        if (!useThreads) log()
        log "Processed batch $batch.number ($counter.summary)"
        return doContinue
    }

    /**
     * @return true to continue, false to break.
     */
    private boolean doProcess(Closure process, DocumentItem item, def counter) {
        String inJsonStr = stepWise
                ? jsonWriter.writeValueAsString(item.doc.data)
                : null
        counter.countProcessed()
        statistics.withContext(item.doc.shortId) {
            process(item)
        }
        if (item.needsSaving) {
            if (item.loud) {
                assert allowLoud: "Loud changes need to be explicitly allowed"
            }
            if (item.restoreToTime != null) {
                if (!doRevertToTime(item))
                    return true
            }
            if (stepWise && !confirmNextStep(inJsonStr, item.doc)) {
                return false
            }
            try {
                if (item.doDelete) {
                    doDeletion(item)
                    counter.countDeleted()
                } else if (item.existsInStorage) {
                    try {
                        doModification(item)
                    }
                    catch (StaleUpdateException e) {
                        logRetry(e, item)
                        Document doc = whelk.getDocument(item.doc.shortId)
                        item = new DocumentItem(
                                number: item.number,
                                doc: doc,
                                whelk: whelk,
                                preUpdateChecksum: doc.getChecksum(whelk.jsonld),
                                existsInStorage: true,
                                logFailed: this.&logFailed
                        )
                        return doProcess(process, item, counter)
                    }
                    counter.countModified()
                } else {
                    assert allowLoud: "To save *new* records, loud changes need to be explicitly allowed. These cannot be silent - there is no old modified-time to preserve."
                    doSaveNew(item)
                    counter.countNewSaved()
                }
            } catch (Exception err) {
                if (item.onError) {
                    item.onError(err)
                } else {
                    throw err
                }
            }
            storeScriptJob()
        }
        return true
    }

    private void logRetry(StaleUpdateException e, DocumentItem item) {
        def msg = "Re-processing ${item.doc.shortId} because of concurrent modification. This is not a problem if script is correct (pure function of document) but might affect logs and statistics. $e"
        errorLog.println(msg)
        mainLog.println(msg)
    }

    private void doDeletion(DocumentItem item) {
        if (recordChanges) {
            recordChange(whelk.getDocument(item.doc.shortId), null, item.number)
        }
        if (!validateInDataset(item.doc)) {
            return
        }
        if (!dryRun) {
            whelk.remove(item.doc.shortId, changedIn, item.changedBy ?: defaultChangedBy)
        }
        deletedLog.println(item.doc.shortId)
    }

    private boolean doRevertToTime(DocumentItem item) {

        // The 'versions' list is sorted, with the oldest version first.
        List<Document> versions = item.getVersions()

        ZonedDateTime restoreTime = ZonedDateTime.parse(item.restoreToTime)

        // If restoreTime is older than any stored version (we can't go back that far)
        ZonedDateTime oldestStoredTime = getLatestModification(versions.get(0))
        if (restoreTime.isBefore(oldestStoredTime)) {
            errorLog.println("Cannot restore ${item.doc.shortId} to ${restoreTime}, oldest stored version from: ${oldestStoredTime}")
            return false
        }

        // Go over the versions, oldest first,
        // until you've found the oldest version that is younger than the desired time target.
        Document selectedVersion = null
        for (Document version : versions) {
            ZonedDateTime latestModificationTime = getLatestModification(version)
            if (restoreTime.isAfter(latestModificationTime))
                selectedVersion = version
        }

        if (selectedVersion != null) {
            item.doc.data = selectedVersion.data
            return true
        }
        return false
    }

    private ZonedDateTime getLatestModification(Document version) {
        ZonedDateTime modified = ZonedDateTime.parse(version.getModified())
        if (version.getGenerationDate() != null) {
            ZonedDateTime generation = ZonedDateTime.parse(version.getGenerationDate())
            if (generation.isAfter(modified))
                return generation
        }
        return modified
    }

    private void doModification(DocumentItem item) {
        Document doc = item.doc
        doc.setGenerationDate(new Date())
        doc.setGenerationProcess(item.generationProcess ?: script.scriptJobUri)

        if (recordChanges) {
            recordChange(whelk.getDocument(doc.shortId), doc.clone(), item.number)
        }

        if (!validateJsonLd(doc) || !validateInDataset(doc)) {
            return
        }

        if (!dryRun) {
            whelk.storeAtomicUpdate(doc, !item.loud, true, changedIn, item.changedBy ?: defaultChangedBy, item.preUpdateChecksum)
        }

        modifiedLog.println(doc.shortId)
    }

    private void doSaveNew(DocumentItem item) {
        Document doc = item.doc
        doc.setControlNumber(doc.getShortId())
        doc.setGenerationDate(new Date())
        doc.setGenerationProcess(item.generationProcess ?: script.scriptJobUri)

        if (recordChanges) {
            recordChange(null, doc.clone(), item.number)
        }

        if (!validateJsonLd(doc) || !validateInDataset(doc)) {
            return
        }

        if (!dryRun) {
            var collection = LegacyIntegrationTools.determineLegacyCollection(doc, whelk.getJsonld())
            if (!whelk.createDocument(doc, changedIn, item.changedBy ?: defaultChangedBy, collection, false))
                throw new WhelkException("Failed to save a new document. See general whelk log for details.")
        }
        createdLog.println(doc.shortId)
    }

    private boolean validateJsonLd(Document doc) {
        if (jsonLdValidation == ValidationMode.OFF) {
            return true
        }
        List<JsonLdValidator.Error> errors = validator.validate(doc.data, doc.getLegacyCollection(whelk.jsonld))
        if (errors) {
            String msg = "Invalid JSON-LD in document ${doc.completeId}. Errors: ${errors.collect { it.toMap() }}"
            if (jsonLdValidation == ValidationMode.ON) {
                throw new Exception(msg)
            } else if (jsonLdValidation == ValidationMode.SKIP_AND_LOG) {
                logFailed(doc, msg)
                return false
            }
        }
        return true
    }

    private boolean validateInDataset(Document doc) {
        if (inDatasetValidation == ValidationMode.OFF) {
            return true
        }
        if (doc.isInReadOnlyDataset()) {
            var msg = "Cannot write document belonging to read-only dataset: ${doc.completeId}"
            if (inDatasetValidation == ValidationMode.ON) {
                throw new Exception(msg)
            } else if (inDatasetValidation == ValidationMode.SKIP_AND_LOG) {
                logFailed(doc, msg)
                return false
            }
        }
        return true
    }

    void logFailed(Document doc, String msg) {
        failedLog.println(doc.shortId)
        errorLog.println(msg)
    }

    private boolean confirmNextStep(String inJsonStr, Document doc) {
        new File(reportsDir, "IN.jsonld").setText(inJsonStr, 'UTF-8')
        new File(reportsDir, "OUT.jsonld").withWriter {
            jsonWriter.writeValue(it, doc.data)
        }

        def choice = { chosenAnswer == it ? it.toUpperCase() : it }
        def y = choice('y')
        def p = choice('p')
        def n = choice('n')

        println()
        print "Continue [ $y(es) / $n(o) / $p(rint) ]? "

        chosenAnswer = System.in.newReader().readLine()?.toLowerCase() ?: chosenAnswer

        if (chosenAnswer == 'p') {
            println jsonWriter.writeValueAsString(doc.data)
        }

        return chosenAnswer != 'n'
    }

    private synchronized void storeScriptJob() {
        if (hasStoredScriptJob) {
            return
        }
        // TODO: store description about script job
        // entity[ID] = scriptJobUri
        // entity[TYPE] = 'ScriptJob'
        // entity.created = storage.formatDate(...)
        // entity.modified = storage.formatDate(...)
        hasStoredScriptJob = true
    }

    boolean isInstanceOf(Map entity, String baseType) {
        def type = entity['@type']
        if (type == null)
            return false
        def types = type instanceof String ? [type] : type
        return types.any { whelk.jsonld.isSubClassOf(it, baseType) }
    }

    private Bindings createDefaultBindings() {
        Bindings bindings = new SimpleBindings()
        ['graph', 'id', 'type'].each {
            bindings.put(it.toUpperCase(), "@$it" as String)
        }
        bindings.put("isInstanceOf", this.&isInstanceOf)
        bindings.put("findCanonicalId", this.&findCanonicalId)
        bindings.put("load", this.&load)
        return bindings
    }

    Bindings createMainBindings() {
        // Update Whelktool.gdsl when adding new bindings
        Bindings bindings = createDefaultBindings()
        bindings.put("scriptDir", script.scriptDir)
        bindings.put("baseUri", Document.BASE_URI)
        bindings.put("getReportWriter", this.&getReportWriter)
        bindings.put("reportsDir", reportsDir)
        bindings.put("parameters", script.getParameters())
        bindings.put("script", { String s -> script.compileSubScript(this, s) })
        bindings.put("selectByCollection", this.&selectByCollection)
        bindings.put("selectByIds", this.&selectByIds)
        bindings.put("selectByIdsAndCollection", this.&selectByIdsAndCollection)
        bindings.put("selectBySqlWhere", this.&selectBySqlWhere)
        bindings.put("selectByForm", this.&selectByForm)
        bindings.put("selectFromIterable", this.&selectFromIterable)
        bindings.put("create", this.&create)
        bindings.put("queryIds", this.&queryIds)
        bindings.put("queryDocs", this.&queryDocs)
        bindings.put("incrementStats", statistics.&increment)
        bindings.put("asList", JsonLd::asList)
        bindings.put("getAtPath", DocumentUtil::getAtPath)
        bindings.put("getWhelk", this.&getWhelk)
        bindings.put("isLoudAllowed", this.allowLoud)
        return bindings
    }

    public void run() {
        whelk.setSkipIndex(skipIndex)
        if (allowIdRemoval) {
            whelk.storage.doVerifyDocumentIdRetention = false
        }

        log "Running Whelk against:"
        log "  PostgreSQL:"
        log "    url:     ${whelk.storage.connectionPool.getJdbcUrl()}"
        if (whelk.elastic) {
            log "  ElasticSearch:"
            log "    hosts:   ${whelk.elastic.elasticHosts}"
            log "    index:   ${whelk.elastic.defaultIndex}"
        }
        log "Using script: $script"
        log "Using report dir: $reportsDir"
        if (skipIndex) log "  skipIndex"
        if (dryRun) log "  dryRun"
        if (stepWise) log "  stepWise"
        if (noThreads) log "  noThreads"
        if (limit > -1) log "  limit: $limit"
        if (allowLoud) log "  allowLoud"
        if (allowIdRemoval) log "  allowIdRemoval"
        log "  JSON-LD validation: ${jsonLdValidation.name()}"
        log "  Dataset validation: ${inDatasetValidation.name()}"
        log()

        bindings = createMainBindings()

        try {
            script.compiledScript.eval(bindings)
        } finally {
            finish()
        }
    }

    private void finish() {
        def logWriters = [mainLog, errorLog, modifiedLog, createdLog, deletedLog, failedLog] + reports.values()
        logWriters.each {
            it.flush()
            it.close()
        }
        finalizeLogs()
        
        if (errorDetected) {
            log "Script terminated due to an error, see $reportsDir/ERRORS.txt for more info"
            throw new RuntimeException("Script terminated due to an error", errorDetected)
        }
        finished = true
        log "Done!"
    }

    private PrintWriter getReportWriter(String reportName) {
        reports.computeIfAbsent(reportName, { new PrintWriter(new File(reportsDir, it)) })
    }

    private void log() {
        mainLog.println()
        System.err.println()
    }

    private void log(String msg) {
        if (logger) {
            logger.info(msg)
        } else {
            System.err.println(msg)
        }

        mainLog.println(msg)
    }

    private void repeat(String msg) {
        mainLog.println(msg)
        System.err.print "\r$msg"
    }

    static void main(String[] args) {
        main2(args, null)
    }

    static void main2(String[] args, Whelk preExistingWhelk) {
        def cli = new CliBuilder(usage: 'whelktool [options] <SCRIPT>')
        cli.h(longOpt: 'help', 'Print this help message and exit.')
        cli.r(longOpt: 'report', args: 1, argName: 'REPORT-DIR', 'Directory where reports are written (defaults to "reports").')
        cli.I(longOpt: 'skip-index', 'Do not index any changes, only write to storage.')
        cli.d(longOpt: 'dry-run', 'Do not save any modifications.')
        cli.T(longOpt: 'no-threads', 'Do not use threads to parallellize batch processing.')
        cli.t(longOpt: 'num-threads', args: 1, argName: 'N', "Override default number of threads (${defaultNumThreads()}).")
        cli.s(longOpt: 'step', 'Change one document at a time, prompting to continue.')
        cli.l(longOpt: 'limit', args: 1, argName: 'LIMIT', 'Amount of documents to process.')
        cli.a(longOpt: 'allow-loud', 'Allow scripts to do loud modifications.')
        cli.idchg(longOpt: 'allow-id-removal', '[UNSAFE] Allow script to remove document ids, e.g. sameAs.')
        cli.v(longOpt: 'validation', args: 1, argName: 'MODE', '[UNSAFE] Set JSON-LD validation mode. Defaults to ON.' +
                ' Possible values: ON/OFF/SKIP_AND_LOG')
        cli.dv(longOpt: 'dataset-validation', args: 1, argName: 'MODE', '[UNSAFE] Set read-only dataset validation mode. Defaults to ON.' +
                ' Possible values: ON/OFF/SKIP_AND_LOG')
        cli.n(longOpt: 'stats-num-ids', args: 1, 'Number of ids to print per entry in STATISTICS.txt.')
        cli.p(longOpt: 'parameters', args: 1, argName: 'PARAMETER-FILE', 'Path to JSON file with parameters to script')

        def options = cli.parse(args)
        if (options.h) {
            cli.usage()
            System.exit 0
        }
        def reportsDir = new File(options.r ?: 'reports')
        def scriptPath = options.arguments()[0]

        int statsNumIds = options.n ? Integer.parseInt(options.n) : DEFAULT_STATS_NUM_IDS

        Script script = null
        if (!scriptPath) {
            cli.usage()
            System.exit(1)
        }

        try {
            script = new FileScript(scriptPath)

            if (options.p) {
                script.setParameters(mapper.readValue(new File(options.p).getText("UTF-8"), Map))
            }
        }
        catch (IOException e) {
            System.err.println("Could not load script [$scriptPath] : $e")
            System.exit(1)
        }

        def tool = new WhelkTool(preExistingWhelk, script, reportsDir, statsNumIds)
        tool.skipIndex = options.I
        tool.dryRun = options.d
        tool.stepWise = options.s
        tool.noThreads = options.T
        tool.allowLoud = options.a
        tool.allowIdRemoval = options.idchg
        try {
            if (options.t) {
                tool.numThreads = Integer.parseInt(options.t)
            }
            if (options.l) {
                tool.limit = Integer.parseInt(options.l)
            }
            if (options.v) {
                tool.jsonLdValidation = parseValidationMode(options.v)
            }
            if (options.dv) {
                tool.inDatasetValidation = parseValidationMode(options.dv)
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid argument(s) $e")
            cli.usage()
            System.exit(1)
        }

        try {
            Runtime.addShutdownHook {
                tool.finalizeLogs() // print stats even if process is killed from outside
            }
            tool.run()
        } catch (Exception e) {
            System.err.println(e.toString())
            System.exit(1)
        }
    }

    private static ValidationMode parseValidationMode(String arg) throws IllegalArgumentException {
        return ValidationMode.valueOf(arg.toUpperCase())
    }

    void recordChange(Document before, Document after, int number) {
        //if (recordingLimit >= 0 && number > recordingLimit) {
        if (recordingLimit >= 0 && recordedChanges.size() > recordingLimit) {
            recordedChanges.add(new RecordedChange(null, null, number))
        } else {
            recordedChanges.add(new RecordedChange(before, after, number))
        }
    }

    List<RecordedChange> getRecordedChanges() {
        //return recordedChanges.collect().toSorted()
        return recordedChanges.collect()
    }
}

class Script {
    GroovyScriptEngineImpl engine
    File scriptDir
    CompiledScript compiledScript
    String scriptJobUri

    protected Map<Object, Object> scriptParams = Collections.emptyMap()

    private Map<String, Closure> compiledScripts = [:]

    Script(String source, String scriptJobUri) {
        this(source, scriptJobUri, File.createTempDir())
    }

    Script(String source, String scriptJobUri, File scriptDir) {
        this.scriptDir = scriptDir
        this.scriptJobUri = scriptJobUri
        this.scriptDir = scriptDir

        ScriptEngineManager manager = new ScriptEngineManager()
        this.engine = (GroovyScriptEngineImpl) manager.getEngineByName("groovy")
        this.compiledScript = engine.compile(source)
    }

    void setParameters(Map<Object, Object> scriptParams) {
        this.scriptParams = Collections.unmodifiableMap(scriptParams)
    }

    Map<Object, Object> getParameters() {
        return scriptParams;
    }

    Closure compileSubScript(WhelkTool tool, String scriptPath) {
        if (!compiledScripts.containsKey(scriptPath)) {
            File scriptFile = new File(scriptDir, scriptPath)
            String scriptSource = scriptFile.getText("UTF-8")
            CompiledScript script = engine.compile(scriptSource)
            Bindings bindings = tool.createMainBindings()
            Closure process = null
            bindings.put("scriptDir", scriptDir)
            bindings.put("getReportWriter", tool.&getReportWriter)
            bindings.put("process", { process = it })
            script.eval(bindings)
            compiledScripts[scriptPath] = process
        }
        return compiledScripts[scriptPath]
    }

    @Override
    String toString() {
        return scriptJobUri
    }
}

class FileScript extends Script {
    String path

    FileScript(String scriptPath) throws IOException {
        super(new File(scriptPath).getText("UTF-8"), scriptJobUri(scriptPath), new File(scriptPath).parentFile)
        this.path = scriptPath
    }

    private static String scriptJobUri(String scriptPath) {
        var scriptFile = new File(scriptPath)

        def segment = '/scripts/'
        def path = scriptFile.toURI().toString()
        path = path.substring(path.lastIndexOf(segment) + segment.size())
        // FIXME: de-KBV/Libris-ify
        return "https://libris.kb.se/sys/globalchanges/${path}"
    }

    @Override
    String toString() {
        return path
    }
}

class DocumentItem {
    int number
    Document doc
    String preUpdateChecksum
    String changedBy
    String generationProcess
    private Whelk whelk
    private boolean needsSaving = false
    private boolean doDelete = false
    private boolean loud = false
    boolean existsInStorage = true
    private String restoreToTime = null
    Closure onError = null
    Closure logFailed = null

    List getGraph() {
        return doc.data['@graph']
    }

    void scheduleSave(Map params = [:]) {
        needsSaving = true
        set(params)
        assert (restoreToTime == null)
    }

    void scheduleDelete(Map params = [:]) {
        needsSaving = true
        doDelete = true
        set(params)
    }

    void scheduleRevertTo(Map params = [:]) {
        needsSaving = true
        set(params)
        assert (restoreToTime != null)
    }

    void reportFailed(String message) {
        logFailed(doc, message)
    }

    private void set(Map params) {
        if (params.containsKey('loud')) {
            this.loud = params.loud
        }
        if (params.containsKey('time')) {
            this.restoreToTime = params.time
        }
        if (params.containsKey('changedBy')) {
            this.changedBy = params.changedBy
        }
        if (params.containsKey('generationProcess')) {
            this.generationProcess = params.generationProcess
        }
        this.onError = params.onError
    }

    def getVersions() {
        whelk.storage.loadAllVersions(doc.shortId)
    }

    def getDependers() {
        whelk.storage.getDependers(doc.shortId)
    }

    Map asCard(boolean withSearchKey = false) {
        return whelk.jsonld.toCard(doc.data, false, withSearchKey)
    }
}


class Batch {
    int number
    List<DocumentItem> items = []
}


class Counter {
    long startTime = System.currentTimeMillis()
    long lastSummary = startTime
    AtomicInteger createdCount = new AtomicInteger()
    AtomicInteger newSavedCount = new AtomicInteger()
    AtomicInteger readCount = new AtomicInteger()
    AtomicInteger processedCount = new AtomicInteger()
    AtomicInteger modifiedCount = new AtomicInteger()
    AtomicInteger deleteCount = new AtomicInteger()

    synchronized int getSaved() { modifiedCount.get() + deleteCount.get() }

    String getSummary() {
        lastSummary = System.currentTimeMillis()
        double docsPerSec = processedCount.get() / getElapsedSeconds()
        "read: ${readCount.get()}, processed: ${processedCount.get()}, modified: ${modifiedCount.get()}, deleted: ${deleteCount.get()}, new saved: ${newSavedCount.get()} (at ${docsPerSec.round(2)} docs/s)"
    }

    double getElapsedSeconds() {
        return (System.currentTimeMillis() - startTime) / 1000
    }

    void countNewSaved() {
        newSavedCount.incrementAndGet()
    }

    void countRead() {
        readCount.incrementAndGet()
    }

    void countProcessed() {
        processedCount.incrementAndGet()
    }

    void countModified() {
        modifiedCount.incrementAndGet()
    }

    void countDeleted() {
        deleteCount.incrementAndGet()
    }

    double timeSinceLastSummarySeconds() {
        return (System.currentTimeMillis() - lastSummary) / 1000
    }
}