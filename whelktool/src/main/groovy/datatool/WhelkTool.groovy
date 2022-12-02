package whelk.datatool

import com.google.common.util.concurrent.MoreExecutors
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl
import whelk.Document
import whelk.IdGenerator
import whelk.JsonLd
import whelk.Whelk
import whelk.exception.StaleUpdateException
import whelk.exception.WhelkException
import whelk.search.ESQuery
import whelk.search.ElasticFind
import whelk.util.DocumentUtil
import whelk.util.LegacyIntegrationTools
import whelk.util.Statistics
import whelk.meta.WhelkConstants

import javax.script.Bindings
import javax.script.Compilable
import javax.script.CompiledScript
import javax.script.ScriptEngineManager
import javax.script.SimpleBindings
import java.sql.SQLException
import java.time.ZonedDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import static java.util.concurrent.TimeUnit.SECONDS
import static whelk.util.Jackson.mapper

class WhelkTool {
    static final int DEFAULT_BATCH_SIZE = 500
    static final int DEFAULT_FETCH_SIZE = 100

    Whelk whelk

    private GroovyScriptEngineImpl engine

    File scriptFile
    CompiledScript script
    String scriptJobUri
    private boolean hasStoredScriptJob

    String changedIn = "xl"

    File reportsDir
    PrintWriter mainLog
    PrintWriter errorLog
    PrintWriter modifiedLog
    PrintWriter createdLog
    PrintWriter deletedLog
    ConcurrentHashMap<String, PrintWriter> reports = new ConcurrentHashMap<>()

    Counter counter = new Counter()

    boolean skipIndex
    boolean dryRun
    boolean noThreads = true
    boolean stepWise
    int limit = -1

    private String chosenAnswer = 'y'

    boolean allowLoud

    private Throwable errorDetected

    private def jsonWriter = mapper.writerWithDefaultPrettyPrinter()

    Map<String, Closure> compiledScripts = [:]

    ElasticFind elasticFind
    Statistics statistics

    private ScheduledExecutorService timedLogger = MoreExecutors.getExitingScheduledExecutorService(
            Executors.newScheduledThreadPool(1))

    WhelkTool(String scriptPath, File reportsDir=null, int statsNumIds) {
        try {
            whelk = Whelk.createLoadedSearchWhelk()
        } catch (NullPointerException e) {
            whelk = Whelk.createLoadedCoreWhelk()
        }
        initScript(scriptPath)
        this.reportsDir = reportsDir
        reportsDir.mkdirs()
        mainLog = new PrintWriter(new File(reportsDir, "MAIN.txt"))
        errorLog = new PrintWriter(new File(reportsDir, "ERRORS.txt"))

        def modifiedLogFile = new File(reportsDir, "MODIFIED.txt")
        modifiedLog = new PrintWriter(modifiedLogFile)
        def createdLogFile = new File(reportsDir, "CREATED.txt")
        createdLog = new PrintWriter(createdLogFile)
        def deletedLogFile = new File(reportsDir, "DELETED.txt")
        deletedLog = new PrintWriter(deletedLogFile)

        try {
            elasticFind = new ElasticFind(new ESQuery(whelk))
        }
        catch (Exception e) {
            log "Could not initialize elasticsearch: " + e
        }
        statistics = new Statistics(statsNumIds)
        Runtime.addShutdownHook {
            if (!statistics.isEmpty()) {
                new PrintWriter(new File(reportsDir, "STATISTICS.txt")).withCloseable {
                    statistics.print(0, it)
                }
            }

            [modifiedLogFile, createdLogFile, deletedLogFile].each { if (it.length() == 0) it.delete() }
        }
    }

    private void initScript(String scriptPath) {
        ScriptEngineManager manager = new ScriptEngineManager()
        engine = (GroovyScriptEngineImpl) manager.getEngineByName("groovy")
        scriptFile = new File(scriptPath)
        String scriptSource = null
        try {
            scriptSource = scriptFile.getText("UTF-8")
        }
        catch (IOException e) {
            System.err.println("Could not load script [$scriptPath] : $e")
            System.exit(1)
        }
        script = ((Compilable) engine).compile(scriptSource)
        def segment = '/scripts/'
        def path = scriptFile.toURI().toString()
        path = path.substring(path.lastIndexOf(segment) + segment.size())
        // FIXME: de-KBV/Libris-ify
        scriptJobUri = "https://libris.kb.se/sys/globalchanges/${path}"
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
        def uriIdMap = findShortIdsForUris(ids.findAll { it.contains(':') })
        def shortIds = ids.findResults { it.contains(':') ? uriIdMap[it] : it }

        def idItems = shortIds.collect { "'$it'" }.join(',\n')
        if (idItems.isEmpty()) {
            return
        }
        doSelectBySqlWhere("id IN ($idItems) AND deleted = false", process,
                batchSize)
    }

    DocumentItem create(Map data) {
        Document doc = new Document(data)
        doc.deepReplaceId(Document.BASE_URI.toString() + IdGenerator.generate())
        DocumentItem item = new DocumentItem(number: counter.createdCount, doc: doc, whelk: whelk)
        item.existsInStorage = false
        return item
    }

    Map<String, String> findShortIdsForUris(Collection uris) {
        def uriIdMap = [:]
        if (!uris) {
            return uriIdMap
        }
        def uriItems = uris.collect { "'$it'" }.join(',\n')
        def query = """
            SELECT id, iri
            FROM lddb__identifiers
            WHERE iri IN ($uriItems)
            """
        whelk.storage.withDbConnection {
            def conn = whelk.storage.getMyConnection()
            def stmt
            def rs
            try {
                stmt = conn.prepareStatement(query)
                rs = stmt.executeQuery()
                while (rs.next()) {
                    uriIdMap[rs.getString("iri")] = rs.getString("id")
                }
            } finally {
                try { rs?.close() } catch (SQLException e) {}
                try { stmt?.close() } catch (SQLException e) {}
            }
        }
        return uriIdMap
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
            int batchSize = DEFAULT_BATCH_SIZE) {
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
            int batchSize = DEFAULT_BATCH_SIZE, boolean newItems = false) {
        if (errorDetected) {
            log "Error detected, refusing further processing."
            return
        }

        int batchCount = 0
        Batch batch = new Batch(number: ++batchCount)

        def executorService = useThreads && !isWorkerThread() ? createExecutorService(batchSize) : null
        if (executorService) {
            Thread.setDefaultUncaughtExceptionHandler {
                Thread thread, Throwable err ->
                log "Uncaught error: $err"

                executorService.shutdownNow()

                errorLog.println "Thread: $thread"
                errorLog.println "Error:"
                err.printStackTrace errorLog
                errorLog.println "-" * 20
                errorLog.flush()
            }
        }

        def loggerFuture = !stepWise ? setupTimedLogger(counter) : null

        for (Document doc : selection) {
            if (doc.deleted) {
                continue
            }
            counter.countRead()
            if (limit > -1 && counter.readCount > limit) {
                break
            }
            DocumentItem item = new DocumentItem(number: counter.readCount, doc: doc, whelk: whelk, preUpdateChecksum: doc.getChecksum(whelk.jsonld))
            item.existsInStorage = !newItems
            batch.items << item
            if (batch.items.size() == batchSize) {
                def batchToProcess = batch
                if (executorService) {
                    executorService.submit {
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
            executorService.submit {
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
    }

    private def createExecutorService(int batchSize) {
        int cpus = Runtime.getRuntime().availableProcessors()
        int maxPoolSize = cpus * 4
        def linkedBlockingDeque = new LinkedBlockingDeque<Runnable>((int) (maxPoolSize * 1.5))

        def executorService = new ThreadPoolExecutor(cpus, maxPoolSize,
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
                assert allowLoud : "Loud changes need to be explicitly allowed"
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
                        item = new DocumentItem(number: item.number, doc: doc, whelk: whelk,
                                preUpdateChecksum: doc.getChecksum(whelk.jsonld), existsInStorage: true)
                        return doProcess(process, item, counter)
                    }
                    counter.countModified()
                } else {
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
        if (!dryRun) {
            whelk.remove(item.doc.shortId, changedIn, scriptJobUri)
        }
        deletedLog.println(item.doc.shortId)
    }

    private boolean doRevertToTime(DocumentItem item) {

        // The 'versions' list is sorted, with the oldest version first.
        List<Document> versions = whelk.storage.loadAllVersions(item.doc.shortId)

        ZonedDateTime restoreTime = ZonedDateTime.parse(item.restoreToTime)

        // If restoreTime is older than any stored version (we can't go back that far)
        ZonedDateTime oldestStoredTime = getLatestModification(versions.get(0))
        if ( restoreTime.isBefore( oldestStoredTime ) ) {
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
        doc.setGenerationProcess(scriptJobUri)
        if (!dryRun) {
            whelk.storeAtomicUpdate(doc, !item.loud, true, true, changedIn, scriptJobUri, item.preUpdateChecksum)
        }
        modifiedLog.println(doc.shortId)
    }

    private void doSaveNew(DocumentItem item) {
        Document doc = item.doc
        doc.setControlNumber(doc.getShortId())
        doc.setGenerationDate(new Date())
        doc.setGenerationProcess(scriptJobUri)
        if (!dryRun) {
            if (!whelk.createDocument(doc, changedIn, scriptJobUri,
                    LegacyIntegrationTools.determineLegacyCollection(doc, whelk.getJsonld()), false, true))
                throw new WhelkException("Failed to save a new document. See general whelk log for details.")
        }
        createdLog.println(doc.shortId)
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

    private Closure compileScript(String scriptPath) {
        if (!compiledScripts.containsKey(scriptPath)) {
            File scriptFile = new File(this.scriptFile.parent, scriptPath)
            String scriptSource = scriptFile.getText("UTF-8")
            CompiledScript script = ((Compilable) engine).compile(scriptSource)
            Bindings bindings = createDefaultBindings()
            Closure process = null
            bindings.put("scriptDir", scriptFile.parent)
            bindings.put("getReportWriter", this.&getReportWriter)
            bindings.put("process", { process = it })
            script.eval(bindings)
            compiledScripts[scriptPath] = process
        }
        return compiledScripts[scriptPath]
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

    private Bindings createMainBindings() {
        Bindings bindings = createDefaultBindings()
        bindings.put("scriptDir", scriptFile.parent)
        bindings.put("baseUri", Document.BASE_URI)
        bindings.put("getReportWriter", this.&getReportWriter)
        bindings.put("script", this.&compileScript)
        bindings.put("selectByCollection", this.&selectByCollection)
        bindings.put("selectByIds", this.&selectByIds)
        bindings.put("selectBySqlWhere", this.&selectBySqlWhere)
        bindings.put("selectFromIterable", this.&selectFromIterable)
        bindings.put("create", this.&create)
        bindings.put("queryIds", this.&queryIds)
        bindings.put("queryDocs", this.&queryDocs)
        bindings.put("incrementStats", statistics.&increment)
        bindings.put("asList", JsonLd::asList)
        bindings.put("getAtPath", DocumentUtil::getAtPath)
        return bindings
    }

    private void run() {
        whelk.setSkipIndex(skipIndex)
        
        log "Running Whelk against:"
        log "  PostgreSQL:"
        log "    url:     ${whelk.storage.connectionPool.getJdbcUrl()}"
        if (whelk.elastic) {
            log "  ElasticSearch:"
            log "    hosts:   ${whelk.elastic.elasticHosts}"
            log "    cluster: ${whelk.elastic.elasticCluster}"
            log "    index:   ${whelk.elastic.defaultIndex}"
        }
        log "Using script: $scriptFile"
        log "Using report dir: $reportsDir"
        if (skipIndex) log "  skipIndex"
        if (dryRun) log "  dryRun"
        if (stepWise) log "  stepWise"
        if (noThreads) log "  noThreads"
        if (limit > -1) log "  limit: $limit"
        if (allowLoud) log "  allowLoud"
        log()
        Bindings bindings = createMainBindings()
        script.eval(bindings)
        finish()
    }

    private void finish() {
        def logWriters = [mainLog, errorLog, modifiedLog, createdLog, deletedLog] + reports.values()
        logWriters.each {
            it.flush()
            it.close()
        }
        log "Done!"
    }

    private PrintWriter getReportWriter(String reportName) {
        reports.computeIfAbsent(reportName, { new PrintWriter(new File(reportsDir, it)) } )
    }

    private void log() {
        mainLog.println()
        System.err.println()
    }

    private void log(String msg) {
        mainLog.println(msg)
        System.err.println(msg)
    }

    private void repeat(String msg) {
        mainLog.println(msg)
        System.err.print "\r$msg"
    }

    static void main(String[] args) {
        def cli = new CliBuilder(usage:'whelktool [options] <SCRIPT>')
        cli.h(longOpt: 'help', 'Print this help message and exit.')
        cli.r(longOpt:'report', args:1, argName:'REPORT-DIR', 'Directory where reports are written (defaults to "reports").')
        cli.I(longOpt:'skip-index', 'Do not index any changes, only write to storage.')
        cli.d(longOpt:'dry-run', 'Do not save any modifications.')
        cli.T(longOpt:'no-threads', 'Do not use threads to parallellize batch processing.')
        cli.s(longOpt:'step', 'Change one document at a time, prompting to continue.')
        cli.l(longOpt:'limit', args:1, argName:'LIMIT', 'Amount of documents to process.')
        cli.a(longOpt:'allow-loud', 'Allow scripts to do loud modifications.')
        cli.n(longOpt:'stats-num-ids', args:1, 'Number of ids to print per entry in STATISTICS.txt.')

        def options = cli.parse(args)
        if (options.h) {
            cli.usage()
            System.exit 0
        }
        def reportsDir = new File(options.r ?: 'reports')
        def scriptPath = options.arguments()[0]

        int statsNumIds = options.n ? Integer.parseInt(options.n) : 3
        def tool = new WhelkTool(scriptPath, reportsDir, statsNumIds)
        tool.skipIndex = options.I
        tool.dryRun = options.d
        tool.stepWise = options.s
        tool.noThreads = options.T
        tool.limit = options.l ? Integer.parseInt(options.l) : -1
        tool.allowLoud = options.a
        tool.run()
    }

}


class DocumentItem {
    int number
    Document doc
    String preUpdateChecksum
    private Whelk whelk
    private boolean needsSaving = false
    private boolean doDelete = false
    private boolean loud = false
    boolean existsInStorage = true
    private String restoreToTime = null
    Closure onError = null

    List getGraph() {
        return doc.data['@graph']
    }

    void scheduleSave(Map params=[:]) {
        needsSaving = true
        set(params)
        assert (restoreToTime == null)
    }

    void scheduleDelete(Map params=[:]) {
        needsSaving = true
        doDelete = true
        set(params)
    }

    void scheduleRevertTo(Map params=[:]) {
        needsSaving = true
        set(params)
        assert (restoreToTime != null)
    }

    private void set(Map params) {
        if (params.containsKey('loud')) {
            this.loud = params.loud
        }
        if (params.containsKey('time')) {
            this.restoreToTime = params.time
        }
        this.onError = params.onError
    }

    def getVersions() {
        whelk.loadAllVersionsByMainId(doc.shortId)
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
