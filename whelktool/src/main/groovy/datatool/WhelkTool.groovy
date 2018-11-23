package whelk.datatool

import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import java.sql.ResultSet
import java.sql.SQLException

import javax.script.ScriptEngineManager
import javax.script.Bindings
import javax.script.SimpleBindings
import javax.script.CompiledScript
import javax.script.Compilable

import groovy.util.CliBuilder
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl

import org.codehaus.jackson.map.ObjectMapper

import whelk.Whelk
import whelk.Document


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
    Map<String, PrintWriter> reports = [:]

    boolean dryRun
    boolean noThreads = true
    boolean stepWise
    int limit = -1

    boolean allowLoud

    private boolean errorDetected

    private def jsonWriter = new ObjectMapper().writerWithDefaultPrettyPrinter()

    Map<String, Closure> compiledScripts = [:]

    WhelkTool(String scriptPath, File reportsDir=null) {
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

    }

    private void initScript(String scriptPath) {
        ScriptEngineManager manager = new ScriptEngineManager()
        engine = (GroovyScriptEngineImpl) manager.getEngineByName("groovy")
        scriptFile = new File(scriptPath)
        String scriptSource = scriptFile.getText("UTF-8")
        script = ((Compilable) engine).compile(scriptSource)
        def segment = '/scripts/'
        def path = scriptFile.toURI().toString()
        path = path.substring(path.lastIndexOf(segment) + segment.size())
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
            int batchSize = DEFAULT_BATCH_SIZE) {
        log "Select by ${ids.size()} IDs"
        def uriIdMap = findShortIdsForUris(ids.findAll { it.contains(':') })
        def shortIds = ids.findResults { it.contains(':') ? uriIdMap[it] : it }

        def idItems = shortIds.collect { "'$it'" }.join(',\n')
        doSelectBySqlWhere("id IN ($idItems) AND deleted = false", process,
                batchSize)
    }

    Map<String, String> findShortIdsForUris(Collection uris) {
        def uriIdMap = [:]
        if (!uris) {
            return uriIdMap
        }
        def uriItems = uris.collect { "'$it'" }.join(',\n')
        def query = """
            SELECT id, iri
            FROM ${whelk.storage.mainTableName}__identifiers
            WHERE iri IN ($uriItems)
            """
        def conn = whelk.storage.getConnection()
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
            conn.close()
        }
        return uriIdMap
    }

    void selectBySqlWhere(String whereClause, Closure process,
            int batchSize = DEFAULT_BATCH_SIZE) {
        log "Select by SQL"
        doSelectBySqlWhere(whereClause, process, batchSize)
    }

    private void doSelectBySqlWhere(String whereClause, Closure process,
            int batchSize = DEFAULT_BATCH_SIZE) {
        def query = """
            SELECT id, data, created, modified, deleted
            FROM $whelk.storage.mainTableName
            WHERE $whereClause
            """
        def conn = whelk.storage.getConnection()
        conn.setAutoCommit(false)
        def stmt = conn.prepareStatement(query)
        stmt.setFetchSize(DEFAULT_FETCH_SIZE)
        def rs = stmt.executeQuery()
        select(iterateDocuments(rs), process, batchSize)
    }

    Iterable<Document> iterateDocuments(ResultSet rs) {
        def conn = rs.statement.connection
        boolean more = rs.next() // rs starts at "-1"
        if (!more) {
            try {
                conn.commit()
                conn.setAutoCommit(true)
            } finally {
                conn.close()
            }
        }
        return new Iterable<Document>() {
            Iterator<Document> iterator() {
                return new Iterator<Document>() {
                    @Override
                    public Document next() {
                        Document doc = whelk.storage.assembleDocument(rs)
                        more = rs.next()
                        if (!more) {
                            try {
                                conn.commit()
                                conn.setAutoCommit(true)
                            } finally {
                                conn.close()
                            }
                        }
                        return doc
                    }

                    @Override
                    public boolean hasNext() {
                        return more
                    }
                }
            }
        }
    }

    void selectByCollection(String collection, Closure process,
            int batchSize = DEFAULT_BATCH_SIZE) {
        log "Select by collection: ${collection}"
        select(whelk.storage.loadAll(collection), process)
    }

    private void select(Iterable<Document> selection, Closure process,
            int batchSize = DEFAULT_BATCH_SIZE) {
        if (errorDetected) {
            log "Error detected, refusing further processing."
            return
        }

        def counter = new Counter()

        int batchCount = 0
        Batch batch = new Batch(number: ++batchCount)

        def executorService = useThreads ? createExecutorService(batchSize) : null

        if (executorService) {
            Thread.setDefaultUncaughtExceptionHandler {
                Thread thread, Throwable err ->
                log "Uncaught error: $err"

                executorService.shutdownNow()

                errorLog.println "Thread: $thread"
                errorLog.println "Error:"
                err.printStackTrace errorLog
                errprLog.println "-" * 20
                errorLog.flush()
            }
        }

        for (Document doc : selection) {
            if (doc.deleted) {
                continue
            }
            counter.countRead()
            if (limit > -1 && counter.readCount > limit) {
                break
            }
            batch.items << new DocumentItem(number: counter.readCount, doc: doc, whelk: whelk)
            if (batch.items.size() == batchSize) {
                def batchToProcess = batch
                if (executorService) {
                    executorService.submit {
                        if (!processBatch(process, batchToProcess, counter)) {
                            errorDetected = true
                            executorService.shutdownNow()
                        }
                    }
                } else {
                    if (!processBatch(process, batchToProcess, counter)) {
                        errorDetected = true
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
    }

    private def createExecutorService(int batchSize) {
        int cpus = Runtime.getRuntime().availableProcessors()
        int maxPoolSize = cpus * 4
        def linkedBlockingDeque = new LinkedBlockingDeque<Runnable>(maxPoolSize)
        def executorService = new ThreadPoolExecutor(cpus, maxPoolSize,
                1, TimeUnit.DAYS,
                linkedBlockingDeque, new ThreadPoolExecutor.CallerRunsPolicy())
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
        process(item)
        if (item.needsSaving) {
            if (stepWise && !confirmNextStep(inJsonStr, item.doc)) {
                return false
            }
            try {
                if (item.doDelete) {
                    doDeletion(item)
                    counter.countDeleted()
                } else {
                    doModification(item)
                    counter.countModified()
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

    private void doDeletion(DocumentItem item) {
        if (!dryRun) {
            whelk.storage.remove(item.doc.shortId, changedIn, scriptJobUri)
        }
    }

    private void doModification(DocumentItem item) {
        Document doc = item.doc
        if (item.loud) {
            assert allowLoud : "Loud changes need to be explicitly allowed"
        }
        doc.setGenerationDate(new Date())
        doc.setGenerationProcess(scriptJobUri)
        if (!dryRun) {
            whelk.storage.storeAtomicUpdate(doc.shortId, !item.loud, changedIn, scriptJobUri, {
                it.data = doc.data
            })
        }
    }

    private boolean confirmNextStep(String inJsonStr, Document doc) {
        new File(reportsDir, "IN.jsonld").setText(inJsonStr, 'UTF-8')
        new File(reportsDir, "OUT.jsonld").withWriter {
            jsonWriter.writeValue(it, doc.data)
        }
        println()
        print 'Continue [Y/n]? '
        def answer = System.in.newReader().readLine()
        return answer.toLowerCase() != 'n'
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
        bindings.put("getReportWriter", this.&getReportWriter)
        bindings.put("script", this.&compileScript)
        bindings.put("selectByCollection", this.&selectByCollection)
        bindings.put("selectByIds", this.&selectByIds)
        bindings.put("selectBySqlWhere", this.&selectBySqlWhere)
        return bindings
    }

    private void run() {
        log "Running Whelk against:"
        log "  PostgreSQL:"
        log "    url:     ${whelk.storage.connectionPool.url}"
        log "    table:   ${whelk.storage.mainTableName}"
        if (whelk.elastic) {
            log "  ElasticSearch:"
            log "    hosts:   ${whelk.elastic.elasticHosts}"
            log "    cluster: ${whelk.elastic.elasticCluster}"
            log "    index:   ${whelk.elastic.defaultIndex}"
        }
        log "Using script: $scriptFile"
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
        mainLog.flush()
        mainLog.close()
        errorLog.flush()
        errorLog.close()
        reports.values().each {
            it.flush()
            it.close()
        }
        log "Done!"
    }

    private PrintWriter getReportWriter(String reportName) {
        def report = reports[reportName]
        if (!report) {
            report = reports[reportName] = new PrintWriter(
                    new File(reportsDir, reportName))
        }
        return report
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
        cli.d(longOpt:'dry-run', 'Do not save any modifications.')
        cli.T(longOpt:'no-threads', 'Do not use threads to parallellize batch processing.')
        cli.s(longOpt:'step', 'Change one document at a time, prompting to continue.')
        cli.l(longOpt:'limit', args:1, argName:'LIMIT', 'Amount of documents to process.')
        cli.a(longOpt:'allow-loud', 'Allow scripts to do loud modifications.')

        def options = cli.parse(args)
        if (options.h) {
            cli.usage()
            System.exit 0
        }
        def reportsDir = new File(options.r ?: 'reports')
        def scriptPath = options.arguments()[0]

        def tool = new WhelkTool(scriptPath, reportsDir)
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
    private Whelk whelk
    private boolean needsSaving = false
    private boolean doDelete = false
    private boolean loud = false
    Closure onError = null

    def List getGraph() {
        return doc.data['@graph']
    }

    void scheduleSave(Map params=[:]) {
        needsSaving = true
        set(params)
    }

    void scheduleDelete(Map params=[:]) {
        needsSaving = true
        doDelete = true
        set(params)
    }

    private void set(Map params) {
        if (params.containsKey('loud')) {
            this.loud = params.loud
        }
        this.onError = params.onError
    }

    def getVersions() {
        whelk.loadAllVersionsByMainId(doc.shortId)
    }
}


class Batch {
    int number
    List<DocumentItem> items = []
}


class Counter {
    long startTime = System.currentTimeMillis()
    int readCount = 0
    int processedCount = 0
    int modifiedCount = 0
    int deleteCount = 0

    synchronized int getSaved() { modifiedCount + deleteCount }

    String getSummary() {
        double docsPerSec = readCount / elapsedSeconds
        "read: $readCount, processed: ${processedCount}, modified: ${modifiedCount}, deleted: ${deleteCount} (at ${docsPerSec.round(3)} docs/s)"
    }

    double getElapsedSeconds() {
        return (System.currentTimeMillis() - startTime) / 1000
    }

    synchronized void countRead() {
        readCount++
    }

    synchronized void countProcessed() {
        processedCount++
    }

    synchronized void countModified() {
        modifiedCount++
    }

    synchronized void countDeleted() {
        deleteCount++
    }
}
