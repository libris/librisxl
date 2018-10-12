package whelk.datatool

import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

import java.sql.ResultSet

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
    Whelk whelk

    private GroovyScriptEngineImpl engine

    File scriptFile
    CompiledScript script
    String scriptJobUri

    String changedIn = "xl"
    String changedBy = null

    File reportsDir
    File errorLog

    boolean dryRun
    boolean noThreads = true
    boolean stepWise
    int limit = -1

    private def jsonWriter = new ObjectMapper().writerWithDefaultPrettyPrinter()

    Map<String, Closure> compiledScripts = [:]

    WhelkTool(String scriptPath, File reportsDir=null) {
        whelk = Whelk.createLoadedCoreWhelk()
        initScript(scriptPath)
        this.reportsDir = reportsDir
        reportsDir.mkdirs()
        errorLog = new File(reportsDir, "ERRORS.txt")

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
        scriptJobUri = "https://id.kb.se/generator/scripts/${path}"
    }

    boolean getUseThreads() { !noThreads && !stepWise }

    Map load(String id) {
        return whelk.storage.load(id)?.data
    }

    void selectByCollection(String collection, Closure process,
            int batchSize = DEFAULT_BATCH_SIZE) {
        select(whelk.storage.loadAll(collection), process)
    }

    private void select(Iterable<Document> selection, Closure process,
            int batchSize = DEFAULT_BATCH_SIZE) {
        def counter = new Counter()

        int batchCount = 0
        Batch batch = new Batch(number: ++batchCount)
        long startTime = System.currentTimeMillis()

        def executorService = useThreads ? createExecutorService(batchSize) : null

        if (executorService) {
            Thread.setDefaultUncaughtExceptionHandler {
                Thread thread, Throwable err ->
                System.err.println "Uncaught error: $err"
                executorService.shutdownNow()
                errorLog.withPrintWriter {
                    pw.println "Thread: $thread"
                    pw.println "Error:"
                    err.printStackTrace pw
                    pw.flush()
                }
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
                            executorService.shutdownNow()
                        }
                    }
                } else {
                    if (!processBatch(process, batchToProcess, counter)) {
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
            System.err.println()
            System.err.println "Processed: $counter.summary."
        }
    }

    def createExecutorService(int batchSize) {
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
    boolean processBatch(Closure process, Batch batch, def counter) {
        boolean doContinue = true
        for (DocumentItem item : batch.items) {
            if (!useThreads) {
                System.err.print "\rProcessing $item.number: ${item.doc.id} ($counter.summary)"
            }
            try {
                doContinue = doProcess(process, item, counter)
            } catch (Throwable err) {
                System.err.println "Error occurred when processing <$item.doc.completeId>: $err"
                errorLog.withPrintWriter {
                    it.println "Stopped at document <$item.doc.completeId>"
                    it.println "Process status: $counter.summary"
                    it.println "Error:"
                    err.printStackTrace it
                    it.flush()
                }
                return false
            }
            if (!doContinue) {
                break
            }
        }
        System.err.println()
        System.err.println "\rProcessed batch $batch.number ($counter.summary)"
        return doContinue
    }

    /**
     * @return true to continue, false to break.
     */
    boolean doProcess(Closure process, DocumentItem item, def counter) {
        String inJsonStr = stepWise
            ? jsonWriter.writeValueAsString(item.doc.data)
            : null
        counter.countProcessed()
        process(item)
        if (item.needsSaving) {
            if (stepWise && !confirmNextStep(inJsonStr, item.doc)) {
                return false
            }
            if (item.doDelete) {
                doDeletion(item.doc)
                counter.countDeleted()
            } else {
                doModification(item.doc)
                counter.countModified()
            }
            if (counter.saved == 1) {
                storeScriptJob()
            }
        }
        return true
    }

    void doDeletion(Document doc) {
        if (!dryRun) {
            whelk.storage.remove(doc.shortId, changedIn, changedBy)
        }
    }

    void doModification(Document doc) {
        doc.setGenerationDate(new Date())
        doc.setGenerationProcess(scriptJobUri)
        if (!dryRun) {
            whelk.storage.storeAtomicUpdate(doc.shortId, !item.loud, changedIn, changedBy, {
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

    private void storeScriptJob() {
        // TODO: store description about script job
        // entity[ID] = scriptJobUri
        // entity[TYPE] = 'ScriptJob'
        // entity.created = storage.formatDate(...)
        // entity.modified = storage.formatDate(...)
    }

    private Closure compileScript(String scriptPath) {
        if (!compiledScripts.containsKey(scriptPath)) {
            String scriptSource = new File(scriptFile.parent, scriptPath).getText("UTF-8")
            CompiledScript script = ((Compilable) engine).compile(scriptSource)
            Bindings bindings = createDefaultBindings()
            Closure process = null
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
        return bindings
    }

    private Bindings createMainBindings() {
        Bindings bindings = createDefaultBindings()
        bindings.put("script", this.&compileScript)
        bindings.put("selectByCollection", this.&selectByCollection)
        bindings.put("load", this.&load)
        return bindings
    }

    private void run() {
        System.err.println "Running Whelk against:"
        System.err.println "  PostgreSQL: ${whelk.storage.connectionPool.url}"
        System.err.println "  ElasticSearch: ${whelk.elastic?.elasticHosts}"
        System.err.println "Using script: $scriptFile"
        if (dryRun) System.err.println "  dryRun"
        if (stepWise) System.err.println "  stepWise"
        if (noThreads) System.err.println "  noThreads"
        if (limit > -1) System.err.println "  limit: $limit"
        System.err.println()
        Bindings bindings = createMainBindings()
        script.eval(bindings)
    }

    static void main(String[] args) {
        def cli = new CliBuilder(usage:'whelktool [options] <SCRIPT>')
        cli.h(longOpt: 'help', 'Print this help message')
        cli.r(longOpt:'report', args:1, argName:'REPORT-DIR', 'Directory where reports are written (defaults to "reports")')
        cli.d(longOpt:'dry-run', 'Do not save any modifications')
        cli.T(longOpt:'no-threads', 'Do not use threads to parallellize batch processing')
        cli.s(longOpt:'step', 'Change one document at a time, prompting to continue')
        cli.l(longOpt:'limit', args:1, argName:'LIMIT', 'Amount of documents to process.')

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
        tool.run()
    }

}


class DocumentItem {
    int number
    Document doc
    private Whelk whelk
    private boolean needsSaving = false
    private boolean doDelete = false
    private boolean loud = true

    def List getGraph() {
        return doc.data['@graph']
    }

    void scheduleSave(loud=true) {
        needsSaving = true
        this.loud = loud
    }

    void scheduleDelete(loud=true) {
        needsSaving = true
        doDelete = true
        this.loud = loud
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
    int readCount = 0
    int processedCount = 0
    int modifiedCount = 0
    int deleteCount = 0

    int getSaved() { modifiedCount + deleteCount }

    String getSummary() {
        "read: $readCount, processed: ${processedCount}, modified: ${modifiedCount}, deleted: ${deleteCount}"
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
