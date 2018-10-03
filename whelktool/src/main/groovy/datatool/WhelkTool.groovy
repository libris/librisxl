package whelk.datatool

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

    Whelk whelk

    private GroovyScriptEngineImpl engine

    File scriptFile
    CompiledScript script
    String scriptJobUri

    String changedIn = "xl"
    String changedBy = null

    File reportsDir
    boolean dryRun
    boolean stepWise
    private def jsonWriter = new ObjectMapper().writerWithDefaultPrettyPrinter()

    Map<String, Closure> compiledScripts = [:]

    WhelkTool(String scriptPath, File reportsDir=null) {
        whelk = Whelk.createLoadedCoreWhelk()
        println "Running Whelk against:"
        println "  PostgreSQL: ${whelk.storage.connectionPool.url}"
        println "  ElasticSearch: ${whelk.elastic?.elasticHosts}"
        println "Using script: $scriptPath"
        println()
        initScript(scriptPath)
        this.reportsDir = reportsDir
        reportsDir.mkdirs()
    }

    private void initScript(String scriptPath) {
        ScriptEngineManager manager = new ScriptEngineManager()
        engine = (GroovyScriptEngineImpl) manager.getEngineByName("groovy")
        scriptFile = new File(scriptPath)
        String scriptSource = scriptFile.getText("UTF-8")
        script = ((Compilable) engine).compile(scriptSource)
        scriptJobUri = "https://id.kb.se/generator/globalchanges/${scriptPath}"
    }

    Map load(String id) {
        return whelk.storage.load(id)?.data
    }

    void selectIds(String[] ids, Closure process) {
        // FIXME: implement
    }

    void selectByCollection(String collection, Closure process) {
        select(whelk.storage.loadAll(collection), process)
    }

    private void select(Iterable<Document> selection, Closure process) {
        def i = 0
        def modifiedCount = 0
        def deleteCount = 0

        //new File(reportsDir, "MODIFIED.txt")
        //new File(reportsDir, "DELETED.txt")

        for (Document doc : selection) {
            print "\rProcessing $i: ${doc.id} (modified: ${modifiedCount}, deleted: ${deleteCount})"

            String inJsonStr

            if (stepWise) inJsonStr = jsonWriter.writeValueAsString(doc.data)

            def item = new DocumentItem(doc: doc, whelk: whelk)
            process(item)
            if (item.needsSaving) {
                if (item.doDelete) {
                    deleteCount++
                    if (!dryRun) {
                        whelk.storage.remove(doc.shortId, changedIn, changedBy)
                    }
                } else {
                    modifiedCount++

                    if (stepWise) {
                        if (!confirmNextStep(inJsonStr, doc)) {
                            break
                        }
                    }

                    if (!dryRun) {
                        whelk.storage.storeAtomicUpdate(doc.shortId, true, changedIn, changedBy, {
                            it.data = doc.data
                            it.setGenerationDate(new Date())
                            it.setGenerationProcess(scriptJobUri)
                        })
                    }
                }
                if (modifiedCount == 1 || deleteCount == 1) {
                    storeScriptJob()
                }
            }
            i++
        }
        println "Processed: ${i} records, modified: ${modifiedCount}."
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
        // FIXME: store...
        // entity[ID] = scriptJobUri
        // entity[TYPE] = 'ScriptJob'
        // entity.created = storage.formatDate(...)
        // entity.modified = storage.formatDate(...)
    }

    private Closure compileScript(String scriptPath) {
        if (!compiledScripts.containsKey(scriptPath)) {
            String scriptSource = new File(scriptFile.parent, scriptPath).getText("UTF-8")
            CompiledScript script = ((Compilable) engine).compile(scriptSource)
            compiledScripts[scriptPath] = { item ->
                Bindings bindings = createBindings()
                bindings.put("data", item)
                script.eval(bindings)
            }
        }
        return compiledScripts[scriptPath]
    }

    private Bindings createBindings() {
        Bindings bindings = new SimpleBindings()
        ['graph', 'id', 'type'].each {
            bindings.put(it.toUpperCase(), "@$it" as String)
        }
        return bindings
    }

    private void run() {
        Bindings bindings = createBindings()
        bindings.put("script", this.&compileScript)
        bindings.put("selectByCollection", this.&selectByCollection)
        bindings.put("selectIds", this.&selectIds)
        bindings.put("load", this.&load)
        script.eval(bindings)
    }

    static void main(String[] args) {
        def cli = new CliBuilder(usage:'whelktool [options] <SCRIPT>')
        cli.h(longOpt: 'help', 'Print this help message')
        cli.r(longOpt:'report', args:1, argName:'REPORT-DIR', 'Directory where reports are written (defaults to "reports")')
        cli.d(longOpt:'dry-run', 'Do not save any modifications')
        cli.s(longOpt:'step', 'Change one document at a time, prompting to continue')

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
        tool.run()
    }

}


class DocumentItem {
    Document doc
    private Whelk whelk
    private boolean needsSaving = false
    private boolean doDelete = false
    private boolean versioned = true

    def List getGraph() {
        return doc.data['@graph']
    }

    void scheduleSave(versioned=true) {
        needsSaving = true
        versioned = versioned
    }

    void scheduleDelete(versioned=true) {
        doDelete = true
        versioned = versioned
    }

    def getVersions() {
        whelk.loadAllVersionsByMainId(doc.shortId)
    }
}
