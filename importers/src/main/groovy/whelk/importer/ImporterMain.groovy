package whelk.importer

import whelk.ElasticConfigGenerator

import java.lang.annotation.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import groovy.util.logging.Slf4j as Log
import groovy.sql.Sql
import org.picocontainer.PicoContainer

import whelk.Document
import whelk.Whelk
import whelk.converter.marc.MarcFrameConverter
import whelk.filter.LinkFinder
import whelk.reindexer.ElasticReindexer

import whelk.MySQLToMarcJSONDumper
import whelk.PostgresLoadfileWriter
import whelk.util.PropertyLoader
import whelk.util.Tools

@Log
class ImporterMain {

    PicoContainer pico
    Properties props

    ImporterMain(String... propNames) {
        log.info("Setting up import program.")

        props = PropertyLoader.loadProperties(propNames)

        pico = Whelk.getPreparedComponentsContainer(props)
        pico.addComponent(new MarcFrameConverter())
        pico.addComponent(ElasticReindexer)
        pico.addComponent(DefinitionsImporter)
        pico.addComponent(LinkFinder)
        pico.addComponent(MockImporter)
        pico.start()

        log.info("Started ...")
    }

    @Command(args='TO_FILE_NAME COLLECTION')
    void vcopydump(String toFileName, String collection) {
        def connUrl = props.getProperty("mysqlConnectionUrl")
        PostgresLoadfileWriter.dumpGpars(toFileName, collection, connUrl)
    }

    /**
     * Typical invocation:
     * java -jar build/libs/vcopyImporter.jar generateEsConfig ../librisxl-tools/elasticsearch/libris_config.json ../../definitions/source/vocab/display.jsonld generated_es_config.json
     */
    @Command(args='TEMPLATE_FILE_NAME DISPLAY_INFO_FILE_NAME TO_FILE_NAME')
    void generateEsConfig(String templateFileName, String displayInfoFileName, String toFileName) {
        String templateString = new File(templateFileName).text
        String displayInfoString = new File(displayInfoFileName).text
        String generatedConfig = ElasticConfigGenerator.generate(templateString, displayInfoString)

        new File(toFileName).write(generatedConfig)
    }

    @Command(args='TO_FOLDER_NAME')
    void vcopystats(String toFolderName) {
        def connUrl = props.getProperty("mysqlConnectionUrl")
        PostgresLoadfileWriter.dumpAuthStats(toFolderName, connUrl)
    }

    @Command(args='COLLECTION [TO_FILE_NAME]')
    void vcopyjsondump(String collection, String toFileName=null) {
        def connUrl = props.getProperty("mysqlConnectionUrl")
        MySQLToMarcJSONDumper.dump(connUrl, collection, toFileName)
    }

    @Command(args='FNAME')
    void defs(String fname) {
        def defsimport = pico.getComponent(DefinitionsImporter)
        defsimport.definitionsFilename = fname
        defsimport.run("definitions")
    }

    @Command(args='COLLECTION [SOURCE_SYSTEM]')
    void vcopyharvest(String collection, String sourceSystem = 'vcopy') {
        println collection
        println sourceSystem
        def connUrl = props.getProperty("mysqlConnectionUrl")
        def whelk = pico.getComponent(Whelk.class)
        println whelk.version
        VCopyImporter importer = new VCopyImporter()
        importer.doImport(whelk, collection, sourceSystem, connUrl)
    }

    @Command(args='[COLLECTION]')
    void reindex(String collection=null) {
        def reindex = pico.getComponent(ElasticReindexer)
        reindex.reindex(collection)
    }

    @Command(args='COLLECTION')
    void benchmark(String collection) {
        log.info("Starting benchmark for collection ${collection ?: 'all'}")
        def whelk = pico.getComponent(Whelk)

        long startTime = System.currentTimeMillis()
        long lastTime = startTime
        int counter = 0
        for (doc in whelk.storage.loadAll(collection)) {
            if (++counter % 1000 == 0) {
                long currTime = System.currentTimeMillis()
                log.info("Now read 1000 (total ${counter++}) documents in ${(currTime - lastTime)} milliseconds. Velocity: ${(1000 / ((currTime - lastTime) / 1000))} docs/sec.")
                lastTime = currTime
            }
        }
        log.info("Done!")
    }

    static void sendToQueue(Whelk whelk, List doclist, LinkFinder lf, ExecutorService queue, Map counters, String collection) {
        Document[] workList = new Document[doclist.size()]
        System.arraycopy(doclist.toArray(), 0, workList, 0, doclist.size())
        queue.execute({
            List<Document> storeList = []
            for (Document wdoc in Arrays.asList(workList)) {
                Document fdoc = lf.findLinks(wdoc)
                if (fdoc) {
                    counters["changed"]++
                    storeList << fdoc
                }
                counters["found"]++
                if (!log.isDebugEnabled()) {
                    Tools.printSpinner("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.", counters["found"])
                } else {
                    log.debug("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.")
                }
            }
            log.info("Saving ${storeList.size()} documents ...")
            whelk.storage.bulkStore(storeList, true, "whelk?", "?", collection)
        } as Runnable)
    }

    @Command(args='FILE')
    void vcopyloadexampledata(String file) {
        def connUrl = props.getProperty("mysqlConnectionUrl")
        def whelk = pico.getComponent(Whelk.class)
        println whelk.version
        VCopyImporter importer = new VCopyImporter()

        def idgroups = new File(file).readLines()
                .findAll { String line -> ['\t', '/'].every { it -> line.contains(it) } }
                .collect { line ->
            def split = line.substring(0, line.indexOf("\t")).split('/')
            [collection: split[0], id: split[1]]
        }
        .groupBy { it -> it.collection }
                .collect { k, v -> [key: k, value: v.collect { it -> it.id }] }

        def bibIds = idgroups.find{it->it.key == 'bib'}.value
        def extraAuthIds = getExtraAuthIds(connUrl,bibIds)
        println "Found ${extraAuthIds.count {it}} linked authority records from bibliographic records"

        idgroups.each { group ->
            ImportResult importResult = importer.doImport(whelk, group.key, 'vcopy', connUrl, group.value as String[])
            println "Created ${importResult?.numberOfDocuments} documents från  ${group.key}."
        }
        ImportResult importResult = importer.doImport(whelk, 'auth', 'vcopy', connUrl, extraAuthIds as String[])
        println "Created ${importResult?.numberOfDocuments} documents från  linked authority records"
        println "All done importing example data."
    }

    static List<String> getExtraAuthIds(String connUrl, List<String> bibIds){
        String sqlQuery = 'SELECT bib_id, auth_id FROM auth_bib WHERE bib_id IN (?)'.replace('?',bibIds.collect{it->'?'}.join(','))
        def sql = Sql.newInstance(connUrl, "com.mysql.jdbc.Driver")
        def rows = sql.rows(sqlQuery,bibIds)
        return rows.collect {it->it.auth_id}
    }

    @Command(args='COLLECTION')
    void linkfind(String collection) {
        log.info("Starting linkfinder for collection ${collection ?: 'all'}")
        def whelk = pico.getComponent(Whelk)
        whelk.storage.versioning = false
        def lf = pico.getComponent(LinkFinder)

        ExecutorService queue = Executors.newCachedThreadPool()

        long startTime = System.currentTimeMillis()
        def doclist = []
        Map counters = [
                "read"   : 0,
                "found"  : 0,
                "changed": 0
        ]

        for (doc in whelk.storage.loadAll(collection)) {
            counters["read"]++
            doclist << doc
            if (doclist.size() % 2000 == 0) {
                log.info("Sending off a batch for processing ...")
                sendToQueue(whelk, doclist, lf, queue, counters,collection)
                doclist = []
            }
            if (!log.isDebugEnabled()) {
                Tools.printSpinner("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.", counters["read"])
            } else {
                log.debug("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.")
            }
        }


        if (doclist.size() > 0) {
            sendToQueue(whelk, doclist, lf, queue, counters, collection)
        }

        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)
        println "Linkfinding completed. Elapsed time: ${System.currentTimeMillis() - startTime}"
    }

    @Command
    void setversion() {
        def importer = pico.getComponent(MockImporter)
        importer.run(null)
    }

    static COMMANDS = getMethods().findAll { it.getAnnotation(Command)
                                    }.collectEntries { [it.name, it]}

    static void help() {
        System.err.println "Usage: <program> COMMAND ARGS..."
        System.err.println()
        System.err.println("Available commands:")
        COMMANDS.values().sort().each {
            System.err.println "\t${it.name} ${it.getAnnotation(Command).args()}"
        }
    }

    static void main(String... args) {
        if (args.length == 0) {
            help()
            System.exit(1)
        }

        def cmd = args[0]
        def arglist = args.length > 1? args[1..-1] : []

        def method = COMMANDS[cmd]
        if (!method) {
            System.err.println "Unknown command: ${cmd}"
            help()
            System.exit(1)
        }

        def main
        if (cmd.startsWith("vcopy")) {
            main = new ImporterMain("secret", "mysql")
        } else {
            main = new ImporterMain("secret")
        }

        try {
            main."${method.name}"(*arglist)
        } catch (IllegalArgumentException e) {
            System.err.println "Missing arguments. Expected:"
            System.err.println "\t${method.name} ${method.getAnnotation(Command).args()}"
            System.exit(1)
        }
    }

}

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface Command {
    String args() default ''
}
