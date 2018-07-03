package whelk.importer

import java.lang.annotation.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import groovy.util.logging.Log4j2 as Log
import groovy.sql.Sql

import whelk.Conversiontester
import whelk.Document
import whelk.ElasticConfigGenerator
import whelk.MySQLToMarcJSONDumper
import whelk.PostgresLoadfileWriter
import whelk.Whelk
import whelk.actors.StatsMaker
import whelk.component.PostgreSQLComponent
import whelk.converter.JsonLdToTurtle
import whelk.filter.LinkFinder
import whelk.importer.DefinitionsImporter
import whelk.reindexer.ElasticReindexer
import whelk.util.PropertyLoader
import whelk.util.Tools

@Log
class ImporterMain {

    private Properties props

    ImporterMain(String... propNames) {
        props = PropertyLoader.loadProperties(propNames)
    }

    def getWhelk() {
        return Whelk.createLoadedSearchWhelk(props)
    }

    @Command(args='TO_FILE_NAME COLLECTION')
    void vcopydump(String toFileName, String collection) {
        def connUrl = props.getProperty("mysqlConnectionUrl")
        PostgreSQLComponent psql = new PostgreSQLComponent(props)
        PostgresLoadfileWriter.dumpToFile(toFileName, collection, connUrl, psql)
    }

    @Command(args='TO_FILE_NAME COLLECTION DATA_SELECTION_TSVFILE')
    void vcopydumptestdata(String toFileName, String collection, String exampleDataFileName) {
        def connUrl = props.getProperty("mysqlConnectionUrl")
        PostgreSQLComponent psql = new PostgreSQLComponent(props)
        PostgresLoadfileWriter.dumpToFile(toFileName, collection, connUrl, exampleDataFileName, psql)
    }

    @Command(args='DATA_SELECTION_TSVFILE')
    void vcopydumpexampledependencies(String exampleDataFileName) {
        List vcopyIdsToImport = PostgresLoadfileWriter.collectIDsFromExampleFile(exampleDataFileName, "bib")
        dumpLinkedRecords(vcopyIdsToImport)
    }

    /**
     * Typical invocation:
     * java -jar build/libs/vcopyImporter.jar vcopyconversiontest bib
     * or (for also generating a marc reversion diff file):
     * java -jar build/libs/vcopyImporter.jar vcopyconversiontest bib diff
     */
    @Command(args='COLLECTION [DIFF_OPTION]')
    void vcopyconversiontest(String collection, diffOption = null) {
        String connUrl = props.getProperty("mysqlConnectionUrl")
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        List<Object> queryParameters = [0]
        boolean generateDiffFile = false
        if (diffOption == "diff")
            generateDiffFile = true
        Conversiontester conversionTester = new Conversiontester(generateDiffFile)
        MySQLLoader.run(conversionTester, sqlQuery, queryParameters, collection, connUrl)
        conversionTester.close()
    }

    /**
     * Typical invocation:
     * java -jar build/libs/vcopyImporter.jar generateEsConfig ../librisxl-tools/elasticsearch/libris_config.json ../../definitions/source/vocab/display.jsonld ../../definitions/build/vocab.jsonld generated_es_config.json
     */
    @Command(args='TEMPLATE_FILE_NAME DISPLAY_INFO_FILE_NAME VOCAB_FILE_NAME TO_FILE_NAME')
    static void generateEsConfig(String templateFileName, String displayInfoFileName, String vocabFileName, String toFileName) {
        String templateString = new File(templateFileName).text
        String displayInfoString = new File(displayInfoFileName).text
        String vocabString = new File(vocabFileName).text
        String generatedConfig = ElasticConfigGenerator.generateConfigString(
                templateString,
                displayInfoString,
                vocabString)

        new File(toFileName).write(generatedConfig)
    }

    @Command(args='TO_FOLDER_NAME')
    void vcopystats(String toFolderName) {
        def connectionUrl = props.getProperty("mysqlConnectionUrl")
        def collection = 'bib'
        StatsMaker statsMaker = new StatsMaker()
        String sqlQuery = MySQLLoader.selectByMarcType[collection]

        MySQLLoader.run(statsMaker, sqlQuery, [0], collection, connectionUrl)
    }

    @Command(args='COLLECTION [TO_FILE_NAME]')
    void vcopyjsondump(String collection, String toFileName=null) {
        def connectionUrl = props.getProperty("mysqlConnectionUrl")
        MySQLToMarcJSONDumper myDumper = new MySQLToMarcJSONDumper(toFileName)
        String sqlQuery = MySQLLoader.selectByMarcType[collection]
        MySQLLoader.run(myDumper, sqlQuery, [0], collection, connectionUrl)
        myDumper.dumpWriter.close()
        def endSecs = (System.currentTimeMillis() - myDumper.startTime) / 1000
        System.err.println "Done in $endSecs seconds."
    }

    @Command(args='FNAME')
    void defs(String fname) {
        def whelk = new Whelk(new PostgreSQLComponent(props))
        DefinitionsImporter defsImporter = new DefinitionsImporter(whelk)
        defsImporter.definitionsFilename = fname
        defsImporter.run("definitions")
    }

    @Command(args='COLLECTION [SOURCE_SYSTEM]')
    void vcopyharvest(String collection, String sourceSystem = 'vcopy') {
        log.info("Running vcopyharvest for collection ${collection} (source ${sourceSystem})")
        def connUrl = props.getProperty("mysqlConnectionUrl")
        VCopyImporter importer = new VCopyImporter(whelk)
        importer.doImport(collection, sourceSystem, connUrl)
    }

    @Command(args='[COLLECTION]')
    void reindex(String collection=null) {
        boolean useCache = true
        Whelk whelk = Whelk.createLoadedSearchWhelk(props, useCache)
        def reindex = new ElasticReindexer(whelk)
        reindex.reindex(collection)
    }

    @Command(args='COLLECTION')
    void benchmark(String collection) {
        log.info("Starting benchmark for collection ${collection ?: 'all'}")

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

    static void sendToQueue(Whelk whelk, List doclist, ExecutorService queue, Map counters, String collection) {
        LinkFinder lf = new LinkFinder(whelk.storage)
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
            whelk.storage.bulkStore(storeList, "xl", null, collection)
        } as Runnable)
    }

    @Command(args='FILE')
    void vcopyloadexampledata(String file) {
        def connUrl = props.getProperty("mysqlConnectionUrl")

        def idgroups = new File(file).readLines()
                .findAll { String line -> ['\t', '/'].every { it -> line.contains(it) } }
                .collect { line ->
                            def split = line.substring(0, line.indexOf("\t")).split('/')
                            [collection: split[0], id: split[1]]
                         }
                .groupBy { it -> it.collection }
                .collect { k, v -> [key: k, value: v.collect { it -> it.id }] }

        def bibIds = idgroups.find{it->it.key == 'bib'}.value

        VCopyImporter importer = new VCopyImporter(whelk)
        importLinkedRecords(importer, bibIds)

        idgroups.each { group ->
            ImportResult importResult = importer.doImport(group.key, 'vcopy', connUrl, group.value as String[])
            System.err.println("Created ${importResult?.numberOfDocuments} documents from  ${group.key}.")
        }


        System.err.println("All done importing example data.")
    }

    def dumpLinkedRecords(List<String> bibIds) {
        def connUrl = props.getProperty("mysqlConnectionUrl")

        def extraAuthIds = getExtraAuthIds(connUrl,bibIds)
        File out = new File("authdeps")
        String[] list = extraAuthIds as String[]
        StringBuilder builder = new StringBuilder("")
        for (String s : list)
            builder.append("auth/" + s + "\n")
        out.write(builder.toString())

        def extraHoldIds = getExtraHoldIds(connUrl,bibIds)

        out = new File("holddeps")
        list = extraHoldIds as String[]
        builder = new StringBuilder("")
        for (String s : list)
            builder.append("hold/" + s + "\n")
        out.write(builder.toString())
    }

    def importLinkedRecords(VCopyImporter importer, List<String> bibIds) {
        def connUrl = props.getProperty("mysqlConnectionUrl")

        def extraAuthIds = getExtraAuthIds(connUrl,bibIds)
        System.err.println("Found ${extraAuthIds.count {it}} linked authority records from bibliographic records. Importing...")
        ImportResult importResult = importer.doImport('auth', 'vcopy', connUrl, extraAuthIds as String[])
        System.err.println("Created ${importResult?.numberOfDocuments} documents from linked authority records")

        def extraBibIds = getExtraHoldIds(connUrl,bibIds)
        System.err.println("Found ${extraBibIds.count {it}} linked holding records from bibliographic records. Importing...")
        importResult = importer.doImport('hold', 'vcopy', connUrl, extraBibIds as String[])
        System.err.println("Created ${importResult?.numberOfDocuments} documents from linked holding records")
    }

    static List<String> getExtraAuthIds(String connUrl, List<String> bibIds){
        String sqlQuery = 'SELECT bib_id, auth_id FROM auth_bib WHERE bib_id IN (?)'.replace('?',bibIds.collect{it->'?'}.join(','))
        def sql = Sql.newInstance(connUrl, "com.mysql.jdbc.Driver")
        def rows = sql.rows(sqlQuery,bibIds)
        return rows.collect {it->it.auth_id}
    }

    static List<String> getExtraHoldIds(String connUrl, List<String> bibIds){
        String sqlQuery = 'SELECT mfhd_id FROM mfhd_record WHERE mfhd_record.bib_id IN (?) AND deleted = 0'.replace('?',bibIds.collect{it->'?'}.join(','))
        def sql = Sql.newInstance(connUrl, "com.mysql.jdbc.Driver")
        def rows = sql.rows(sqlQuery,bibIds)
        return rows.collect {it->it.mfhd_id}
    }


    @Command(args='COLLECTION')
    void linkfind(String collection) {
        log.info("Starting linkfinder for collection ${collection ?: 'all'}")
        whelk.storage.versioning = false

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
                sendToQueue(whelk, doclist, queue, counters,collection)
                doclist = []
            }
            if (!log.isDebugEnabled()) {
                Tools.printSpinner("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.", counters["read"])
            } else {
                log.debug("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.")
            }
        }


        if (doclist.size() > 0) {
            sendToQueue(whelk, doclist, queue, counters, collection)
        }

        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)
        System.err.println("Linkfinding completed. Elapsed time: ${System.currentTimeMillis() - startTime}")
    }

    @Command(args='FILE')
    void lddbToTrig(String file) {
        def whelk = Whelk.createLoadedCoreWhelk(props)
        def ctx = JsonLdToTurtle.parseContext([
                '@context': whelk.jsonld.context
        ])
        def handleSteam = !file || file == '-' ? { it(System.out) }
                            : new File(file).&withOutputStream
        handleSteam { out ->
            def serializer = new JsonLdToTurtle(ctx, out)
            serializer.prelude()
            int i = 0
            for (Document doc : whelk.storage.loadAll(null)) {
                def id = doc.completeId
                System.err.println "[${++i}] $id"
                serializer.objectToTrig(id, doc.data)
            }
        }
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
        } else if (cmd.startsWith("generateEsConfig")) { // No need for secret.properties to generate es config.
            generateEsConfig(args[1], args[2], args[3], args[4])
            return
        } else {
            main = new ImporterMain("secret")
        }

        try {
            main."${method.name}"(*arglist)
        } catch (IllegalArgumentException e) {
            System.err.println "Missing arguments. Expected:"
            System.err.println "\t${method.name} ${method.getAnnotation(Command).args()}"
            System.err.println e.message
            org.codehaus.groovy.runtime.StackTraceUtils.sanitize(e).printStackTrace()
            System.exit(1)
        }
    }

}

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@interface Command {
    String args() default ''
}
