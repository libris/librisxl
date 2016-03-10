package whelk.importer

import org.picocontainer.Characteristics
import org.picocontainer.PicoContainer
import whelk.Document
import whelk.Whelk
import whelk.converter.marc.MarcFrameConverter

import groovy.util.logging.Slf4j as Log
import whelk.filter.LinkFinder
import whelk.reindexer.ElasticReindexer
import whelk.harvester.OaiPmhHarvester
import whelk.tools.PostgresLoadfileWriter
import whelk.util.PropertyLoader
import whelk.util.Tools

import java.sql.SQLRecoverableException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Log
class ImporterMain {

    PicoContainer pico

    ImporterMain(String... propNames) {
        log.info("Setting up import program.")

        Properties props = PropertyLoader.loadProperties(propNames)

        pico = Whelk.getPreparedComponentsContainer(props)
        pico.addComponent(new MarcFrameConverter())
        pico.as(Characteristics.USE_NAMES).addComponent(MySQLImporter)
        pico.addComponent(ElasticReindexer)
        pico.addComponent(DefinitionsImporter)
        pico.addComponent(LinkFinder)
        pico.addComponent(MockImporter)
        pico.addComponent(OaiPmhHarvester)
        pico.start()

        log.info("Started ...")
    }

    void vcopyCmd(String collection) {
        int startAtId = 0
        boolean importComplete = false
        while (!importComplete) {
            importComplete = true
            def importer = pico.getComponent(MySQLImporter)
            importer.m_startAtId = startAtId
            try {
                importer.run(collection)
            } catch (SQLRecoverableException sre) {
                startAtId = importer.m_failedAtId
                importComplete = false
            }
        }
    }

    void vcopydumpCmd(String toFileName, String collection) {
        PostgresLoadfileWriter writer = new PostgresLoadfileWriter(toFileName, collection)
        writer.generatePostgresLoadFile()
    }

    void defsCmd(String fname) {
        def defsimport = pico.getComponent(DefinitionsImporter)
        defsimport.definitionsFilename = fname
        defsimport.run("definitions")
    }

    void harvestCmd(String serviceUrl, username=null, password=null, sourceSystem=null) {
        def harvester = pico.getComponent(OaiPmhHarvester)
        harvester.harvest(serviceUrl, username, password, sourceSystem,
                "ListRecords", "marcxml")
    }

    void reindexCmd(String collection=null) {
        def reindex = pico.getComponent(ElasticReindexer)
        reindex.reindex(collection)
    }

    void linkfindCmd(String collection) {
        def whelk = pico.getComponent(Whelk)
        whelk.storage.versioning = false
        def lf = pico.getComponent(LinkFinder)

        ExecutorService queue = Executors.newFixedThreadPool(10)
        long startTime = System.currentTimeMillis()
        def doclist = []
        int counter = 0
        for (doc in whelk.storage.loadAll(collection)) {
            doc = lf.findLinks(doc)
            doclist << doc
            if (++counter % 1000 == 0) {
                Document[] saveList = new Document[doclist.size()]
                System.arraycopy(doclist.toArray(), 0, saveList, 0, doclist.size())
                queue.execute({
                    whelk.storage.bulkStore(Arrays.asList(saveList), true)
                } as Runnable)
                doclist = []
            }
            Tools.printSpinner("Finding links. $counter documents analyzed ...", counter)
        }
        if (doclist.size() > 0) {
            queue.execute({
                whelk.storage.bulkStore(doclist,true)
            } as Runnable)
        }
        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)
        println("Linkfinding completed. Elapsed time: ${System.currentTimeMillis()-startTime}")
    }

    void setversionCmd() {
        def importer = pico.getComponent(MockImporter)
        importer.run(null)
    }

    static void main(String... args) {
        if (args.length == 0) {
            println("Usage: <progam> [action] [collection]")
            System.exit(1)
        }
        def cmd = args[0] + 'Cmd'
        if (!ImporterMain.methods*.name.any { it == cmd}) {
            println("Unknown action ${args[0]}")
            System.exit(1)
        }
        def main
        if (cmd.startsWith("vcopy")) {
            main = new ImporterMain("secret", "mysql")
        } else {
            main = new ImporterMain("secret")
        }
        def arglist = args.length > 1? args[1..-1] : []
        main."${cmd}"(*arglist)
    }

}
