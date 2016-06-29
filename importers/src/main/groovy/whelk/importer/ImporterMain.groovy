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
import whelk.tools.MySQLToMarcJSONDumper
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
        pico.addComponent(OaiPmhHarvester)
        pico.start()

        log.info("Started ...")
    }

    void vcopydumpCmd(String toFileName, String collection) {
        def connUrl = props.getProperty("mysqlConnectionUrl")
        PostgresLoadfileWriter.dump(toFileName, collection, connUrl)
    }

    void vcopyjsondumpCmd(String collection, String toFileName) {
        def connUrl = props.getProperty("mysqlConnectionUrl")
        MySQLToMarcJSONDumper.dump(connUrl, collection, toFileName)
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

    void benchmarkCmd(String collection) {
        log.info("Starting benchmark for collection ${collection ?: 'all'}")
        def whelk = pico.getComponent(Whelk)

        long startTime = System.currentTimeMillis()
        long lastTime = startTime
        int counter = 0
        for (doc in whelk.storage.loadAll(collection)) {
            if (++counter % 1000 == 0) {
                long currTime = System.currentTimeMillis()
                int secs = (currTime - lastTime) / 1000
                log.info("Now read 1000 (total ${counter++}) documents in ${(currTime - lastTime)} milliseconds. Velocity: ${(1000 /((currTime - lastTime)/1000))} docs/sec.")
                lastTime = currTime
            }
        }
        log.info("Done!")
    }

    void sendToQueue(Whelk whelk, List doclist, LinkFinder lf, ExecutorService queue, Map counters) {
        Document[] workList = new Document[doclist.size()]
        System.arraycopy(doclist.toArray(), 0, workList, 0, doclist.size())
        queue.execute({
            List storeList = []
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
            whelk.storage.bulkStore(storeList, true)
        } as Runnable)
        doclist = []
    }

    void linkfindCmd(String collection) {
        log.info("Starting linkfinder for collection ${collection ?: 'all'}")
        def whelk = pico.getComponent(Whelk)
        whelk.storage.versioning = false
        def lf = pico.getComponent(LinkFinder)

        ExecutorService queue = Executors.newCachedThreadPool()

        long startTime = System.currentTimeMillis()
        def doclist = []
        Map counters = [
                "read": 0,
                "found": 0,
                "changed": 0
        ]

        for (doc in whelk.storage.loadAll(collection)) {
            counters["read"]++
            doclist << doc
            if (doclist.size() % 2000 == 0) {
                log.info("Sending off a batch for processing ...")
                sendToQueue(whelk, doclist, lf, queue, counters)
                doclist = []
            }
            if (!log.isDebugEnabled()) {
                Tools.printSpinner("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.", counters["read"])
            } else {
                log.debug("Finding links. ${counters["read"]} documents read. ${counters["found"]} processed. ${counters["changed"]} changed.")
            }
        }


        if (doclist.size() > 0) {
            sendToQueue(whelk, doclist, lf, queue, counters)
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
