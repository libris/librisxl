package whelk.importer

import org.picocontainer.Characteristics
import org.picocontainer.PicoContainer
import whelk.Document
import whelk.Whelk
import whelk.converter.marc.MarcFrameConverter

import groovy.util.logging.Slf4j as Log
import whelk.filter.LinkFinder
import whelk.reindexer.ElasticReindexer
import whelk.util.PropertyLoader
import whelk.util.Tools

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
        pico.as(Characteristics.USE_NAMES).addComponent(MySQLImporter.class)
        pico.addComponent(ElasticReindexer.class)
        pico.addComponent(DefinitionsImporter.class)
        pico.addComponent(LinkFinder.class)

        pico.start()

        log.info("Started ...")
    }

    void goMysql(String collection) {
        def importer = pico.getComponent(MySQLImporter.class)
        importer.storageOnly = true
        importer.doImport(collection)
        println("Starting LinkFinder for collection $collection")
        goLinkFind(collection)
    }

    void goReindex() {
        def reindex = pico.getComponent(ElasticReindexer.class)
        reindex.reindex()
    }

    void goDefs(String fname) {
        def defsimport = pico.getComponent(DefinitionsImporter.class)
        defsimport.go(fname)
    }

    void goLinkFind(String collection) {
        ExecutorService queue = Executors.newFixedThreadPool(10)

        def whelk = pico.getComponent(Whelk.class)
        def lf = pico.getComponent(LinkFinder.class)
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
                    whelk.bulkStore(Arrays.asList(saveList))
                } as Runnable)
                doclist = []
            }
            Tools.printSpinner("Finding links. $counter documents analyzed ...", counter)
        }
        if (doclist.size() > 0) {
            queue.execute({
                whelk.bulkStore(doclist)
            } as Runnable)
        }
        queue.shutdown()
        queue.awaitTermination(7, TimeUnit.DAYS)

        println("Linkfinding completed. Elapsed time: ${System.currentTimeMillis()-startTime}")
    }

    static void main(String... args) {
        if (args.length == 0) {
            println("Usage: <progam> [action] [collection]")
            System.exit(1)
        }
        if (args[0] == "vcopy") {
            def main = new ImporterMain("secret", "mysql")
            main.goMysql(args[1])
        } else if (args[0] == "defs") {
            def main = new ImporterMain("secret")
            main.goDefs(args[1])
        } else if (args[0] == "reindex") {
            def main = new ImporterMain("secret")
            main.goReindex()
        } else if (args[0] == "linkfind") {
            def main = new ImporterMain("secret")
            main.goLinkFind(args[1])
        } else {
            println("Unknown action ${args[0]}")
            System.exit(1)
        }
    }

}
