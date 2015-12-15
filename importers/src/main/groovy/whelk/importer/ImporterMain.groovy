package whelk.importer

import org.picocontainer.Characteristics
import org.picocontainer.PicoContainer
import whelk.Document
import whelk.Whelk
import whelk.component.Storage
import whelk.converter.marc.MarcFrameConverter

import groovy.util.logging.Slf4j as Log
import whelk.filter.LinkFinder
import whelk.reindexer.ElasticReindexer
import whelk.util.PropertyLoader

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
        importer.doImport(collection)
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
        def whelk = pico.getComponent(Whelk.class)
        def lf = pico.getComponent(LinkFinder.class)
        /*
        println("Result1: " + lf.queryForLink("type=Organization&name=NB"))
        println("Result2: " + lf.queryForLink("type=Place&label=Lund"))
        println("Result3: " + lf.queryForLink("type=Person&givenName=Markus&familyName=Sköld"))
        */
        for (doc in whelk.storage.loadAll(collection)) {
            doc = lf.findLinks(doc)
        }
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
