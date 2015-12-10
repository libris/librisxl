package whelk.importer

import org.picocontainer.Characteristics
import org.picocontainer.DefaultPicoContainer
import org.picocontainer.PicoContainer
import org.picocontainer.containers.PropertiesPicoContainer
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter

import groovy.util.logging.Slf4j as Log
import whelk.reindexer.ElasticReindexer
import whelk.util.PropertyLoader

@Log
class ImporterMain {

    PicoContainer pico

    ImporterMain() {

        log.info("Setting up import program.")

        Properties props = PropertyLoader.loadProperties("secret", "mysql")

        pico = Whelk.getPreparedComponentsContainer(props)

        pico.addComponent(new MarcFrameConverter())
        pico.as(Characteristics.USE_NAMES).addComponent(MySQLImporter.class)
        pico.addComponent(ElasticReindexer.class)
        pico.addComponent(DefinitionsImporter.class)

        pico.start()

        log.info("Started ...")
    }

    void goMysql(String dataset) {
        def importer = pico.getComponent(MySQLImporter.class)
        importer.doImport(dataset)
    }

    void goReindex() {
        def reindex = pico.getComponent(ElasticReindexer.class)
        reindex.reindex()
    }

    void goDefs(String fname) {
        def defsimport = pico.getComponent(DefinitionsImporter.class)
        defsimport.go(fname)
    }

    static void main(String... args) {
        if (args.length == 0) {
            println("Usage: <progam> [action] [dataset]")
            System.exit(1)
        }
        def main = new ImporterMain()
        if (args[0] == "vcopy") {
            main.goMysql(args[1])
        } else if (args[0] == "defs") {
            main.goDefs(args[1])
        } else if (args[0] == "reindex") {
            main.goReindex()
        } else {
            println("Unknown action ${args[0]}")
            System.exit(1)
        }
    }

}
