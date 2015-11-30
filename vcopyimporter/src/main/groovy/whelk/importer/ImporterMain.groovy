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

        pico.start()

        log.info("Started ...")
    }

    void go(String dataset) {
        def importer = pico.getComponent(MySQLImporter.class)
        importer.doImport(dataset)
    }

    static void main(String... args) {
        if (args.length == 0) {
            println("Usage: <progam> [dataset]")
            System.exit(1)
        }
        new ImporterMain().go(args[0])
    }

}
