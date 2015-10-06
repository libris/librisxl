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

@Log
class ImporterMain {

    PicoContainer pico

    ImporterMain() {

        log.info("Setting up httpwhelk.")
        // If an environment parameter is set to point to a file, use that one. Otherwise load from classpath
        InputStream secretsConfig = ( System.getProperty("xl.secret.properties")
                ? new FileInputStream(System.getProperty("xl.secret.properties"))
                : this.getClass().getClassLoader().getResourceAsStream("secret.properties") )
        InputStream mysqlConfig = ( System.getProperty("xl.mysql.properties")
                ? new FileInputStream(System.getProperty("xl.mysql.properties"))
                : this.getClass().getClassLoader().getResourceAsStream("mysql.properties") )

        Properties props = new Properties()

        props.load(secretsConfig)
        props.load(mysqlConfig)

        pico = new DefaultPicoContainer(new PropertiesPicoContainer(props))
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(ElasticSearch.class)
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(PostgreSQLComponent.class)
        pico.addComponent(new MarcFrameConverter())
        pico.addComponent(Whelk.class)
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
        new ImporterMain().go(args[1])
    }

}
