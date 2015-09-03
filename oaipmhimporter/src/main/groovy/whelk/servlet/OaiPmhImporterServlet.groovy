package whelk.servlet

import groovy.util.logging.Slf4j as Log

import org.picocontainer.Characteristics
import org.picocontainer.DefaultPicoContainer
import org.picocontainer.PicoContainer
import org.picocontainer.containers.PropertiesPicoContainer

import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.importer.OaiPmhImporter
import whelk.scheduler.ScheduledOperator

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * Created by markus on 15-09-03.
 */
@Log
class OaiPmhImporterServlet extends HttpServlet {

    PicoContainer pico

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        def writer = response.writer
        writer.write("oaipmhimporter online")
        writer.flush()
    }

    void init() {
        log.info("Starting oaipmhimporter.")

        Properties mainProps = new Properties()

        // If an environment parameter is set to point to a file, use that one. Otherwise load from classpath
        InputStream secretsConfig = ( System.getProperty("xl.secret.properties")
                ? new FileInputStream(System.getProperty("xl.secret.properties"))
                : this.getClass().getClassLoader().getResourceAsStream("secret.properties") )

        Properties props = new Properties()

        props.load(secretsConfig)

        pico = new DefaultPicoContainer(new PropertiesPicoContainer(props))

        pico.as(Characteristics.USE_NAMES).addComponent(ElasticSearch.class)
        pico.as(Characteristics.USE_NAMES).addComponent(PostgreSQLComponent.class)
        pico.as(Characteristics.USE_NAMES).addComponent(OaiPmhImporter.class)
        pico.addComponent(ScheduledOperator.class)
        pico.addComponent(Whelk.class)

        pico.start()

        pico.getComponent(ScheduledOperator.class)

        log.info("Started ...")
    }
}
