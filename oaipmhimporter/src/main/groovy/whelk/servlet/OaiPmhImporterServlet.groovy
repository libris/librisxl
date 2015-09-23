package whelk.servlet

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper
import org.picocontainer.Characteristics
import org.picocontainer.DefaultPicoContainer
import org.picocontainer.PicoContainer
import org.picocontainer.containers.PropertiesPicoContainer
import whelk.Document
import whelk.Whelk
import whelk.component.ElasticSearch
import whelk.component.PostgreSQLComponent
import whelk.converter.marc.MarcFrameConverter
import whelk.importer.OaiPmhImporter
import whelk.scheduler.ScheduledOperator

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/**
 * Created by markus on 15-09-03.
 */
@Log
class OaiPmhImporterServlet extends HttpServlet {

    PicoContainer pico
    int scheduleDelaySeconds = 10
    int scheduleIntervalSeconds = 30
    
    public OaiPmhImporterServlet() {
        log.info("Starting oaipmhimporter.")

        Properties mainProps = new Properties()

        // If an environment parameter is set to point to a file, use that one. Otherwise load from classpath
        InputStream secretsConfig = ( System.getProperty("xl.secret.properties")
                ? new FileInputStream(System.getProperty("xl.secret.properties"))
                : this.getClass().getClassLoader().getResourceAsStream("secret.properties") )

        InputStream oaipmhConfig = ( System.getProperty("xl.oaipmh.properties")
                ? new FileInputStream(System.getProperty("xl.oaipmh.properties"))
                : this.getClass().getClassLoader().getResourceAsStream("oaipmh.properties") )

        Properties props = new Properties()

        props.load(secretsConfig)
        props.load(oaipmhConfig)

        pico = new DefaultPicoContainer(new PropertiesPicoContainer(props))

        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(ElasticSearch.class)
        pico.as(Characteristics.CACHE, Characteristics.USE_NAMES).addComponent(PostgreSQLComponent.class)
        pico.as(Characteristics.USE_NAMES).addComponent(OaiPmhImporter.class)
        pico.addComponent(new MarcFrameConverter())
        pico.addComponent(Whelk.class)

        pico.start()

        log.info("Started ...")
    }

    @Override
    void doGet(HttpServletRequest request, HttpServletResponse response) {
        def storage = pico.getComponent(PostgreSQLComponent)
        def whelkState = storage.load("/sys/whelk.state")?.data ?: [:]

        log.info("Current whelkstate: $whelkState")

        def status = whelkState.get("status")
        def writer = response.writer
        writer.write("oaipmhimporter online, state: $status")
        writer.flush()

        //def writer = response.writer
        //writer.write("oaipmhimporter online")
        //writer.flush()
    }

    void init() {

        ScheduledExecutorService ses = Executors.newScheduledThreadPool(3)

        for (dataset in ["auth", "bib", "hold"]) {
            log.info("Setting up schedule for $dataset")
            def job = new ScheduledJob(pico.getComponent(OaiPmhImporter.class), dataset, pico.getComponent(PostgreSQLComponent.class))
            try {
                ses.scheduleWithFixedDelay(job, scheduleDelaySeconds, scheduleIntervalSeconds, TimeUnit.SECONDS)
            } catch (RejectedExecutionException ree) {
                log.error("execution failed", ree)
            }
        }
        log.info("scheduler started")
    }
}

@Log
class ScheduledJob implements Runnable {

    static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ssX"

    String dataset
    OaiPmhImporter importer
    PostgreSQLComponent storage

    ScheduledJob(OaiPmhImporter imp, String ds, PostgreSQLComponent pg) {
        this.importer = imp
        this.dataset = ds
        this.storage = pg
    }


    void run() {
        assert dataset

        def whelkState = storage.load("/sys/whelk.state")?.data ?: [:]
        log.info("Current whelkstate: $whelkState")
        try {
            String lastImport = whelkState.get(dataset, [:]).get("lastImport")
            Date currentSince
            Date nextSince = new Date()
            if (lastImport) {
                log.trace("Parsing $lastImport as date")
                currentSince = Date.parse(DATE_FORMAT, lastImport)
                nextSince = new Date(currentSince.getTime())
                nextSince.set(second: currentSince[Calendar.SECOND] + 1)
                log.trace("Next since (upped by 1 second): $nextSince")
            } else {
                def lastWeeksDate = nextSince[Calendar.DATE] - 7
                nextSince.set(date: lastWeeksDate)
                currentSince = nextSince
                log.info("Importer has no state for last import from $dataset. Setting last week (${nextSince})")
            }
            log.debug("Executing OAIPMH import for $dataset since $nextSince from ${importer.serviceUrl}")
            whelkState.put("status", "RUNNING")
            whelkState.put("importOperator", dataset)
            whelkState.remove("lastImportOperator")
            def result = importer.doImport(dataset, null, -1, true, true, nextSince)

            int totalCount = result.numberOfDocuments
            if (result.numberOfDocuments > 0 || result.numberOfDeleted > 0 || result.numberOfDocumentsSkipped > 0) {
                log.info("Imported ${result.numberOfDocuments} documents and deleted ${result.numberOfDeleted} for $dataset. Last record has datestamp: ${result.lastRecordDatestamp.format(DATE_FORMAT)}")
                whelkState.put("lastImportNrImported", result.numberOfDocuments)
                whelkState.put("lastImportNrDeleted", result.numberOfDeleted)
                whelkState.put("lastImportNrSkipped", result.numberOfDocumentsSkipped)
                whelkState.put("lastImport", result.lastRecordDatestamp.format(DATE_FORMAT))
            } else {
                log.debug("Imported ${result.numberOfDocuments} document for $dataset.")
                whelkState.put("lastImport", currentSince.format(DATE_FORMAT))
            }
            whelkState.remove("importOperator")
            whelkState.put("status", "IDLE")
            whelkState.put("lastRunNrImported", result.numberOfDocuments)
            whelkState.put("lastRun", new Date().format(DATE_FORMAT))

        } catch (Exception e) {
            log.error("Something failed: ${e.message}", e)
        } finally {

            storage.store(new Document("/sys/whelk.state", whelkState).withDataset("sys"), false)

        }
    }

}

